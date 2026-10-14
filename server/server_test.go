package server

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/nosoop-onlyslop/go-websub-relay/config"
	"github.com/nosoop-onlyslop/go-websub-relay/protocol"
	"github.com/nosoop-onlyslop/go-websub-relay/relay"
	"github.com/nosoop-onlyslop/go-websub-relay/topics"
)

type hubState struct {
	mu       sync.Mutex
	callback string
	secret   string
}

func (s *hubState) set(callback, secret string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.callback = callback
	s.secret = secret
}

func (s *hubState) get() (string, string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.callback, s.secret
}

func TestRelayHTTPWebSocketRoundTrip(t *testing.T) {
	state := &hubState{}

	// Fake WebSub hub: on subscribe, call the relay's verification callback.
	hubServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		callback := r.Form.Get("hub.callback")
		secret := r.Form.Get("hub.secret")
		state.set(callback, secret)

		verifyURL, err := url.Parse(callback)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		query := verifyURL.Query()
		query.Set("hub.mode", r.Form.Get("hub.mode"))
		query.Set("hub.topic", r.Form.Get("hub.topic"))
		query.Set("hub.challenge", "test-challenge")
		query.Set("hub.lease_seconds", r.Form.Get("hub.lease_seconds"))
		verifyURL.RawQuery = query.Encode()

		response, err := http.Get(verifyURL.String())
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		defer response.Body.Close()
		body, _ := io.ReadAll(response.Body)
		if response.StatusCode != http.StatusOK || string(body) != "test-challenge" {
			http.Error(w, "verification failed", http.StatusBadGateway)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer hubServer.Close()

	cfg := config.Defaults()
	cfg.PublicURL = "http://placeholder"
	cfg.DefaultHub = hubServer.URL
	cfg.ManagerInterval = config.Duration(time.Hour)
	cfg.Tenants = []config.TenantConfig{{ID: "home", Token: "token"}}

	service, err := relay.New(&cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	relayServer := httptest.NewServer(New(service).Handler())
	defer relayServer.Close()
	cfg.PublicURL = relayServer.URL

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := service.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer service.Stop()

	wsURL := strings.Replace(relayServer.URL, "http://", "ws://", 1) + "/v1/control"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	send := func(message any) {
		t.Helper()
		data, err := protocol.Encode(message)
		if err != nil {
			t.Fatal(err)
		}
		if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
			t.Fatal(err)
		}
	}
	receive := func() protocol.ServerMessage {
		t.Helper()
		_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		_, data, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		message, err := protocol.DecodeServer(data)
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		return message
	}

	send(protocol.Hello{Tenant: "home", Token: "token"})
	welcome, ok := receive().(protocol.Welcome)
	if !ok {
		t.Fatalf("expected welcome")
	}
	if welcome.ProtocolVersion != protocol.ProtocolVersion {
		t.Fatalf("unexpected protocol version %d", welcome.ProtocolVersion)
	}

	feedURL := topics.YouTubeRSSURL("UCabc", "")
	send(protocol.Resume{Cursor: 0})
	send(protocol.WatchSync{Topics: []protocol.WatchTarget{{
		Topic:   feedURL,
		Routing: map[string]string{"kind": "youtube", "channel_id": "UCabc"},
	}}})

	stateUpdate, ok := receive().(protocol.WatchStateUpdate)
	if !ok {
		t.Fatalf("expected watch.state")
	}
	if len(stateUpdate.Watches) != 1 {
		t.Fatalf("expected one watch, got %+v", stateUpdate.Watches)
	}
	wantTopic := topics.NormalizeTopic(feedURL)
	if stateUpdate.Watches[0].Topic != wantTopic {
		t.Fatalf("unexpected topic %q", stateUpdate.Watches[0].Topic)
	}
	if stateUpdate.Watches[0].Status != "active" {
		t.Fatalf("unexpected status %q", stateUpdate.Watches[0].Status)
	}
	callback, secret := state.get()
	if callback == "" {
		t.Fatal("hub did not capture callback")
	}

	body := []byte("<feed><yt:channelId>UCabc</yt:channelId></feed>")
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	signature := "sha256=" + hex.EncodeToString(mac.Sum(nil))

	request, _ := http.NewRequest(http.MethodPost, callback, strings.NewReader(string(body)))
	request.Header.Set("Content-Type", "application/atom+xml")
	request.Header.Set("X-Hub-Signature-256", signature)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusNoContent {
		t.Fatalf("unexpected push status %d", response.StatusCode)
	}

	notification, ok := receive().(protocol.Notification)
	if !ok {
		t.Fatalf("expected notification")
	}
	if notification.Routing["channel_id"] != "UCabc" {
		t.Fatalf("unexpected routing %+v", notification.Routing)
	}
	if notification.Body != string(body) {
		t.Fatalf("unexpected body %q", notification.Body)
	}
	send(protocol.Ack{UpTo: notification.ID})
}
