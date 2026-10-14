// Package server wires the relay service to HTTP and WebSocket endpoints,
// mirroring websub_relay.app.
package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"github.com/nosoop-onlyslop/go-websub-relay/protocol"
	"github.com/nosoop-onlyslop/go-websub-relay/relay"
)

// maxMessageSize matches aiohttp's default WebSocket message size.
const maxMessageSize = 4 * 1024 * 1024

const writeWait = 10 * time.Second

// wsSink serialises writes to a WebSocket connection.
type wsSink struct {
	conn *websocket.Conn
	mu   sync.Mutex
}

func (s *wsSink) Send(data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.conn.WriteMessage(websocket.TextMessage, data)
}

func (s *wsSink) Close() error { return s.conn.Close() }

// App holds the HTTP handlers for the relay.
type App struct {
	Service  *relay.Service
	Logger   *slog.Logger
	upgrader websocket.Upgrader
}

// New returns an App serving the relay service.
func New(service *relay.Service) *App {
	logger := service.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &App{
		Service: service,
		Logger:  logger,
		upgrader: websocket.Upgrader{
			ReadBufferSize:  4096,
			WriteBufferSize: 4096,
			// The relay is a public host; clients are authenticated by token.
			CheckOrigin: func(*http.Request) bool { return true },
		},
	}
}

// Handler returns the HTTP routes for the relay.
func (a *App) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", a.handleHealth)
	mux.HandleFunc("GET /websub/cb/{token}", a.handleVerify)
	mux.HandleFunc("POST /websub/cb/{token}", a.handlePush)
	mux.HandleFunc("GET /v1/control", a.handleControl)
	return mux
}

func (a *App) handleHealth(w http.ResponseWriter, request *http.Request) {
	subscriptions, err := a.Service.DB.AllSubscriptions()
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"status":        "ok",
		"tenants":       len(a.Service.Config.Tenants),
		"subscriptions": len(subscriptions),
		"sessions":      a.Service.SessionCount(),
	})
}

func (a *App) handleVerify(w http.ResponseWriter, request *http.Request) {
	query := request.URL.Query()
	challenge := query.Get("hub.challenge")
	if challenge == "" {
		http.Error(w, "missing hub.challenge", http.StatusBadRequest)
		return
	}
	ok, err := a.Service.Verify(
		request.Context(),
		request.PathValue("token"),
		query.Get("hub.mode"),
		query.Get("hub.topic"),
		query.Get("hub.lease_seconds"),
	)
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = io.WriteString(w, challenge)
}

func (a *App) handlePush(w http.ResponseWriter, request *http.Request) {
	body, err := io.ReadAll(request.Body)
	if err != nil {
		http.Error(w, "could not read body", http.StatusBadRequest)
		return
	}
	signature := request.Header.Get("X-Hub-Signature-256")
	if signature == "" {
		signature = request.Header.Get("X-Hub-Signature")
	}
	accepted, known, err := a.Service.Push(
		request.Context(),
		request.PathValue("token"),
		body,
		request.Header.Get("Content-Type"),
		signature,
	)
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	if !known {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	if !accepted {
		http.Error(w, "invalid hub signature", http.StatusUnauthorized)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (a *App) handleControl(w http.ResponseWriter, request *http.Request) {
	conn, err := a.upgrader.Upgrade(w, request, nil)
	if err != nil {
		return
	}
	defer conn.Close()
	conn.SetReadLimit(maxMessageSize)

	sink := &wsSink{conn: conn}

	fail := func(code, message string, closeCode int) {
		_ = sink.Send(mustEncode(protocol.ErrorMessage{Code: code, Message: message}))
		_ = conn.WriteControl(
			websocket.CloseMessage,
			websocket.FormatCloseMessage(closeCode, truncate(message, 120)),
			time.Now().Add(writeWait),
		)
	}

	messageType, data, err := conn.ReadMessage()
	if err != nil {
		return
	}
	if messageType != websocket.TextMessage && messageType != websocket.BinaryMessage {
		fail("expected_hello", "first message must be hello", 4400)
		return
	}
	decoded, err := protocol.DecodeClient(data)
	if err != nil {
		fail("bad_message", "could not decode hello", 4400)
		return
	}
	hello, ok := decoded.(protocol.Hello)
	if !ok {
		fail("expected_hello", "first message must be hello", 4400)
		return
	}
	if hello.ProtocolVersion != protocol.ProtocolVersion {
		fail(
			"protocol_version",
			fmt.Sprintf("server speaks protocol %d", protocol.ProtocolVersion),
			4406,
		)
		return
	}
	tenant, err := a.Service.Authenticate(hello.Token)
	if err != nil {
		a.Logger.Error("authenticating tenant failed", "error", err)
		fail("internal_error", "authentication failed", 1011)
		return
	}
	if tenant == nil {
		fail("unauthorized", "invalid tenant token", 4401)
		return
	}

	session := a.Service.OpenSession(
		tenant.ID, tenant.DeliveryKey.String, tenant.LastAcked, sink,
	)
	ctx, cancel := context.WithCancel(request.Context())
	defer cancel()
	defer a.Service.CloseSession(session)

	welcome, err := a.Service.BuildWelcome(session)
	if err != nil {
		a.Logger.Error("building welcome failed", "error", err)
		fail("internal_error", "could not build welcome", 1011)
		return
	}
	if err := session.SendJSON(welcome); err != nil {
		return
	}
	a.Logger.Info("tenant connected", "tenant", session.TenantID)

	deliverCtx, deliverCancel := context.WithCancel(ctx)
	defer deliverCancel()
	go a.Service.DeliverLoop(deliverCtx, session)

	// aiohttp's WebSocketResponse heartbeat sends ping frames and expects pongs.
	heartbeat := time.Duration(a.Service.Config.Heartbeat * float64(time.Second))
	if heartbeat > 0 {
		pongWait := 2 * heartbeat
		_ = conn.SetReadDeadline(time.Now().Add(pongWait))
		conn.SetPongHandler(func(string) error {
			return conn.SetReadDeadline(time.Now().Add(pongWait))
		})
		done := make(chan struct{})
		defer close(done)
		go func() {
			ticker := time.NewTicker(heartbeat)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					if err := conn.WriteControl(
						websocket.PingMessage, nil, time.Now().Add(writeWait),
					); err != nil {
						return
					}
				case <-done:
					return
				}
			}
		}()
	}

	defer a.Logger.Info("tenant disconnected", "tenant", session.TenantID)
	for {
		messageType, data, err := conn.ReadMessage()
		if err != nil {
			return
		}
		if messageType != websocket.TextMessage && messageType != websocket.BinaryMessage {
			continue
		}
		clientMessage, err := protocol.DecodeClient(data)
		if err != nil {
			_ = session.SendJSON(protocol.ErrorMessage{
				Code: "bad_message", Message: "could not decode",
			})
			continue
		}
		if err := a.Service.HandleClientMessage(ctx, session, clientMessage); err != nil {
			a.Logger.Error("handling client message failed", "error", err)
			return
		}
	}
}

func mustEncode(message any) []byte {
	data, err := protocol.Encode(message)
	if err != nil {
		return []byte(`{"type":"error","code":"internal_error","message":"encode failed"}`)
	}
	return data
}

func truncate(value string, length int) string {
	if len(value) <= length {
		return value
	}
	return value[:length]
}
