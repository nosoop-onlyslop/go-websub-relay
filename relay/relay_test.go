package relay

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/nosoop-onlyslop/go-websub-relay/config"
	"github.com/nosoop-onlyslop/go-websub-relay/protocol"
	"github.com/nosoop-onlyslop/go-websub-relay/topics"
)

type fakeSink struct {
	mu     sync.Mutex
	sent   []protocol.ServerMessage
	closed bool
}

func (f *fakeSink) Send(data []byte) error {
	message, err := protocol.DecodeServer(data)
	if err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sent = append(f.sent, message)
	return nil
}

func (f *fakeSink) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	return nil
}

func (f *fakeSink) notifications() []protocol.Notification {
	f.mu.Lock()
	defer f.mu.Unlock()
	var notifications []protocol.Notification
	for _, message := range f.sent {
		if notification, ok := message.(protocol.Notification); ok {
			notifications = append(notifications, notification)
		}
	}
	return notifications
}

func waitFor(t *testing.T, timeout time.Duration, condition func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return condition()
}

func TestRelaySubscriptionLifecycle(t *testing.T) {
	hubServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusAccepted)
	}))
	defer hubServer.Close()

	cfg := config.Defaults()
	cfg.PublicURL = "https://relay.example"
	cfg.DBPath = t.TempDir() + "/relay.db3"
	cfg.DefaultHub = hubServer.URL
	cfg.ManagerInterval = config.Duration(time.Hour)
	deliveryKey := "key-one"
	cfg.Tenants = []config.TenantConfig{
		{ID: "t1", Token: "token-one", DeliveryKey: &deliveryKey},
		{ID: "t2", Token: "token-two"},
	}

	service, err := New(&cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := service.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer service.Stop()

	feedURL := topics.YouTubeRSSURL("UCabc", "")
	topic := topics.NormalizeTopic(feedURL)
	target := protocol.WatchTarget{
		Topic:   feedURL,
		Routing: map[string]string{"kind": "youtube", "channel_id": "UCabc"},
	}

	if err := service.SyncWatches(ctx, "t1", []protocol.WatchTarget{target}); err != nil {
		t.Fatal(err)
	}
	if err := service.SyncWatches(ctx, "t2", []protocol.WatchTarget{target}); err != nil {
		t.Fatal(err)
	}

	subscriptions, err := service.DB.AllSubscriptions()
	if err != nil {
		t.Fatal(err)
	}
	if len(subscriptions) != 1 {
		t.Fatalf("expected one shared subscription, got %d", len(subscriptions))
	}
	subscription := subscriptions[0]
	if subscription.Topic != topic {
		t.Fatalf("unexpected topic %q", subscription.Topic)
	}
	count, _ := service.DB.WatchCount(hubServer.URL, topic)
	if count != 2 {
		t.Fatalf("expected watch count 2, got %d", count)
	}
	if subscription.Status != "pending" {
		t.Fatalf("expected pending status, got %q", subscription.Status)
	}

	verified, err := service.Verify(ctx, subscription.CallbackToken, "subscribe", topic, "3600")
	if err != nil || !verified {
		t.Fatalf("Verify = %v, %v", verified, err)
	}
	active, _ := service.DB.GetSubscription(hubServer.URL, topic)
	if active.Status != "active" {
		t.Fatalf("expected active status, got %q", active.Status)
	}

	body := []byte("<feed><yt:channelId>UCabc</yt:channelId></feed>")
	mac := hmac.New(sha256.New, []byte(active.Secret.String))
	mac.Write(body)
	signature := "sha256=" + hex.EncodeToString(mac.Sum(nil))
	accepted, known, err := service.Push(ctx, subscription.CallbackToken, body, "application/atom+xml", signature)
	if err != nil || !accepted || !known {
		t.Fatalf("Push = %v, %v, %v", accepted, known, err)
	}
	if pending, _ := service.DB.NextPendingForTenant("t1", 0); pending == nil {
		t.Fatal("expected pending delivery for t1")
	}
	if pending, _ := service.DB.NextPendingForTenant("t2", 0); pending == nil {
		t.Fatal("expected pending delivery for t2")
	}

	accepted, known, err = service.Push(
		ctx, subscription.CallbackToken, []byte("<feed>tampered</feed>"),
		"application/atom+xml", signature,
	)
	if err != nil || accepted || !known {
		t.Fatalf("tampered Push = %v, %v, %v", accepted, known, err)
	}

	tenant, err := service.Authenticate("token-one")
	if err != nil || tenant == nil {
		t.Fatalf("Authenticate: %v, %v", tenant, err)
	}
	sink := &fakeSink{}
	session := service.OpenSession(tenant.ID, tenant.DeliveryKey.String, tenant.LastAcked, sink)
	deliverCtx, cancelDelivery := context.WithCancel(ctx)
	defer cancelDelivery()
	go service.DeliverLoop(deliverCtx, session)

	if !waitFor(t, 2*time.Second, func() bool { return len(sink.notifications()) > 0 }) {
		t.Fatal("timed out waiting for notification")
	}
	notification := sink.notifications()[0]
	if notification.Routing["channel_id"] != "UCabc" {
		t.Fatalf("unexpected routing: %+v", notification.Routing)
	}
	if notification.Signature == nil {
		t.Fatal("expected delivery signature for t1")
	}
	if !protocol.VerifyDeliverySignature(
		deliveryKey, "t1", notification.ID, notification.Topic, notification.Body,
		*notification.Signature,
	) {
		t.Fatal("delivery signature did not verify")
	}

	if err := service.HandleClientMessage(ctx, session, protocol.Ack{UpTo: notification.ID}); err != nil {
		t.Fatal(err)
	}
	if !waitFor(t, 2*time.Second, func() bool {
		pending, _ := service.DB.NextPendingForTenant("t1", 0)
		return pending == nil
	}) {
		t.Fatal("expected delivery to be acked")
	}
	service.CloseSession(session)
}
