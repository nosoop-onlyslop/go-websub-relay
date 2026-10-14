// Package relay implements the relay service: subscription management, WebSub
// callbacks, and per-tenant delivery.  It mirrors websub_relay.relay.
package relay

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nosoop-onlyslop/go-websub-relay/config"
	"github.com/nosoop-onlyslop/go-websub-relay/hub"
	"github.com/nosoop-onlyslop/go-websub-relay/protocol"
	"github.com/nosoop-onlyslop/go-websub-relay/store"
	"github.com/nosoop-onlyslop/go-websub-relay/topics"
)

// HashToken returns the token hash stored in the tenants table.
func HashToken(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

// MessageSink is the subset of a WebSocket connection the relay needs.
type MessageSink interface {
	Send(data []byte) error
	Close() error
}

// Session is one authenticated control connection for a tenant.
type Session struct {
	TenantID    string
	DeliveryKey string

	sink MessageSink

	mu        sync.Mutex
	lastAcked int64

	wake      chan struct{}
	ackCh     chan struct{}
	closed    chan struct{}
	closeOnce sync.Once
}

func newSession(tenantID, deliveryKey string, lastAcked int64, sink MessageSink) *Session {
	return &Session{
		TenantID:    tenantID,
		DeliveryKey: deliveryKey,
		sink:        sink,
		lastAcked:   lastAcked,
		wake:        make(chan struct{}, 1),
		ackCh:       make(chan struct{}, 1),
		closed:      make(chan struct{}),
	}
}

// LastAcked returns the highest delivery id the client has acknowledged.
func (s *Session) LastAcked() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastAcked
}

// setLastAcked raises the cursor to value, reporting whether it advanced.
func (s *Session) setLastAcked(value int64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if value > s.lastAcked {
		s.lastAcked = value
		return true
	}
	return false
}

// SignalWake notifies the delivery loop that new work may be available.
func (s *Session) SignalWake() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

func (s *Session) signalAck() {
	select {
	case s.ackCh <- struct{}{}:
	default:
	}
}

// Close marks the session closed and unblocks any waiters.
func (s *Session) Close() {
	s.closeOnce.Do(func() { close(s.closed) })
}

// Closed reports whether the session has been closed.
func (s *Session) Closed() bool {
	select {
	case <-s.closed:
		return true
	default:
		return false
	}
}

// SendJSON encodes and sends a protocol message.
func (s *Session) SendJSON(message any) error {
	data, err := protocol.Encode(message)
	if err != nil {
		return err
	}
	return s.sink.Send(data)
}

// Service is the relay daemon.
type Service struct {
	Config *config.RelayConfig
	DB     *store.DB
	HTTP   *http.Client
	Logger *slog.Logger

	mu       sync.Mutex
	sessions map[string]*Session

	managerCancel context.CancelFunc
	managerWG     sync.WaitGroup
}

// New opens the database and returns a relay service.
func New(cfg *config.RelayConfig, logger *slog.Logger) (*Service, error) {
	if logger == nil {
		logger = slog.Default()
	}
	db, err := store.Open(cfg.DBPath)
	if err != nil {
		return nil, err
	}
	return &Service{
		Config:   cfg,
		DB:       db,
		Logger:   logger,
		sessions: map[string]*Session{},
	}, nil
}

// Start upserts tenants and launches the subscription manager.
func (s *Service) Start(ctx context.Context) error {
	for _, tenant := range s.Config.Tenants {
		if err := s.DB.UpsertTenant(
			tenant.ID, HashToken(tenant.Token), tenant.DeliveryKey, tenant.WatchLimit,
		); err != nil {
			return err
		}
	}
	if s.HTTP == nil {
		s.HTTP = &http.Client{
			Timeout: time.Duration(s.Config.RequestTimeout * float64(time.Second)),
		}
	}
	managerCtx, cancel := context.WithCancel(ctx)
	s.managerCancel = cancel
	s.managerWG.Add(1)
	go func() {
		defer s.managerWG.Done()
		s.managerLoop(managerCtx)
	}()
	return nil
}

// Stop shuts down the manager, closes sessions and closes the database.
func (s *Service) Stop() {
	if s.managerCancel != nil {
		s.managerCancel()
		s.managerWG.Wait()
		s.managerCancel = nil
	}
	s.mu.Lock()
	sessions := make([]*Session, 0, len(s.sessions))
	for _, session := range s.sessions {
		sessions = append(sessions, session)
	}
	s.mu.Unlock()
	for _, session := range sessions {
		session.Close()
		_ = session.sink.Close()
	}
	_ = s.DB.Close()
}

// Authenticate returns the tenant for a plaintext token, or nil.
func (s *Service) Authenticate(token string) (*store.Tenant, error) {
	if token == "" {
		return nil, nil
	}
	return s.DB.TenantForToken(HashToken(token))
}

// --- subscription manager --------------------------------------------------

func (s *Service) managerLoop(ctx context.Context) {
	ticker := time.NewTicker(s.Config.ManagerInterval.Duration())
	defer ticker.Stop()
	for {
		s.managerIteration(ctx)
		select {
		case <-ticker.C:
		case <-ctx.Done():
			return
		}
	}
}

func (s *Service) managerIteration(ctx context.Context) {
	if err := s.renewDue(ctx); err != nil {
		s.Logger.Error("renewing subscriptions failed", "error", err)
	}
	if err := s.retryPending(ctx); err != nil {
		s.Logger.Error("retrying subscriptions failed", "error", err)
	}
	if err := s.DB.PruneDeliveries(
		store.ToISO(store.UTCNow().Add(-s.Config.Retention.Duration())),
	); err != nil {
		s.Logger.Error("pruning deliveries failed", "error", err)
	}
}

func (s *Service) renewDue(ctx context.Context) error {
	threshold := store.ToISO(store.UTCNow().Add(s.Config.RenewMargin.Duration()))
	subscriptions, err := s.DB.SubscriptionsDue(threshold)
	if err != nil {
		return err
	}
	for _, subscription := range subscriptions {
		s.Logger.Info("renewing subscription", "hub", subscription.Hub, "topic", subscription.Topic)
		s.postSubscribe(ctx, subscription, true)
	}
	return nil
}

func (s *Service) retryPending(ctx context.Context) error {
	now := store.UTCNow()
	subscriptions, err := s.DB.SubscriptionsRetryable(
		store.ToISO(now), store.ToISO(now.Add(-15*time.Minute)),
	)
	if err != nil {
		return err
	}
	for _, subscription := range subscriptions {
		s.postSubscribe(ctx, subscription, false)
	}
	return nil
}

func (s *Service) callbackURL(token string) string {
	return strings.TrimRight(s.Config.PublicURL, "/") + "/websub/cb/" + token
}

func (s *Service) postSubscribe(ctx context.Context, subscription store.Subscription, renewing bool) {
	attempt := subscription.Attempt + 1
	var lastError *string
	if subscription.LastError.Valid {
		lastError = &subscription.LastError.String
	}
	if err := s.DB.MarkPending(subscription.Hub, subscription.Topic, attempt, lastError); err != nil {
		s.Logger.Error("marking subscription pending failed", "error", err)
		return
	}
	response, err := hub.Subscribe(
		ctx,
		s.HTTP,
		subscription.Hub,
		s.callbackURL(subscription.CallbackToken),
		subscription.Topic,
		subscription.Secret.String,
		s.Config.LeaseSeconds,
	)
	if err == nil {
		defer response.Body.Close()
		if response.StatusCode >= 400 {
			err = fmt.Errorf("subscription request failed with status %s", response.Status)
		}
	}
	if err != nil {
		backoff := 1 << min(attempt, 12)
		if backoff > 3600 {
			backoff = 3600
		}
		message := truncate(err.Error(), 500)
		if markErr := s.DB.MarkError(
			subscription.Hub, subscription.Topic, message,
			store.ToISO(store.UTCNow().Add(time.Duration(backoff)*time.Second)),
		); markErr != nil {
			s.Logger.Error("marking subscription error failed", "error", markErr)
		}
		s.Logger.Warn("subscribe failed", "topic", subscription.Topic, "error", err)
		return
	}
	if renewing {
		// Renewals are not re-verified by the hub; extend optimistically.
		leaseExpiry := store.ToISO(
			store.UTCNow().Add(time.Duration(s.Config.LeaseSeconds) * time.Second),
		)
		if err := s.DB.MarkActive(subscription.Hub, subscription.Topic, leaseExpiry); err != nil {
			s.Logger.Error("marking subscription active failed", "error", err)
		}
	}
	s.Logger.Info("subscribed", "topic", subscription.Topic, "hub", subscription.Hub)
}

func (s *Service) unsubscribe(ctx context.Context, hubURL, topic string) {
	subscription, err := s.DB.GetSubscription(hubURL, topic)
	if err != nil || subscription == nil {
		return
	}
	response, err := hub.Unsubscribe(
		ctx, s.HTTP, hubURL, s.callbackURL(subscription.CallbackToken), topic,
	)
	if err == nil {
		response.Body.Close()
		if response.StatusCode >= 400 {
			err = fmt.Errorf("unsubscribe failed with status %s", response.Status)
		}
	}
	if err != nil {
		s.Logger.Warn("unsubscribe failed", "topic", topic, "error", err)
	}
	if err := s.DB.DeleteSubscription(hubURL, topic); err != nil {
		s.Logger.Error("deleting subscription failed", "error", err)
	}
}

func (s *Service) resolveHub(ctx context.Context, topic string) string {
	if s.Config.DefaultHub != "" {
		return s.Config.DefaultHub
	}
	return hub.DiscoverHub(ctx, s.HTTP, topic, "")
}

// SyncWatches applies a client's full desired watch set.
func (s *Service) SyncWatches(ctx context.Context, tenantID string, targets []protocol.WatchTarget) error {
	type key struct{ hub, topic string }
	desired := map[key]bool{}

	for _, target := range targets {
		if target.Topic == "" {
			continue
		}
		topic := topics.NormalizeTopic(target.Topic)
		hubURL := s.resolveHub(ctx, target.Topic)
		desired[key{hubURL, topic}] = true

		routingMap := target.Routing
		if routingMap == nil {
			routingMap = map[string]string{}
		}
		routing, err := json.Marshal(routingMap)
		if err != nil {
			return err
		}
		if err := s.DB.UpsertWatch(tenantID, hubURL, topic, string(routing)); err != nil {
			return err
		}
		subscription, err := s.DB.GetSubscription(hubURL, topic)
		if err != nil {
			return err
		}
		if subscription == nil {
			callbackToken, err := randomToken()
			if err != nil {
				return err
			}
			secret, err := randomToken()
			if err != nil {
				return err
			}
			if err := s.DB.CreateSubscription(hubURL, topic, callbackToken, secret); err != nil {
				return err
			}
			newSubscription, err := s.DB.GetSubscription(hubURL, topic)
			if err != nil {
				return err
			}
			if newSubscription != nil {
				s.postSubscribe(ctx, *newSubscription, false)
			}
		} else if subscription.Status == "error" {
			s.postSubscribe(ctx, *subscription, false)
		}
	}

	watches, err := s.DB.WatchesForTenant(tenantID)
	if err != nil {
		return err
	}
	for _, watch := range watches {
		current := key{watch.Hub, watch.Topic}
		if desired[current] {
			continue
		}
		if err := s.DB.DeleteWatch(tenantID, watch.Hub, watch.Topic); err != nil {
			return err
		}
		count, err := s.DB.WatchCount(watch.Hub, watch.Topic)
		if err != nil {
			return err
		}
		if count == 0 {
			s.unsubscribe(ctx, watch.Hub, watch.Topic)
		}
	}
	return nil
}

// --- WebSub callbacks ------------------------------------------------------

// Verify handles a hub subscription verification request.
func (s *Service) Verify(ctx context.Context, token, mode, topic, leaseSeconds string) (bool, error) {
	subscription, err := s.DB.SubscriptionForToken(token)
	if err != nil || subscription == nil {
		return false, err
	}
	if mode != "subscribe" && mode != "unsubscribe" {
		return false, nil
	}
	if topic != "" && topics.NormalizeTopic(topic) != subscription.Topic {
		s.Logger.Warn(
			"callback topic mismatch",
			"got", topics.NormalizeTopic(topic), "want", subscription.Topic,
		)
		return false, nil
	}
	if mode == "subscribe" {
		lease := s.Config.LeaseSeconds
		if leaseSeconds != "" {
			if parsed, parseErr := strconv.Atoi(leaseSeconds); parseErr == nil {
				lease = parsed
			}
		}
		leaseExpiry := store.ToISO(
			store.UTCNow().Add(time.Duration(lease) * time.Second),
		)
		if err := s.DB.MarkActive(subscription.Hub, subscription.Topic, leaseExpiry); err != nil {
			return false, err
		}
		s.Logger.Info("verified subscription", "topic", subscription.Topic)
	} else {
		if err := s.DB.DeleteSubscription(subscription.Hub, subscription.Topic); err != nil {
			return false, err
		}
		s.Logger.Info("unsubscribed", "topic", subscription.Topic)
	}
	return true, nil
}

// Push handles a hub content delivery.  accepted is false when the signature is
// invalid; known is false when the callback token is unknown.
func (s *Service) Push(
	ctx context.Context,
	token string,
	body []byte,
	contentType, signature string,
) (accepted bool, known bool, err error) {
	subscription, err := s.DB.SubscriptionForToken(token)
	if err != nil {
		return false, false, err
	}
	if subscription == nil {
		return false, false, nil
	}
	if subscription.Secret.Valid && subscription.Secret.String != "" &&
		!protocol.VerifyHubSignature(subscription.Secret.String, body, signature) {
		s.Logger.Warn("invalid hub signature", "topic", subscription.Topic)
		return false, true, nil
	}
	var contentTypePtr, signaturePtr *string
	if contentType != "" {
		contentTypePtr = &contentType
	}
	if signature != "" {
		signaturePtr = &signature
	}
	deliveryID, err := s.DB.InsertDelivery(
		subscription.Hub, subscription.Topic, contentTypePtr, signaturePtr, body,
	)
	if err != nil {
		return false, true, err
	}
	tenantIDs, err := s.DB.Fanout(deliveryID, subscription.Hub, subscription.Topic)
	if err != nil {
		return false, true, err
	}
	s.mu.Lock()
	sessions := make([]*Session, 0, len(tenantIDs))
	for _, tenantID := range tenantIDs {
		if session, ok := s.sessions[tenantID]; ok {
			sessions = append(sessions, session)
		}
	}
	s.mu.Unlock()
	for _, session := range sessions {
		session.SignalWake()
	}
	s.Logger.Info("accepted delivery", "id", deliveryID, "topic", subscription.Topic)
	return true, true, nil
}

// --- control sessions ------------------------------------------------------

// OpenSession registers a new session, replacing any existing one for the tenant.
func (s *Service) OpenSession(tenantID, deliveryKey string, lastAcked int64, sink MessageSink) *Session {
	s.mu.Lock()
	if existing, ok := s.sessions[tenantID]; ok {
		existing.Close()
	}
	session := newSession(tenantID, deliveryKey, lastAcked, sink)
	s.sessions[tenantID] = session
	s.mu.Unlock()
	return session
}

// CloseSession removes a session if it is still the current one.
func (s *Service) CloseSession(session *Session) {
	session.Close()
	s.mu.Lock()
	if s.sessions[session.TenantID] == session {
		delete(s.sessions, session.TenantID)
	}
	s.mu.Unlock()
}

// SessionCount returns the number of active sessions.
func (s *Service) SessionCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.sessions)
}

// WatchStates returns the current subscription state for a tenant.
func (s *Service) WatchStates(tenantID string) ([]protocol.WatchState, error) {
	rows, err := s.DB.Conn().Query(
		`SELECT w.topic, w.hub, s.status, s.lease_expires, s.last_error
		 FROM tenant_watches w
		 LEFT JOIN hub_subscriptions s ON s.hub = w.hub AND s.topic = w.topic
		 WHERE w.tenant_id = ? ORDER BY w.topic`,
		tenantID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	states := []protocol.WatchState{}
	for rows.Next() {
		var topic, hubURL string
		var status, leaseExpires, lastError *string
		if err := rows.Scan(&topic, &hubURL, &status, &leaseExpires, &lastError); err != nil {
			return nil, err
		}
		state := protocol.WatchState{Topic: topic, Hub: &hubURL}
		if status != nil {
			state.Status = *status
		} else {
			state.Status = "unknown"
		}
		state.LeaseExpires = leaseExpires
		state.LastError = lastError
		states = append(states, state)
	}
	return states, rows.Err()
}

// BuildWelcome constructs the welcome message for a session.
func (s *Service) BuildWelcome(session *Session) (protocol.Welcome, error) {
	oldest, err := s.DB.OldestDeliveryID()
	if err != nil {
		return protocol.Welcome{}, err
	}
	resumeOK := oldest == nil || session.LastAcked() >= *oldest-1
	var oldestAvailable int64
	if oldest != nil {
		oldestAvailable = *oldest
	}
	watchStates, err := s.WatchStates(session.TenantID)
	if err != nil {
		return protocol.Welcome{}, err
	}
	sessionID, err := randomHex(8)
	if err != nil {
		return protocol.Welcome{}, err
	}
	return protocol.Welcome{
		ProtocolVersion:   protocol.ProtocolVersion,
		Session:           sessionID,
		ResumeOK:          resumeOK,
		OldestAvailableID: oldestAvailable,
		ServerTime:        store.ToISO(store.UTCNow()),
		Watches:           watchStates,
	}, nil
}

// SendWatchStates pushes a watch.state update to a session.
func (s *Service) SendWatchStates(session *Session) error {
	watchStates, err := s.WatchStates(session.TenantID)
	if err != nil {
		return err
	}
	return session.SendJSON(protocol.WatchStateUpdate{Watches: watchStates})
}

// HandleClientMessage dispatches a decoded client message.
func (s *Service) HandleClientMessage(ctx context.Context, session *Session, message protocol.ClientMessage) error {
	switch msg := message.(type) {
	case protocol.WatchSync:
		if err := s.SyncWatches(ctx, session.TenantID, msg.Topics); err != nil {
			return err
		}
		return s.SendWatchStates(session)
	case protocol.Resume:
		session.setLastAcked(msg.Cursor)
		if err := s.DB.SetLastAcked(session.TenantID, session.LastAcked()); err != nil {
			return err
		}
		session.SignalWake()
	case protocol.Ack:
		session.setLastAcked(msg.UpTo)
		if err := s.DB.SetLastAcked(session.TenantID, session.LastAcked()); err != nil {
			return err
		}
		if err := s.DB.AckUpTo(session.TenantID, session.LastAcked()); err != nil {
			return err
		}
		session.signalAck()
	case protocol.Ping:
		return session.SendJSON(protocol.Pong{TS: msg.TS})
	}
	return nil
}

// DeliverLoop sends pending deliveries to a session until it closes.
func (s *Service) DeliverLoop(ctx context.Context, session *Session) {
	const idleTimeout = 30 * time.Second
	const ackTimeout = 30 * time.Second
	for {
		if session.Closed() || ctx.Err() != nil {
			return
		}
		pending, err := s.DB.NextPendingForTenant(session.TenantID, session.LastAcked())
		if err != nil {
			s.Logger.Error("fetching pending delivery failed", "error", err)
			return
		}
		if pending == nil {
			// Drain a stale wake token before blocking.
			select {
			case <-session.wake:
			default:
			}
			select {
			case <-session.wake:
			case <-time.After(idleTimeout):
			case <-session.closed:
				return
			case <-ctx.Done():
				return
			}
			continue
		}

		body := string(pending.Body)
		routing := map[string]string{}
		if pending.Routing.Valid && pending.Routing.String != "" {
			if err := json.Unmarshal([]byte(pending.Routing.String), &routing); err != nil {
				s.Logger.Warn("decoding routing metadata failed", "error", err)
			}
		}
		var signature *string
		if session.DeliveryKey != "" {
			computed := protocol.ComputeDeliverySignature(
				session.DeliveryKey, session.TenantID, pending.ID, pending.Topic, body,
			)
			signature = &computed
		}
		notification := protocol.Notification{
			ID:          pending.ID,
			Topic:       pending.Topic,
			Routing:     routing,
			ContentType: nullableString(pending.ContentType),
			Signature:   signature,
			ReceivedAt:  pending.ReceivedAt,
			Body:        body,
		}
		if err := session.SendJSON(notification); err != nil {
			return
		}
		waitForAck(session, pending.ID, ackTimeout)
	}
}

func waitForAck(session *Session, id int64, timeout time.Duration) {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for session.LastAcked() < id {
		if session.Closed() {
			return
		}
		select {
		case <-session.ackCh:
		case <-deadline.C:
			return
		case <-session.closed:
			return
		}
	}
}

// --- helpers ---------------------------------------------------------------

func randomBytes(length int) ([]byte, error) {
	buffer := make([]byte, length)
	if _, err := rand.Read(buffer); err != nil {
		return nil, err
	}
	return buffer, nil
}

// randomToken mimics secrets.token_urlsafe(32).
func randomToken() (string, error) {
	buffer, err := randomBytes(32)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(buffer), nil
}

// randomHex mimics secrets.token_hex(length).
func randomHex(length int) (string, error) {
	buffer, err := randomBytes(length)
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(buffer), nil
}

func nullableString(value sql.NullString) *string {
	if !value.Valid {
		return nil
	}
	return &value.String
}

func truncate(value string, length int) string {
	if len(value) <= length {
		return value
	}
	return value[:length]
}
