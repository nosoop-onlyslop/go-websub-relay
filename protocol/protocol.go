// Package protocol implements the websub-relay control protocol.
//
// This is the Go counterpart of websub_relay.protocol.  The JSON wire format
// must stay byte-compatible (in the encoding/json sense) with the canonical
// Python implementation so that Python clients and tenants can talk to this
// daemon unchanged.
//
// Any behavioural change must bump ProtocolVersion.
package protocol

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"strings"
)

// ProtocolVersion is the version of the control protocol spoken by this build.
const ProtocolVersion = 1

// --- signatures ------------------------------------------------------------

var hubHashes = map[string]func() hash.Hash{
	"sha1":   sha1.New,
	"sha256": sha256.New,
}

// VerifyHubSignature verifies an X-Hub-Signature / X-Hub-Signature-256 value
// produced by a WebSub hub using the subscription secret.
func VerifyHubSignature(secret string, body []byte, signature string) bool {
	if secret == "" || signature == "" {
		return false
	}
	algorithm, provided, ok := strings.Cut(signature, "=")
	if !ok {
		return false
	}
	newHash, ok := hubHashes[strings.ToLower(strings.TrimSpace(algorithm))]
	if !ok {
		return false
	}
	mac := hmac.New(newHash, []byte(secret))
	mac.Write(body)
	expected := hex.EncodeToString(mac.Sum(nil))
	return hmac.Equal([]byte(expected), []byte(strings.ToLower(strings.TrimSpace(provided))))
}

func deliveryPayload(tenantID string, deliveryID int64, topic, body string) []byte {
	return []byte(fmt.Sprintf("%s\n%d\n%s\n%s", tenantID, deliveryID, topic, body))
}

// ComputeDeliverySignature computes the optional relay -> client HMAC-SHA256
// signature for a notification.
func ComputeDeliverySignature(key, tenantID string, deliveryID int64, topic, body string) string {
	mac := hmac.New(sha256.New, []byte(key))
	mac.Write(deliveryPayload(tenantID, deliveryID, topic, body))
	return hex.EncodeToString(mac.Sum(nil))
}

// VerifyDeliverySignature checks a signature produced by ComputeDeliverySignature.
func VerifyDeliverySignature(key, tenantID string, deliveryID int64, topic, body, signature string) bool {
	if key == "" || signature == "" {
		return false
	}
	expected := ComputeDeliverySignature(key, tenantID, deliveryID, topic, body)
	return hmac.Equal([]byte(expected), []byte(signature))
}

// --- JSON helpers ----------------------------------------------------------

// marshal encodes v as compact JSON without Go's default HTML escaping, which
// matches msgspec's output (e.g. a feed body containing "<feed/>").
func marshal(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimRight(buf.Bytes(), "\n"), nil
}

// Encode encodes a client or server message to its JSON representation.
func Encode(message any) ([]byte, error) {
	return marshal(message)
}

// --- control messages ------------------------------------------------------

// ClientMessage is implemented by every message a client sends to the relay.
type ClientMessage interface {
	isClientMessage()
}

// ServerMessage is implemented by every message the relay sends to a client.
type ServerMessage interface {
	isServerMessage()
}

// Hello is the first message on a control connection; it authenticates the tenant.
type Hello struct {
	ProtocolVersion int    `json:"protocol_version"`
	Tenant          string `json:"tenant"`
	Token           string `json:"token"`
}

func (Hello) isClientMessage() {}

func (m Hello) MarshalJSON() ([]byte, error) {
	if m.ProtocolVersion == 0 {
		m.ProtocolVersion = ProtocolVersion
	}
	type plain Hello
	return marshal(struct {
		Type string `json:"type"`
		plain
	}{"hello", plain(m)})
}

// WatchTarget is one topic the client wants to watch.
type WatchTarget struct {
	Topic   string            `json:"topic"`
	Routing map[string]string `json:"routing"`
}

// WatchSync is the full desired set of topics; idempotent across reconnects.
type WatchSync struct {
	Topics []WatchTarget `json:"topics"`
}

func (WatchSync) isClientMessage() {}

func (m WatchSync) MarshalJSON() ([]byte, error) {
	if m.Topics == nil {
		m.Topics = []WatchTarget{}
	}
	for i := range m.Topics {
		if m.Topics[i].Routing == nil {
			m.Topics[i].Routing = map[string]string{}
		}
	}
	type plain WatchSync
	return marshal(struct {
		Type string `json:"type"`
		plain
	}{"watch.sync", plain(m)})
}

// Resume sets the delivery cursor before replaying missed notifications.
type Resume struct {
	Cursor int64 `json:"cursor"`
}

func (Resume) isClientMessage() {}

func (m Resume) MarshalJSON() ([]byte, error) {
	type plain Resume
	return marshal(struct {
		Type string `json:"type"`
		plain
	}{"resume", plain(m)})
}

// Ack acknowledges deliveries, allowing the relay to prune them.
type Ack struct {
	UpTo int64 `json:"up_to"`
}

func (Ack) isClientMessage() {}

func (m Ack) MarshalJSON() ([]byte, error) {
	type plain Ack
	return marshal(struct {
		Type string `json:"type"`
		plain
	}{"ack", plain(m)})
}

// Ping is an application-level keepalive; the relay answers with Pong.
type Ping struct {
	TS float64 `json:"ts"`
}

func (Ping) isClientMessage() {}

func (m Ping) MarshalJSON() ([]byte, error) {
	type plain Ping
	return marshal(struct {
		Type string `json:"type"`
		plain
	}{"ping", plain(m)})
}

// WatchState is the current status of one hub subscription.
type WatchState struct {
	Topic        string  `json:"topic"`
	Status       string  `json:"status"`
	Hub          *string `json:"hub"`
	LeaseExpires *string `json:"lease_expires"`
	LastError    *string `json:"last_error"`
}

// Welcome is the first message from the relay; the session is ready once it is
// received.
type Welcome struct {
	ProtocolVersion   int          `json:"protocol_version"`
	Session           string       `json:"session"`
	ResumeOK          bool         `json:"resume_ok"`
	OldestAvailableID int64        `json:"oldest_available_id"`
	ServerTime        string       `json:"server_time"`
	Watches           []WatchState `json:"watches"`
}

func (Welcome) isServerMessage() {}

func (m Welcome) MarshalJSON() ([]byte, error) {
	if m.ProtocolVersion == 0 {
		m.ProtocolVersion = ProtocolVersion
	}
	if m.Watches == nil {
		m.Watches = []WatchState{}
	}
	type plain Welcome
	return marshal(struct {
		Type string `json:"type"`
		plain
	}{"welcome", plain(m)})
}

// WatchStateUpdate is pushed after a WatchSync to report subscription state.
type WatchStateUpdate struct {
	Watches []WatchState `json:"watches"`
}

func (WatchStateUpdate) isServerMessage() {}

func (m WatchStateUpdate) MarshalJSON() ([]byte, error) {
	if m.Watches == nil {
		m.Watches = []WatchState{}
	}
	type plain WatchStateUpdate
	return marshal(struct {
		Type string `json:"type"`
		plain
	}{"watch.state", plain(m)})
}

// Notification is a verified feed delivery, forwarded to interested tenants.
type Notification struct {
	ID          int64             `json:"id"`
	Topic       string            `json:"topic"`
	Routing     map[string]string `json:"routing"`
	ContentType *string           `json:"content_type"`
	Signature   *string           `json:"signature"`
	ReceivedAt  string            `json:"received_at"`
	Body        string            `json:"body"`
}

func (Notification) isServerMessage() {}

func (m Notification) MarshalJSON() ([]byte, error) {
	if m.Routing == nil {
		m.Routing = map[string]string{}
	}
	type plain Notification
	return marshal(struct {
		Type string `json:"type"`
		plain
	}{"notification", plain(m)})
}

// ErrorMessage is a non-fatal protocol or request error.
type ErrorMessage struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (ErrorMessage) isServerMessage() {}

func (m ErrorMessage) MarshalJSON() ([]byte, error) {
	type plain ErrorMessage
	return marshal(struct {
		Type string `json:"type"`
		plain
	}{"error", plain(m)})
}

// Pong is the response to a Ping.
type Pong struct {
	TS float64 `json:"ts"`
}

func (Pong) isServerMessage() {}

func (m Pong) MarshalJSON() ([]byte, error) {
	type plain Pong
	return marshal(struct {
		Type string `json:"type"`
		plain
	}{"pong", plain(m)})
}

// --- decoding --------------------------------------------------------------

func messageType(data []byte) (string, error) {
	var envelope struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(data, &envelope); err != nil {
		return "", err
	}
	if envelope.Type == "" {
		return "", errors.New("object missing required field `type`")
	}
	return envelope.Type, nil
}

// DecodeClient decodes a client -> relay message.
func DecodeClient(data []byte) (ClientMessage, error) {
	tag, err := messageType(data)
	if err != nil {
		return nil, err
	}
	switch tag {
	case "hello":
		m := Hello{ProtocolVersion: ProtocolVersion}
		if err := json.Unmarshal(data, &m); err != nil {
			return nil, err
		}
		return m, nil
	case "watch.sync":
		m := WatchSync{}
		if err := json.Unmarshal(data, &m); err != nil {
			return nil, err
		}
		if m.Topics == nil {
			m.Topics = []WatchTarget{}
		}
		for i := range m.Topics {
			if m.Topics[i].Routing == nil {
				m.Topics[i].Routing = map[string]string{}
			}
		}
		return m, nil
	case "resume":
		var m Resume
		if err := json.Unmarshal(data, &m); err != nil {
			return nil, err
		}
		return m, nil
	case "ack":
		var m Ack
		if err := json.Unmarshal(data, &m); err != nil {
			return nil, err
		}
		return m, nil
	case "ping":
		var m Ping
		if err := json.Unmarshal(data, &m); err != nil {
			return nil, err
		}
		return m, nil
	default:
		return nil, fmt.Errorf("unknown client message type %q", tag)
	}
}

// DecodeServer decodes a relay -> client message.
func DecodeServer(data []byte) (ServerMessage, error) {
	tag, err := messageType(data)
	if err != nil {
		return nil, err
	}
	switch tag {
	case "welcome":
		m := Welcome{ProtocolVersion: ProtocolVersion, ResumeOK: true, Watches: []WatchState{}}
		if err := json.Unmarshal(data, &m); err != nil {
			return nil, err
		}
		if m.Watches == nil {
			m.Watches = []WatchState{}
		}
		return m, nil
	case "watch.state":
		m := WatchStateUpdate{Watches: []WatchState{}}
		if err := json.Unmarshal(data, &m); err != nil {
			return nil, err
		}
		if m.Watches == nil {
			m.Watches = []WatchState{}
		}
		return m, nil
	case "notification":
		var m Notification
		if err := json.Unmarshal(data, &m); err != nil {
			return nil, err
		}
		if m.Routing == nil {
			m.Routing = map[string]string{}
		}
		return m, nil
	case "error":
		var m ErrorMessage
		if err := json.Unmarshal(data, &m); err != nil {
			return nil, err
		}
		return m, nil
	case "pong":
		var m Pong
		if err := json.Unmarshal(data, &m); err != nil {
			return nil, err
		}
		return m, nil
	default:
		return nil, fmt.Errorf("unknown server message type %q", tag)
	}
}
