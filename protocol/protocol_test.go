package protocol

import (
	"crypto/hmac"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"
)

func ptr(value string) *string { return &value }

// TestWireFormatMatchesPython pins the JSON encoding to fixtures captured from
// the canonical Python implementation (websub_relay.protocol).
func TestWireFormatMatchesPython(t *testing.T) {
	fixtures := []struct {
		name    string
		message any
		want    string
	}{
		{
			"hello",
			Hello{Tenant: "acme", Token: "secret"},
			`{"type":"hello","protocol_version":1,"tenant":"acme","token":"secret"}`,
		},
		{
			"watch.sync",
			WatchSync{Topics: []WatchTarget{{
				Topic: "https://example/t", Routing: map[string]string{"k": "v"},
			}}},
			`{"type":"watch.sync","topics":[{"topic":"https://example/t","routing":{"k":"v"}}]}`,
		},
		{"resume", Resume{Cursor: 5}, `{"type":"resume","cursor":5}`},
		{"ack", Ack{UpTo: 9}, `{"type":"ack","up_to":9}`},
		{"ping", Ping{TS: 1.5}, `{"type":"ping","ts":1.5}`},
		{
			"welcome",
			Welcome{Session: "abc", ResumeOK: false},
			`{"type":"welcome","protocol_version":1,"session":"abc","resume_ok":false,` +
				`"oldest_available_id":0,"server_time":"","watches":[]}`,
		},
		{
			"watch.state",
			WatchStateUpdate{Watches: []WatchState{{
				Topic: "t", Status: "active", Hub: ptr("h"),
				LeaseExpires: ptr("2020-01-01T00:00:00+00:00"),
			}}},
			`{"type":"watch.state","watches":[{"topic":"t","status":"active","hub":"h",` +
				`"lease_expires":"2020-01-01T00:00:00+00:00","last_error":null}]}`,
		},
		{
			"notification",
			Notification{ID: 7, Topic: "t", Body: "<feed/>"},
			`{"type":"notification","id":7,"topic":"t","routing":{},` +
				`"content_type":null,"signature":null,"received_at":"","body":"<feed/>"}`,
		},
		{
			"error",
			ErrorMessage{Code: "bad_message", Message: "oops"},
			`{"type":"error","code":"bad_message","message":"oops"}`,
		},
		{"pong", Pong{TS: 3.25}, `{"type":"pong","ts":3.25}`},
	}

	for _, fixture := range fixtures {
		t.Run(fixture.name, func(t *testing.T) {
			got, err := Encode(fixture.message)
			if err != nil {
				t.Fatalf("Encode: %v", err)
			}
			if string(got) != fixture.want {
				t.Errorf("wire mismatch\n got: %s\nwant: %s", got, fixture.want)
			}
		})
	}
}

func TestHelloRoundTrip(t *testing.T) {
	encoded, err := Encode(Hello{Tenant: "acme", Token: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeClient(encoded)
	if err != nil {
		t.Fatal(err)
	}
	hello, ok := decoded.(Hello)
	if !ok {
		t.Fatalf("got %T", decoded)
	}
	if hello.ProtocolVersion != ProtocolVersion || hello.Tenant != "acme" || hello.Token != "secret" {
		t.Fatalf("unexpected hello: %+v", hello)
	}
}

func TestWatchSyncRoundTrip(t *testing.T) {
	message := WatchSync{Topics: []WatchTarget{{
		Topic: "https://example/t", Routing: map[string]string{"k": "v"},
	}}}
	encoded, err := Encode(message)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeClient(encoded)
	if err != nil {
		t.Fatal(err)
	}
	sync, ok := decoded.(WatchSync)
	if !ok {
		t.Fatalf("got %T", decoded)
	}
	if len(sync.Topics) != 1 || sync.Topics[0].Routing["k"] != "v" {
		t.Fatalf("unexpected watch.sync: %+v", sync)
	}
}

func TestServerMessagesRoundTrip(t *testing.T) {
	welcomeBytes, _ := Encode(Welcome{Session: "abc", ResumeOK: false})
	decoded, err := DecodeServer(welcomeBytes)
	if err != nil {
		t.Fatal(err)
	}
	welcome, ok := decoded.(Welcome)
	if !ok || welcome.Session != "abc" || welcome.ResumeOK {
		t.Fatalf("unexpected welcome: %+v", decoded)
	}

	notificationBytes, _ := Encode(Notification{ID: 7, Topic: "t", Body: "<feed/>"})
	decoded, err = DecodeServer(notificationBytes)
	if err != nil {
		t.Fatal(err)
	}
	notification, ok := decoded.(Notification)
	if !ok || notification.ID != 7 || notification.Body != "<feed/>" {
		t.Fatalf("unexpected notification: %+v", decoded)
	}

	statesBytes, _ := Encode(WatchStateUpdate{Watches: []WatchState{{Topic: "t"}}})
	decoded, err = DecodeServer(statesBytes)
	if err != nil {
		t.Fatal(err)
	}
	states, ok := decoded.(WatchStateUpdate)
	if !ok || len(states.Watches) != 1 || states.Watches[0].Topic != "t" {
		t.Fatalf("unexpected watch.state: %+v", decoded)
	}
}

// TestDecodePythonProduces checks that messages captured from a Python client
// (including Python's json.dumps-style routing spacing) decode correctly.
func TestDecodePythonProduces(t *testing.T) {
	raw := `{"type":"watch.sync","topics":[{"topic":"t","routing":{"kind": "youtube", "channel_id": "UCabc"}}]}`
	decoded, err := DecodeClient([]byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	sync, ok := decoded.(WatchSync)
	if !ok {
		t.Fatalf("got %T", decoded)
	}
	if sync.Topics[0].Routing["channel_id"] != "UCabc" {
		t.Fatalf("unexpected routing: %+v", sync.Topics[0].Routing)
	}

	// msgspec accepts an integer where a float is expected.
	pong, err := DecodeServer([]byte(`{"type":"pong","ts":1}`))
	if err != nil {
		t.Fatal(err)
	}
	if pong.(Pong).TS != 1.0 {
		t.Fatalf("unexpected pong: %+v", pong)
	}
}

func TestUnknownMessageTypeErrors(t *testing.T) {
	if _, err := DecodeClient([]byte(`{"type":"nope"}`)); err == nil {
		t.Fatal("expected error for unknown client message type")
	}
	if _, err := DecodeClient([]byte(`{"ts":1}`)); err == nil {
		t.Fatal("expected error for missing type")
	}
}

func TestHubSignature(t *testing.T) {
	body := []byte("<feed/>")
	secret := "topsecret"
	digest := hmac.New(sha256.New, []byte(secret))
	digest.Write(body)
	if !VerifyHubSignature(secret, body, "sha256="+hex.EncodeToString(digest.Sum(nil))) {
		t.Fatal("expected valid sha256 signature")
	}
	if VerifyHubSignature(secret, body, "sha256=deadbeef") {
		t.Fatal("expected invalid signature to be rejected")
	}
	if VerifyHubSignature(secret, body, "") {
		t.Fatal("expected empty signature to be rejected")
	}
	sha1Digest := hmac.New(sha1.New, []byte(secret))
	sha1Digest.Write(body)
	if !VerifyHubSignature(secret, body, "sha1="+hex.EncodeToString(sha1Digest.Sum(nil))) {
		t.Fatal("expected valid sha1 signature")
	}
}

func TestDeliverySignature(t *testing.T) {
	signature := ComputeDeliverySignature("key", "tenant", 3, "topic", "<feed/>")
	if !VerifyDeliverySignature("key", "tenant", 3, "topic", "<feed/>", signature) {
		t.Fatal("expected valid delivery signature")
	}
	if VerifyDeliverySignature("key", "tenant", 4, "topic", "<feed/>", signature) {
		t.Fatal("expected mismatched id to fail")
	}
	if VerifyDeliverySignature("key", "tenant", 3, "topic", "x", "") {
		t.Fatal("expected empty signature to fail")
	}
}

func TestEncodeDoesNotHTMLEscape(t *testing.T) {
	encoded, err := Encode(Notification{Body: `<feed a="b">&</feed>`})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(encoded), `<feed a=\"b\">&</feed>`) {
		t.Fatalf("expected raw XML in body, got %s", encoded)
	}
}
