package store

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestToISO(t *testing.T) {
	cases := []struct {
		value time.Time
		want  string
	}{
		{
			time.Date(2026, 10, 11, 15, 34, 53, 918967000, time.UTC),
			"2026-10-11T15:34:53.918967+00:00",
		},
		{
			time.Date(2026, 10, 11, 15, 34, 53, 0, time.UTC),
			"2026-10-11T15:34:53+00:00",
		},
	}
	for _, tc := range cases {
		if got := ToISO(tc.value); got != tc.want {
			t.Errorf("ToISO(%v) = %q, want %q", tc.value, got, tc.want)
		}
	}
}

func TestScanNullStringPointer(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "relay.db3"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var nullValue *string
	if err := db.Conn().QueryRow("SELECT NULL").Scan(&nullValue); err != nil {
		t.Fatalf("scanning NULL into *string: %v", err)
	}
	if nullValue != nil {
		t.Fatalf("expected nil, got %q", *nullValue)
	}

	var value *string
	if err := db.Conn().QueryRow("SELECT 'active'").Scan(&value); err != nil {
		t.Fatalf("scanning value into *string: %v", err)
	}
	if value == nil || *value != "active" {
		t.Fatalf("unexpected value: %v", value)
	}
}

func TestDatabaseRoundTrip(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "relay.db3"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	deliveryKey := "key"
	if err := db.UpsertTenant("home", "hash", &deliveryKey, nil); err != nil {
		t.Fatal(err)
	}
	tenant, err := db.TenantForToken("hash")
	if err != nil || tenant == nil || tenant.ID != "home" {
		t.Fatalf("TenantForToken = %+v, %v", tenant, err)
	}

	if err := db.CreateSubscription("hub", "topic", "callback", "secret"); err != nil {
		t.Fatal(err)
	}
	subscription, err := db.GetSubscription("hub", "topic")
	if err != nil || subscription == nil || subscription.Status != "new" {
		t.Fatalf("GetSubscription = %+v, %v", subscription, err)
	}
	if err := db.MarkActive("hub", "topic", ToISO(UTCNow())); err != nil {
		t.Fatal(err)
	}

	routing, _ := json.Marshal(map[string]string{"kind": "youtube"})
	if err := db.UpsertWatch("home", "hub", "topic", string(routing)); err != nil {
		t.Fatal(err)
	}
	if count, _ := db.WatchCount("hub", "topic"); count != 1 {
		t.Fatalf("WatchCount = %d", count)
	}

	id, err := db.InsertDelivery("hub", "topic", nil, nil, []byte("<feed/>"))
	if err != nil {
		t.Fatal(err)
	}
	tenants, err := db.Fanout(id, "hub", "topic")
	if err != nil || len(tenants) != 1 {
		t.Fatalf("Fanout = %v, %v", tenants, err)
	}
	pending, err := db.NextPendingForTenant("home", 0)
	if err != nil || pending == nil {
		t.Fatalf("NextPendingForTenant = %+v, %v", pending, err)
	}
	if string(pending.Body) != "<feed/>" {
		t.Fatalf("unexpected body %q", pending.Body)
	}
	if pending.Routing.String != string(routing) {
		t.Fatalf("unexpected routing %q", pending.Routing.String)
	}
	if err := db.AckUpTo("home", id); err != nil {
		t.Fatal(err)
	}
	if pending, _ := db.NextPendingForTenant("home", 0); pending != nil {
		t.Fatal("expected delivery to be acked")
	}
}

// TestOpenPythonDatabase confirms the Go store can read a relay.db3 written by
// the Python daemon.  Set RELAY_PYTHON_DB to such a file to enable it;
// the database is copied to a temporary location before being opened.
func TestOpenPythonDatabase(t *testing.T) {
	source := os.Getenv("RELAY_PYTHON_DB")
	if source == "" {
		t.Skip("RELAY_PYTHON_DB not set")
	}
	target := filepath.Join(t.TempDir(), "relay.db3")
	copyFile(t, source, target)

	db, err := Open(target)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	subscriptions, err := db.AllSubscriptions()
	if err != nil {
		t.Fatal(err)
	}
	if len(subscriptions) == 0 {
		t.Fatal("expected subscriptions in Python database")
	}
	for _, subscription := range subscriptions {
		if subscription.Topic == "" || subscription.Hub == "" {
			t.Fatalf("unexpected subscription: %+v", subscription)
		}
		if subscription.LeaseExpires.Valid {
			if _, err := time.Parse(time.RFC3339, subscription.LeaseExpires.String); err != nil {
				t.Fatalf("lease_expires not ISO 8601: %q", subscription.LeaseExpires.String)
			}
		}
	}

	// The Python daemon stores routing with json.dumps spacing; confirm it
	// decodes with the standard library.
	rows, err := db.Conn().Query("SELECT tenant_id, hub, topic, routing, created_at FROM tenant_watches")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	watches := 0
	for rows.Next() {
		var watch Watch
		if err := rows.Scan(
			&watch.TenantID, &watch.Hub, &watch.Topic, &watch.Routing, &watch.CreatedAt,
		); err != nil {
			t.Fatal(err)
		}
		watches++
		if watch.Routing.Valid {
			var routing map[string]string
			if err := json.Unmarshal([]byte(watch.Routing.String), &routing); err != nil {
				t.Fatalf("routing %q did not decode: %v", watch.Routing.String, err)
			}
		}
	}
	if watches == 0 {
		t.Fatal("expected watches in Python database")
	}
}

func copyFile(t *testing.T, source, target string) {
	t.Helper()
	in, err := os.Open(source)
	if err != nil {
		t.Fatal(err)
	}
	defer in.Close()
	out, err := os.Create(target)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Close()
	if _, err := io.Copy(out, in); err != nil {
		t.Fatal(err)
	}
}
