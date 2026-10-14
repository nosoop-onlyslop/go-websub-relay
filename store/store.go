// Package store provides SQLite persistence for the relay.
//
// The schema and queries mirror websub_relay.db so an existing relay.db3 can be
// opened by either daemon.
package store

import (
	"database/sql"
	"fmt"
	"time"

	_ "modernc.org/sqlite"
)

// UTCNow returns the current time in UTC.
func UTCNow() time.Time { return time.Now().UTC() }

// ToISO formats a time the same way Python's datetime.isoformat() does for a
// UTC datetime: microseconds are included only when non-zero and the offset is
// always "+00:00" (never "Z").
func ToISO(value time.Time) string {
	value = value.UTC()
	if value.Nanosecond()/1000 != 0 {
		return value.Format("2006-01-02T15:04:05.000000-07:00")
	}
	return value.Format("2006-01-02T15:04:05-07:00")
}

var schema = []string{
	`CREATE TABLE IF NOT EXISTS tenants (
  id           TEXT PRIMARY KEY,
  token_hash   TEXT NOT NULL UNIQUE,
  delivery_key TEXT,
  watch_limit  INTEGER,
  last_acked   INTEGER NOT NULL DEFAULT 0,
  created_at   TEXT NOT NULL DEFAULT current_timestamp
)`,
	`CREATE TABLE IF NOT EXISTS hub_subscriptions (
  hub             TEXT NOT NULL,
  topic           TEXT NOT NULL,
  callback_token  TEXT NOT NULL UNIQUE,
  secret          TEXT,
  status          TEXT NOT NULL,
  lease_expires   TEXT,
  attempt         INTEGER NOT NULL DEFAULT 0,
  next_attempt_at TEXT,
  last_error      TEXT,
  created_at      TEXT NOT NULL DEFAULT current_timestamp,
  updated_at      TEXT NOT NULL DEFAULT current_timestamp,
  PRIMARY KEY (hub, topic)
)`,
	`CREATE TABLE IF NOT EXISTS tenant_watches (
  tenant_id  TEXT NOT NULL,
  hub        TEXT NOT NULL,
  topic      TEXT NOT NULL,
  routing    TEXT,
  created_at TEXT NOT NULL DEFAULT current_timestamp,
  PRIMARY KEY (tenant_id, hub, topic)
)`,
	`CREATE TABLE IF NOT EXISTS deliveries (
  id           INTEGER PRIMARY KEY AUTOINCREMENT,
  hub          TEXT NOT NULL,
  topic        TEXT NOT NULL,
  content_type TEXT,
  signature    TEXT,
  body         BLOB NOT NULL,
  received_at  TEXT NOT NULL
)`,
	`CREATE TABLE IF NOT EXISTS tenant_deliveries (
  tenant_id   TEXT NOT NULL,
  delivery_id INTEGER NOT NULL,
  acked_at    TEXT,
  PRIMARY KEY (tenant_id, delivery_id)
)`,
	`CREATE INDEX IF NOT EXISTS tenant_deliveries_pending
  ON tenant_deliveries (tenant_id, acked_at, delivery_id)`,
}

// Tenant is a row from the tenants table.
type Tenant struct {
	ID          string
	TokenHash   string
	DeliveryKey sql.NullString
	WatchLimit  sql.NullInt64
	LastAcked   int64
	CreatedAt   string
}

// Subscription is a row from the hub_subscriptions table.
type Subscription struct {
	Hub           string
	Topic         string
	CallbackToken string
	Secret        sql.NullString
	Status        string
	LeaseExpires  sql.NullString
	Attempt       int
	NextAttemptAt sql.NullString
	LastError     sql.NullString
	CreatedAt     string
	UpdatedAt     string
}

// Watch is a row from the tenant_watches table.
type Watch struct {
	TenantID  string
	Hub       string
	Topic     string
	Routing   sql.NullString
	CreatedAt string
}

// PendingDelivery is a delivery joined with its per-tenant routing metadata.
type PendingDelivery struct {
	ID          int64
	Hub         string
	Topic       string
	ContentType sql.NullString
	Body        []byte
	ReceivedAt  string
	Routing     sql.NullString
}

// DB wraps the SQLite connection.
type DB struct {
	conn *sql.DB
}

// Open opens (creating if needed) the relay database.
func Open(path string) (*DB, error) {
	conn, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	// The Python daemon uses a single synchronous connection; serialising here
	// avoids SQLITE_BUSY between the manager and callback handlers.
	conn.SetMaxOpenConns(1)
	conn.SetMaxIdleConns(1)

	db := &DB{conn: conn}
	if _, err := conn.Exec("PRAGMA journal_mode = wal"); err != nil {
		conn.Close()
		return nil, err
	}
	if _, err := conn.Exec("PRAGMA synchronous = normal"); err != nil {
		conn.Close()
		return nil, err
	}
	for _, statement := range schema {
		if _, err := conn.Exec(statement); err != nil {
			conn.Close()
			return nil, fmt.Errorf("applying schema: %w", err)
		}
	}
	return db, nil
}

// Close closes the underlying database.
func (db *DB) Close() error { return db.conn.Close() }

// Conn exposes the underlying *sql.DB for read-only queries owned by callers.
func (db *DB) Conn() *sql.DB { return db.conn }

// --- tenants ---------------------------------------------------------------

// UpsertTenant inserts or updates a tenant.
func (db *DB) UpsertTenant(tenantID, tokenHash string, deliveryKey *string, watchLimit *int) error {
	_, err := db.conn.Exec(
		`INSERT INTO tenants (id, token_hash, delivery_key, watch_limit)
		 VALUES (?, ?, ?, ?)
		 ON CONFLICT(id) DO UPDATE SET
		 token_hash=excluded.token_hash, delivery_key=excluded.delivery_key,
		 watch_limit=excluded.watch_limit`,
		tenantID, tokenHash, nullableString(deliveryKey), nullableInt(watchLimit),
	)
	return err
}

// TenantForToken looks up a tenant by token hash.
func (db *DB) TenantForToken(tokenHash string) (*Tenant, error) {
	row := db.conn.QueryRow("SELECT * FROM tenants WHERE token_hash = ?", tokenHash)
	tenant, err := scanTenant(row)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return tenant, err
}

// SetLastAcked records the highest acked delivery id for a tenant.
func (db *DB) SetLastAcked(tenantID string, value int64) error {
	_, err := db.conn.Exec("UPDATE tenants SET last_acked = ? WHERE id = ?", value, tenantID)
	return err
}

// --- hub subscriptions -----------------------------------------------------

// GetSubscription returns the subscription for (hub, topic), or nil.
func (db *DB) GetSubscription(hub, topic string) (*Subscription, error) {
	row := db.conn.QueryRow(
		"SELECT * FROM hub_subscriptions WHERE hub = ? AND topic = ?", hub, topic,
	)
	sub, err := scanSubscription(row)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return sub, err
}

// SubscriptionForToken returns the subscription for a callback token, or nil.
func (db *DB) SubscriptionForToken(token string) (*Subscription, error) {
	row := db.conn.QueryRow(
		"SELECT * FROM hub_subscriptions WHERE callback_token = ?", token,
	)
	sub, err := scanSubscription(row)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return sub, err
}

// CreateSubscription inserts a new subscription if one does not already exist.
func (db *DB) CreateSubscription(hub, topic, callbackToken, secret string) error {
	_, err := db.conn.Exec(
		`INSERT INTO hub_subscriptions
		 (hub, topic, callback_token, secret, status, attempt, next_attempt_at)
		 VALUES (?, ?, ?, ?, 'new', 0, ?)
		 ON CONFLICT(hub, topic) DO NOTHING`,
		hub, topic, callbackToken, secret, ToISO(UTCNow()),
	)
	return err
}

// MarkPending marks a subscription as pending a hub response.
func (db *DB) MarkPending(hub, topic string, attempt int, lastError *string) error {
	_, err := db.conn.Exec(
		`UPDATE hub_subscriptions SET status = 'pending', attempt = ?,
		 last_error = ?, updated_at = ? WHERE hub = ? AND topic = ?`,
		attempt, nullableString(lastError), ToISO(UTCNow()), hub, topic,
	)
	return err
}

// MarkActive marks a subscription as active with the given lease expiry.
func (db *DB) MarkActive(hub, topic, leaseExpires string) error {
	_, err := db.conn.Exec(
		`UPDATE hub_subscriptions SET status = 'active', lease_expires = ?,
		 attempt = 0, next_attempt_at = NULL, last_error = NULL, updated_at = ?
		 WHERE hub = ? AND topic = ?`,
		leaseExpires, ToISO(UTCNow()), hub, topic,
	)
	return err
}

// MarkError records a failure and the next retry time.
func (db *DB) MarkError(hub, topic, lastError, nextAttemptAt string) error {
	_, err := db.conn.Exec(
		`UPDATE hub_subscriptions SET status = 'error', last_error = ?,
		 next_attempt_at = ?, updated_at = ? WHERE hub = ? AND topic = ?`,
		lastError, nextAttemptAt, ToISO(UTCNow()), hub, topic,
	)
	return err
}

// DeleteSubscription removes a hub subscription.
func (db *DB) DeleteSubscription(hub, topic string) error {
	_, err := db.conn.Exec(
		"DELETE FROM hub_subscriptions WHERE hub = ? AND topic = ?", hub, topic,
	)
	return err
}

// SubscriptionsDue returns active subscriptions whose lease expires before the
// given threshold.
func (db *DB) SubscriptionsDue(thresholdISO string) ([]Subscription, error) {
	rows, err := db.conn.Query(
		`SELECT * FROM hub_subscriptions WHERE status = 'active'
		 AND lease_expires IS NOT NULL AND lease_expires <= ?`,
		thresholdISO,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanSubscriptions(rows)
}

// SubscriptionsRetryable returns subscriptions that should be (re)subscribed.
func (db *DB) SubscriptionsRetryable(nowISO, pendingBeforeISO string) ([]Subscription, error) {
	rows, err := db.conn.Query(
		`SELECT * FROM hub_subscriptions WHERE
		 (status IN ('new', 'error') AND
		 (next_attempt_at IS NULL OR next_attempt_at <= ?))
		 OR (status = 'pending' AND updated_at <= ?)`,
		nowISO, pendingBeforeISO,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanSubscriptions(rows)
}

// AllSubscriptions returns every hub subscription.
func (db *DB) AllSubscriptions() ([]Subscription, error) {
	rows, err := db.conn.Query("SELECT * FROM hub_subscriptions")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanSubscriptions(rows)
}

// --- watches ---------------------------------------------------------------

// UpsertWatch inserts or updates a tenant's watch on a topic.
func (db *DB) UpsertWatch(tenantID, hub, topic, routing string) error {
	_, err := db.conn.Exec(
		`INSERT INTO tenant_watches (tenant_id, hub, topic, routing)
		 VALUES (?, ?, ?, ?)
		 ON CONFLICT(tenant_id, hub, topic) DO UPDATE SET routing=excluded.routing`,
		tenantID, hub, topic, routing,
	)
	return err
}

// DeleteWatch removes a tenant's watch.
func (db *DB) DeleteWatch(tenantID, hub, topic string) error {
	_, err := db.conn.Exec(
		"DELETE FROM tenant_watches WHERE tenant_id = ? AND hub = ? AND topic = ?",
		tenantID, hub, topic,
	)
	return err
}

// WatchesForTenant returns every watch for a tenant.
func (db *DB) WatchesForTenant(tenantID string) ([]Watch, error) {
	rows, err := db.conn.Query(
		"SELECT * FROM tenant_watches WHERE tenant_id = ?", tenantID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var watches []Watch
	for rows.Next() {
		var watch Watch
		if err := rows.Scan(
			&watch.TenantID, &watch.Hub, &watch.Topic, &watch.Routing, &watch.CreatedAt,
		); err != nil {
			return nil, err
		}
		watches = append(watches, watch)
	}
	return watches, rows.Err()
}

// WatchCount returns how many tenants watch (hub, topic).
func (db *DB) WatchCount(hub, topic string) (int, error) {
	var count int
	err := db.conn.QueryRow(
		"SELECT COUNT(*) FROM tenant_watches WHERE hub = ? AND topic = ?", hub, topic,
	).Scan(&count)
	return count, err
}

// --- deliveries ------------------------------------------------------------

// InsertDelivery stores a raw hub delivery and returns its id.
func (db *DB) InsertDelivery(hub, topic string, contentType, signature *string, body []byte) (int64, error) {
	result, err := db.conn.Exec(
		`INSERT INTO deliveries (hub, topic, content_type, signature, body, received_at)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		hub, topic, nullableString(contentType), nullableString(signature), body, ToISO(UTCNow()),
	)
	if err != nil {
		return 0, err
	}
	return result.LastInsertId()
}

// Fanout queues a delivery for every tenant watching (hub, topic), returning
// those tenant ids.
func (db *DB) Fanout(deliveryID int64, hub, topic string) ([]string, error) {
	rows, err := db.conn.Query(
		"SELECT tenant_id FROM tenant_watches WHERE hub = ? AND topic = ?", hub, topic,
	)
	if err != nil {
		return nil, err
	}
	var tenantIDs []string
	for rows.Next() {
		var tenantID string
		if err := rows.Scan(&tenantID); err != nil {
			rows.Close()
			return nil, err
		}
		tenantIDs = append(tenantIDs, tenantID)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()

	tx, err := db.conn.Begin()
	if err != nil {
		return nil, err
	}
	statement, err := tx.Prepare(
		"INSERT OR IGNORE INTO tenant_deliveries (tenant_id, delivery_id) VALUES (?, ?)",
	)
	if err != nil {
		tx.Rollback()
		return nil, err
	}
	for _, tenantID := range tenantIDs {
		if _, err := statement.Exec(tenantID, deliveryID); err != nil {
			statement.Close()
			tx.Rollback()
			return nil, err
		}
	}
	statement.Close()
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return tenantIDs, nil
}

// NextPendingForTenant returns the oldest unacked delivery after afterID, or nil.
func (db *DB) NextPendingForTenant(tenantID string, afterID int64) (*PendingDelivery, error) {
	row := db.conn.QueryRow(
		`SELECT d.id, d.hub, d.topic, d.content_type, d.body, d.received_at,
		 w.routing AS routing
		 FROM tenant_deliveries td
		 JOIN deliveries d ON d.id = td.delivery_id
		 LEFT JOIN tenant_watches w
		   ON w.tenant_id = td.tenant_id AND w.hub = d.hub AND w.topic = d.topic
		 WHERE td.tenant_id = ? AND td.acked_at IS NULL AND d.id > ?
		 ORDER BY d.id ASC LIMIT 1`,
		tenantID, afterID,
	)
	var pending PendingDelivery
	err := row.Scan(
		&pending.ID, &pending.Hub, &pending.Topic, &pending.ContentType,
		&pending.Body, &pending.ReceivedAt, &pending.Routing,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &pending, nil
}

// AckUpTo marks all of a tenant's deliveries up to upTo as acknowledged.
func (db *DB) AckUpTo(tenantID string, upTo int64) error {
	_, err := db.conn.Exec(
		`UPDATE tenant_deliveries SET acked_at = ?
		 WHERE tenant_id = ? AND delivery_id <= ? AND acked_at IS NULL`,
		ToISO(UTCNow()), tenantID, upTo,
	)
	return err
}

// OldestDeliveryID returns the smallest retained delivery id, or nil.
func (db *DB) OldestDeliveryID() (*int64, error) {
	var oldest sql.NullInt64
	if err := db.conn.QueryRow("SELECT MIN(id) FROM deliveries").Scan(&oldest); err != nil {
		return nil, err
	}
	if !oldest.Valid {
		return nil, nil
	}
	return &oldest.Int64, nil
}

// PruneDeliveries removes deliveries older than beforeISO that have no pending
// tenant deliveries, then removes orphaned tenant_deliveries rows.
func (db *DB) PruneDeliveries(beforeISO string) error {
	tx, err := db.conn.Begin()
	if err != nil {
		return err
	}
	if _, err := tx.Exec(
		`DELETE FROM deliveries WHERE received_at < ? AND NOT EXISTS (
		   SELECT 1 FROM tenant_deliveries td
		   WHERE td.delivery_id = deliveries.id AND td.acked_at IS NULL
		 )`,
		beforeISO,
	); err != nil {
		tx.Rollback()
		return err
	}
	if _, err := tx.Exec(
		`DELETE FROM tenant_deliveries WHERE delivery_id NOT IN
		 (SELECT id FROM deliveries)`,
	); err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}

// --- scan helpers ----------------------------------------------------------

type rowScanner interface {
	Scan(dest ...any) error
}

func scanTenant(row rowScanner) (*Tenant, error) {
	var tenant Tenant
	err := row.Scan(
		&tenant.ID, &tenant.TokenHash, &tenant.DeliveryKey, &tenant.WatchLimit,
		&tenant.LastAcked, &tenant.CreatedAt,
	)
	if err != nil {
		return nil, err
	}
	return &tenant, nil
}

func scanSubscription(row rowScanner) (*Subscription, error) {
	var sub Subscription
	err := row.Scan(
		&sub.Hub, &sub.Topic, &sub.CallbackToken, &sub.Secret, &sub.Status,
		&sub.LeaseExpires, &sub.Attempt, &sub.NextAttemptAt, &sub.LastError,
		&sub.CreatedAt, &sub.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}
	return &sub, nil
}

func scanSubscriptions(rows *sql.Rows) ([]Subscription, error) {
	var subscriptions []Subscription
	for rows.Next() {
		sub, err := scanSubscription(rows)
		if err != nil {
			return nil, err
		}
		subscriptions = append(subscriptions, *sub)
	}
	return subscriptions, rows.Err()
}

func nullableString(value *string) any {
	if value == nil {
		return nil
	}
	return *value
}

func nullableInt(value *int) any {
	if value == nil {
		return nil
	}
	return *value
}
