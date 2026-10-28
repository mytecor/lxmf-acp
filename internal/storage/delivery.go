package storage

import (
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite"
)

// DeliveryStore journals pending traffic independently of identity/config state.
// Only one process may dispatch a database at a time.
type DeliveryStore struct {
	db              *sql.DB
	MaxPendingBytes int64
}

type PendingMessage struct {
	Seq                              int64
	Local, Peer, ID, Wire, Relay     []byte
	Direct, Notice, Control, Stamped bool
	Attempts                         int
	State, LastError                 string
	Next                             time.Time
}

type Incoming struct {
	Seq         int64
	Key         string
	ID, Payload []byte
	State       string
}

func OpenDeliveryStore(path string) (*DeliveryStore, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return nil, err
	}
	// Create before SQLite opens it so sensitive payloads never have default 0644 permissions.
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	f.Close()
	if err := os.Chmod(path, 0600); err != nil {
		return nil, err
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite", (&url.URL{Scheme: "file", Path: absolute}).String())
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	s := &DeliveryStore{db: db, MaxPendingBytes: 128 << 20}
	_, err = db.Exec(`PRAGMA journal_mode=DELETE; PRAGMA locking_mode=EXCLUSIVE; PRAGMA synchronous=FULL; PRAGMA secure_delete=ON; PRAGMA busy_timeout=5000;
 CREATE TABLE IF NOT EXISTS outbox (
 seq INTEGER PRIMARY KEY AUTOINCREMENT, local BLOB NOT NULL, peer BLOB NOT NULL,
 id BLOB NOT NULL, wire BLOB, relay BLOB, direct INTEGER NOT NULL, notice INTEGER NOT NULL, control INTEGER NOT NULL, created INTEGER NOT NULL,
 stamped INTEGER NOT NULL DEFAULT 0, attempts INTEGER NOT NULL DEFAULT 0,
 state TEXT NOT NULL DEFAULT 'queued', next INTEGER NOT NULL DEFAULT 0, error TEXT NOT NULL DEFAULT '',
 UNIQUE(local,peer,id));
 CREATE INDEX IF NOT EXISTS outbox_due ON outbox(state,next);
 CREATE INDEX IF NOT EXISTS outbox_order ON outbox(local,peer,seq);
 CREATE TABLE IF NOT EXISTS inbox (
 seq INTEGER PRIMARY KEY AUTOINCREMENT, chat TEXT NOT NULL, id BLOB NOT NULL,
 payload BLOB, output TEXT NOT NULL DEFAULT '', state TEXT NOT NULL DEFAULT 'buffered', updated INTEGER NOT NULL,
 UNIQUE(chat,id));
 CREATE INDEX IF NOT EXISTS inbox_state ON inbox(state,seq);
 CREATE INDEX IF NOT EXISTS inbox_chat ON inbox(chat,state,seq);
 DELETE FROM outbox WHERE notice=1;
 UPDATE outbox SET state='queued',next=0 WHERE state='sending';
 UPDATE inbox SET state='queued' WHERE state='scheduled';
 UPDATE inbox SET state='uncertain', payload=NULL WHERE state='running';`)
	if err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}
func (s *DeliveryStore) Close() error { return s.db.Close() }

func (s *DeliveryStore) Enqueue(m PendingMessage) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var exists bool
	if err = tx.QueryRow(`SELECT EXISTS(SELECT 1 FROM outbox WHERE local=? AND peer=? AND id=?)`, m.Local, m.Peer, m.ID).Scan(&exists); err != nil {
		return err
	}
	if exists {
		return tx.Commit()
	}
	// Superseded progress notifications are expendable, unlike answer parts.
	if m.Notice {
		if _, err = tx.Exec(`DELETE FROM outbox WHERE local=? AND peer=? AND notice=1 AND state='queued'`, m.Local, m.Peer); err != nil {
			return err
		}
	}
	var payloadBytes int64
	if err = tx.QueryRow(`SELECT COALESCE(SUM(length(wire)+COALESCE(length(relay),0)),0) FROM outbox`).Scan(&payloadBytes); err != nil {
		return err
	}
	if s.MaxPendingBytes > 0 && payloadBytes+int64(len(m.Wire)) > s.MaxPendingBytes {
		return errors.New("outgoing payload budget exceeded; response text retained for recovery")
	}
	_, err = tx.Exec(`INSERT INTO outbox(local,peer,id,wire,direct,notice,control,created,stamped) VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(local,peer,id) DO NOTHING`, m.Local, m.Peer, m.ID, m.Wire, m.Direct, m.Notice, m.Control, time.Now().UnixMilli(), m.Stamped)
	if err != nil {
		return err
	}
	return tx.Commit()
}
func (s *DeliveryStore) Due(now time.Time, limit int) ([]PendingMessage, error) {
	rows, err := s.db.Query(`SELECT o.seq,o.local,o.peer,o.id,o.wire,o.relay,o.direct,o.notice,o.control,o.stamped,o.attempts,o.state,o.next,o.error FROM outbox o
 WHERE o.state='queued' AND o.next<=? AND (o.notice=1 OR o.control=1 OR NOT EXISTS (
 SELECT 1 FROM outbox p WHERE p.local=o.local AND p.peer=o.peer AND p.notice=0 AND p.control=0 AND p.seq<o.seq AND p.state IN ('queued','sending','blocked')))
 ORDER BY o.control DESC,o.notice,o.seq LIMIT ?`, now.UnixMilli(), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []PendingMessage
	for rows.Next() {
		var m PendingMessage
		var next int64
		if err := rows.Scan(&m.Seq, &m.Local, &m.Peer, &m.ID, &m.Wire, &m.Relay, &m.Direct, &m.Notice, &m.Control, &m.Stamped, &m.Attempts, &m.State, &next, &m.LastError); err != nil {
			return nil, err
		}
		m.Next = time.UnixMilli(next)
		result = append(result, m)
	}
	return result, rows.Err()
}
func (s *DeliveryStore) Claim(seq int64) (bool, error) {
	r, e := s.db.Exec(`UPDATE outbox SET state='sending' WHERE seq=? AND state='queued'`, seq)
	if e != nil {
		return false, e
	}
	n, e := r.RowsAffected()
	return n == 1, e
}
func (s *DeliveryStore) SavePrepared(m PendingMessage) error {
	_, e := s.db.Exec(`UPDATE outbox SET wire=?,relay=?,stamped=? WHERE seq=?`, m.Wire, m.Relay, m.Stamped, m.Seq)
	return e
}
func (s *DeliveryStore) Delivered(seq int64) error {
	_, e := s.db.Exec(`DELETE FROM outbox WHERE seq=?`, seq)
	return e
}
func (s *DeliveryStore) Submitted(seq int64) error {
	_, e := s.db.Exec(`UPDATE outbox SET state='submitted',error='',attempts=attempts+1 WHERE seq=?`, seq)
	return e
}
func (s *DeliveryStore) Retry(seq int64, at time.Time, cause error, blocked bool) error {
	state := "queued"
	if blocked {
		state = "blocked"
	}
	_, e := s.db.Exec(`UPDATE outbox SET state=?,next=?,attempts=attempts+1,error=? WHERE seq=?`, state, at.UnixMilli(), cause.Error(), seq)
	return e
}
func (s *DeliveryStore) Wake(peer []byte) error {
	_, e := s.db.Exec(`UPDATE outbox SET state='queued',next=0 WHERE peer=? AND state IN ('queued','submitted')`, peer)
	return e
}
func (s *DeliveryStore) Count(state string) (int, error) {
	var n int
	e := s.db.QueryRow(`SELECT COUNT(*) FROM outbox WHERE state=?`, state).Scan(&n)
	return n, e
}

func (s *DeliveryStore) AcceptIncoming(chat string, id, payload []byte) (bool, error) {
	if len(id) == 0 {
		return false, errors.New("incoming message has no ID")
	}
	r, e := s.db.Exec(`INSERT INTO inbox(chat,id,payload,updated) VALUES(?,?,?,?) ON CONFLICT(chat,id) DO NOTHING`, chat, id, payload, time.Now().UnixMilli())
	if e != nil {
		return false, e
	}
	n, e := r.RowsAffected()
	return n == 1, e
}

// ReadyIncoming atomically saves a complete prompt and retires its raw fragments.
func (s *DeliveryStore) ReadyIncoming(chat string, id, payload []byte, parts [][]byte) error {
	tx, e := s.db.Begin()
	if e != nil {
		return e
	}
	defer tx.Rollback()
	var state string
	if e = tx.QueryRow(`SELECT state FROM inbox WHERE chat=? AND id=?`, chat, id).Scan(&state); e != nil {
		return e
	}
	if state != "buffered" {
		return nil
	}
	for _, part := range parts {
		if _, e = tx.Exec(`UPDATE inbox SET state='done',payload=NULL WHERE chat=? AND id=? AND state='buffered'`, chat, part); e != nil {
			return e
		}
	}
	_, e = tx.Exec(`UPDATE inbox SET state='queued',payload=?,updated=? WHERE chat=? AND id=? AND state IN ('buffered','done')`, payload, time.Now().UnixMilli(), chat, id)
	if e != nil {
		return e
	}
	return tx.Commit()
}
func (s *DeliveryStore) Incoming(state string) ([]Incoming, error) {
	query := `SELECT seq,chat,id,payload,state FROM inbox WHERE state=? ORDER BY seq LIMIT 10000`
	if state == "queued" {
		query = `SELECT seq,chat,id,payload,state FROM inbox WHERE state=? AND seq IN (SELECT MIN(seq) FROM inbox WHERE state='queued' GROUP BY chat) ORDER BY seq LIMIT 10000`
	}
	rows, e := s.db.Query(query, state)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	var result []Incoming
	for rows.Next() {
		var r Incoming
		if e = rows.Scan(&r.Seq, &r.Key, &r.ID, &r.Payload, &r.State); e != nil {
			return nil, e
		}
		result = append(result, r)
	}
	return result, rows.Err()
}
func (s *DeliveryStore) TransitionIncoming(chat string, id []byte, from, to string) (bool, error) {
	r, e := s.db.Exec(`UPDATE inbox SET state=?,updated=?,payload=CASE WHEN ? IN ('done','uncertain') THEN NULL ELSE payload END,output=CASE WHEN ?='done' THEN '' ELSE output END WHERE chat=? AND id=? AND state=?`, to, time.Now().UnixMilli(), to, to, chat, id, from)
	if e != nil {
		return false, e
	}
	n, e := r.RowsAffected()
	return n == 1, e
}
func (s *DeliveryStore) IncomingState(chat string, id []byte) (string, error) {
	var state string
	e := s.db.QueryRow(`SELECT state FROM inbox WHERE chat=? AND id=?`, chat, id).Scan(&state)
	if e != nil {
		return "", fmt.Errorf("incoming state: %w", e)
	}
	return state, nil
}

// AppendOutput saves generated text before it reaches the in-memory segmenter.
func (s *DeliveryStore) AppendOutput(chat string, id []byte, text string) error {
	_, e := s.db.Exec(`UPDATE inbox SET output=output||? WHERE chat=? AND id=? AND state='running'`, text, chat, id)
	return e
}
func (s *DeliveryStore) RecoverOutput(chat string) (string, error) {
	var text string
	e := s.db.QueryRow(`SELECT output FROM inbox WHERE chat=? AND state='uncertain' AND output!='' ORDER BY seq DESC LIMIT 1`, chat).Scan(&text)
	if errors.Is(e, sql.ErrNoRows) {
		return "", nil
	}
	return text, e
}
func (s *DeliveryStore) RetryChat(local, peer []byte) error {
	_, e := s.db.Exec(`UPDATE outbox SET state='queued',next=0 WHERE local=? AND peer=? AND state IN ('queued','blocked','submitted')`, local, peer)
	return e
}
func (s *DeliveryStore) Summary(local, peer []byte, chat string) (string, error) {
	rows, e := s.db.Query(`SELECT state,COUNT(*) FROM outbox WHERE local=? AND peer=? AND notice=0 GROUP BY state`, local, peer)
	if e != nil {
		return "", e
	}
	counts := map[string]int{}
	for rows.Next() {
		var state string
		var n int
		if e = rows.Scan(&state, &n); e != nil {
			rows.Close()
			return "", e
		}
		counts[state] = n
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return "", e
	}
	var uncertain int
	e = s.db.QueryRow(`SELECT COUNT(*) FROM inbox WHERE chat=? AND state='uncertain'`, chat).Scan(&uncertain)
	return fmt.Sprintf("Waiting for delivery: %d\nSending: %d\nNeeds attention: %d\nForwarded to propagation node (recipient delivery unconfirmed): %d\nInterrupted requests: %d\n\n/bridge retry — retry pending messages\n/bridge recover — retrieve saved text from the latest interrupted request", counts["queued"], counts["sending"], counts["blocked"], counts["submitted"], uncertain), e
}

func (s *DeliveryStore) Requeue(seq int64) error {
	_, e := s.db.Exec(`UPDATE outbox SET state='queued',next=0 WHERE seq=?`, seq)
	return e
}
func (s *DeliveryStore) ExpireNotices(now time.Time) error {
	_, e := s.db.Exec(`DELETE FROM outbox WHERE notice=1 AND state='queued' AND created<?`, now.Add(-time.Minute).UnixMilli())
	return e
}

func (s *DeliveryStore) BufferedChat(chat string) ([]Incoming, error) {
	rows, e := s.db.Query(`SELECT seq,chat,id,payload,state FROM inbox WHERE state='buffered' AND chat=? ORDER BY seq`, chat)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	var result []Incoming
	for rows.Next() {
		var r Incoming
		if e = rows.Scan(&r.Seq, &r.Key, &r.ID, &r.Payload, &r.State); e != nil {
			return nil, e
		}
		result = append(result, r)
	}
	return result, rows.Err()
}

// Sending returns claimed rows so the dispatcher can recover a failed outcome write.
func (s *DeliveryStore) Sending() ([]PendingMessage, error) {
	rows, e := s.db.Query(`SELECT seq,local,peer FROM outbox WHERE state='sending'`)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	var result []PendingMessage
	for rows.Next() {
		var m PendingMessage
		if e = rows.Scan(&m.Seq, &m.Local, &m.Peer); e != nil {
			return nil, e
		}
		result = append(result, m)
	}
	return result, rows.Err()
}

func (s *DeliveryStore) BufferedChats() ([]string, error) {
	rows, e := s.db.Query(`SELECT DISTINCT chat FROM inbox WHERE state='buffered'`)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	var chats []string
	for rows.Next() {
		var chat string
		if e = rows.Scan(&chat); e != nil {
			return nil, e
		}
		chats = append(chats, chat)
	}
	return chats, rows.Err()
}
