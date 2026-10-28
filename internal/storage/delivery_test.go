package storage

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestOutboxRestartOrderAndProofLoss(t *testing.T) {
	path := filepath.Join(t.TempDir(), "queue.db")
	s, err := OpenDeliveryStore(path)
	if err != nil {
		t.Fatal(err)
	}
	first := PendingMessage{Local: []byte("local"), Peer: []byte("peer"), ID: []byte("first"), Wire: []byte("fixed signed bytes")}
	for _, m := range []PendingMessage{first, {Local: first.Local, Peer: first.Peer, ID: []byte("second"), Wire: []byte("part two")}, {Local: first.Local, Peer: []byte("other"), ID: []byte("other"), Wire: []byte("independent")}} {
		if err := s.Enqueue(m); err != nil {
			t.Fatal(err)
		}
	}
	due, err := s.Due(time.Now(), 20)
	if err != nil || len(due) != 2 {
		t.Fatalf("only one head per chat should be due: %v %d", err, len(due))
	}
	if ok, err := s.Claim(due[0].Seq); !ok || err != nil {
		t.Fatal("claim", ok, err)
	}
	// Simulate a crash after the recipient got a packet but before its proof was committed.
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = OpenDeliveryStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.Enqueue(first); err != nil {
		t.Fatal(err)
	}
	due, err = s.Due(time.Now(), 20)
	if err != nil || len(due) != 2 || !bytes.Equal(due[0].ID, first.ID) || !bytes.Equal(due[0].Wire, first.Wire) {
		t.Fatal("restart changed or duplicated the signed message", err)
	}
	if err := s.Retry(due[0].Seq, time.Now().Add(time.Hour), errors.New("no proof"), false); err != nil {
		t.Fatal(err)
	}
	later, _ := s.Due(time.Now(), 20)
	if len(later) != 1 || string(later[0].Peer) != "other" {
		t.Fatal("retry blocked another chat or released its own successor")
	}
	if err := s.Wake(first.Peer); err != nil {
		t.Fatal(err)
	}
	if err := s.Delivered(due[0].Seq); err != nil {
		t.Fatal(err)
	}
	next, _ := s.Due(time.Now(), 20)
	if len(next) != 2 || string(next[0].ID) != "second" {
		t.Fatal("confirmation did not release next part")
	}
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatal("queue permissions", err)
	}
}
func TestNoticeNeverBlocksAnswerAndBudgetRetainsExisting(t *testing.T) {
	s, err := OpenDeliveryStore(filepath.Join(t.TempDir(), "queue.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	notice := PendingMessage{Local: []byte("l"), Peer: []byte("p"), ID: []byte("notice"), Wire: []byte("optional"), Notice: true}
	if err := s.Enqueue(notice); err != nil {
		t.Fatal(err)
	}
	notice.ID = []byte("new-notice")
	if err := s.Enqueue(notice); err != nil {
		t.Fatal(err)
	}
	answer := notice
	answer.Notice = false
	answer.ID = []byte("answer")
	answer.Wire = []byte("mandatory")
	if err := s.Enqueue(answer); err != nil {
		t.Fatal(err)
	}
	due, _ := s.Due(time.Now(), 20)
	if len(due) != 2 || string(due[0].ID) != "answer" {
		t.Fatal("answer not prioritized", len(due))
	}
	s.MaxPendingBytes = 1
	answer.ID = []byte("over-budget")
	if err := s.Enqueue(answer); err == nil {
		t.Fatal("budget ignored")
	}
	due, _ = s.Due(time.Now(), 20)
	if len(due) != 2 {
		t.Fatal("budget failure changed pending messages")
	}
}
func TestInboxCrashNeverRepeatsRunningRequest(t *testing.T) {
	path := filepath.Join(t.TempDir(), "queue.db")
	s, err := OpenDeliveryStore(path)
	if err != nil {
		t.Fatal(err)
	}
	chat, id := "chat", []byte("request")
	if ok, err := s.AcceptIncoming(chat, id, []byte("request bytes")); !ok || err != nil {
		t.Fatal(ok, err)
	}
	if err := s.ReadyIncoming(chat, id, []byte("complete prompt"), nil); err != nil {
		t.Fatal(err)
	}
	for _, state := range [][2]string{{"queued", "scheduled"}, {"scheduled", "running"}} {
		if ok, err := s.TransitionIncoming(chat, id, state[0], state[1]); !ok || err != nil {
			t.Fatal(ok, err)
		}
	}
	if err := s.AppendOutput(chat, id, "generated before crash"); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = OpenDeliveryStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	state, err := s.IncomingState(chat, id)
	if err != nil || state != "uncertain" {
		t.Fatal("running request replayed", state, err)
	}
	if ok, err := s.AcceptIncoming(chat, id, []byte("duplicate")); ok || err != nil {
		t.Fatal("duplicate accepted", err)
	}
	output, err := s.RecoverOutput(chat)
	if err != nil || output != "generated before crash" {
		t.Fatal("lost generated output", output, err)
	}
	rows, _ := s.Incoming("queued")
	if len(rows) != 0 {
		t.Fatal("unsafe replay queued")
	}
}
func TestInboxAssemblyAndScheduledRecovery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "queue.db")
	s, err := OpenDeliveryStore(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range [][]byte{[]byte("a"), []byte("b")} {
		if _, err := s.AcceptIncoming("chat", id, []byte("fragment")); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.ReadyIncoming("chat", []byte("b"), []byte("assembled"), [][]byte{[]byte("a"), []byte("b")}); err != nil {
		t.Fatal(err)
	}
	if ok, err := s.TransitionIncoming("chat", []byte("b"), "queued", "scheduled"); !ok || err != nil {
		t.Fatal(ok, err)
	}
	s.Close()
	s, err = OpenDeliveryStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	buffered, _ := s.Incoming("buffered")
	queued, _ := s.Incoming("queued")
	if len(buffered) != 0 || len(queued) != 1 || string(queued[0].Payload) != "assembled" {
		t.Fatal("assembly or scheduled recovery failed")
	}
	if _, err := s.TransitionIncoming("chat", []byte("b"), "queued", "running"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.TransitionIncoming("chat", []byte("b"), "running", "done"); err != nil {
		t.Fatal(err)
	}
	state, _ := s.IncomingState("chat", []byte("a"))
	if state != "done" {
		t.Fatal("fragment dedup lost")
	}
}

func TestReadyCannotResurrectCompletedRequest(t *testing.T) {
	s, err := OpenDeliveryStore(filepath.Join(t.TempDir(), "private ? #.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	id := []byte("id")
	if _, err := s.AcceptIncoming("chat", id, []byte("raw")); err != nil {
		t.Fatal(err)
	}
	if err := s.ReadyIncoming("chat", id, []byte("ready"), nil); err != nil {
		t.Fatal(err)
	}
	for _, states := range [][2]string{{"queued", "scheduled"}, {"scheduled", "running"}, {"running", "done"}} {
		if _, err := s.TransitionIncoming("chat", id, states[0], states[1]); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.ReadyIncoming("chat", id, []byte("stale concurrent assembly"), [][]byte{id}); err != nil {
		t.Fatal(err)
	}
	state, err := s.IncomingState("chat", id)
	if err != nil || state != "done" {
		t.Fatal("stale assembler resurrected an executed request", state, err)
	}
}
func TestControlReplyCanBypassBlockedAnswer(t *testing.T) {
	s, err := OpenDeliveryStore(filepath.Join(t.TempDir(), "queue.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	m := PendingMessage{Local: []byte("l"), Peer: []byte("p"), ID: []byte("answer"), Wire: []byte("data")}
	if err := s.Enqueue(m); err != nil {
		t.Fatal(err)
	}
	due, _ := s.Due(time.Now(), 10)
	if err := s.Retry(due[0].Seq, time.Now(), errors.New("attention required"), true); err != nil {
		t.Fatal(err)
	}
	m.ID = []byte("recover")
	m.Control = true
	if err := s.Enqueue(m); err != nil {
		t.Fatal(err)
	}
	due, _ = s.Due(time.Now(), 10)
	if len(due) != 1 || string(due[0].ID) != "recover" {
		t.Fatal("blocked chain prevented recovery command")
	}
	s.MaxPendingBytes = 1
	if err := s.Enqueue(m); err != nil {
		t.Fatal("idempotent enqueue rejected at budget", err)
	}
}
