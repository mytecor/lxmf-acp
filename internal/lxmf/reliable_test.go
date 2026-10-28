package lxmf

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mytecor/lxmf-acp/internal/storage"
	wirelxmf "github.com/thatSFguy/reticulum-go/lxmf"
	"github.com/thatSFguy/reticulum-go/rns"
	"github.com/vmihailenco/msgpack/v5"
)

func eventually(t *testing.T, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition not reached")
}
func testReliableClient(t *testing.T) (*Client, *storage.DeliveryStore) {
	t.Helper()
	s, err := storage.OpenDeliveryStore(filepath.Join(t.TempDir(), "queue.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	id, _ := rns.NewIdentity()
	c, err := NewClient("", "test", id)
	if err != nil {
		t.Fatal(err)
	}
	c.ConfigureDelivery(s, DeliveryOptions{RetryMin: time.Millisecond, RetryMax: 10 * time.Millisecond, SendTimeout: 200 * time.Millisecond})
	return c, s
}
func TestSignedMessageSurvivesRestartAndTransportChange(t *testing.T) {
	c, s := testReliableClient(t)
	peer, _ := rns.NewIdentity()
	dest := peer.DestinationHashFor(wirelxmf.FullName())
	id, err := c.QueueMessage(c.Hash(), dest, OutboundMessage{Content: "offline answer", Fields: map[any]any{FieldRenderer: RendererMarkdown, FieldReplyTo: []byte("request")}})
	if err != nil {
		t.Fatal(err)
	}
	due, _ := s.Due(time.Now(), 10)
	if len(due) != 1 {
		t.Fatal("message not saved")
	}
	m := due[0]
	before := append([]byte(nil), m.Wire...)
	parsed, err := wirelxmf.ParseDirectBody(m.Wire)
	if err != nil {
		t.Fatal(err)
	}
	if err := parsed.Verify(c.initialID.PublicKey()[32:]); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(id, parsed.MessageID()) {
		t.Fatal("saved ID differs")
	}
	known := &rns.KnownIdentity{DestHash: dest, PublicKey: peer.PublicKey()}
	cost := 4
	known.AppData, _ = rns.EncodeLXMFAppData([]byte("peer"), &cost)
	if err := stampPrepared(&m, known, &wirelxmf.Delivery{}); err != nil {
		t.Fatal(err)
	}
	stamped, err := wirelxmf.ParseDirectBody(m.Wire)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(stamped.MessageID(), id) || !bytes.Equal(m.Wire[:96], before[:96]) {
		t.Fatal("stamp changed signature or ID")
	}
	if err := stamped.Verify(c.initialID.PublicKey()[32:]); err != nil {
		t.Fatal(err)
	}
	if len(stamped.Stamp) == 0 {
		t.Fatal("required stamp missing")
	}
	// Offline propagation encrypts the EXACT same signed body, not a newly packed message.
	token, err := rns.TokenEncrypt(m.Wire[16:], known.EncryptionPublic(), recipientSalt(known))
	if err != nil {
		t.Fatal(err)
	}
	plain, err := rns.TokenDecrypt(peer, token)
	if err != nil {
		t.Fatal(err)
	}
	recovered, err := wirelxmf.ParseOpportunisticBody(plain, dest)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(recovered.MessageID(), id) {
		t.Fatal("method change created a new message")
	}
}
func TestDispatcherRetriesAndOtherChatsProceed(t *testing.T) {
	c, s := testReliableClient(t)
	p1, _ := rns.NewIdentity()
	p2, _ := rns.NewIdentity()
	bad := p1.DestinationHashFor(wirelxmf.FullName())
	good := p2.DestinationHashFor(wirelxmf.FullName())
	for _, peer := range [][]byte{bad, good} {
		if _, err := c.QueueMessage(c.Hash(), peer, OutboundMessage{Content: "answer"}); err != nil {
			t.Fatal(err)
		}
	}
	var attempts atomic.Int32
	var goodDelivered atomic.Bool
	c.outbox.send = func(ctx context.Context, m *storage.PendingMessage) (bool, error) {
		if bytes.Equal(m.Peer, good) {
			goodDelivered.Store(true)
			return false, nil
		}
		if attempts.Add(1) < 3 {
			return false, errors.New("proof lost")
		}
		return false, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	go c.outbox.run(ctx)
	defer func() { cancel(); <-c.outbox.done }()
	eventually(t, func() bool {
		n, _ := s.Count("queued")
		sending, _ := s.Count("sending")
		return attempts.Load() >= 3 && goodDelivered.Load() && n+sending == 0
	})
}
func TestOptionalNoticeDoesNotHoldAnswer(t *testing.T) {
	c, s := testReliableClient(t)
	peer, _ := rns.NewIdentity()
	dest := peer.DestinationHashFor(wirelxmf.FullName())
	_, _ = c.QueueMessage(c.Hash(), dest, OutboundMessage{Content: "pending", Notice: true})
	_, _ = c.QueueMessage(c.Hash(), dest, OutboundMessage{Content: "answer"})
	var answers atomic.Int32
	c.outbox.send = func(ctx context.Context, m *storage.PendingMessage) (bool, error) {
		if m.Notice {
			return false, errors.New("notice proof missing")
		}
		answers.Add(1)
		return false, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	go c.outbox.run(ctx)
	defer func() { cancel(); <-c.outbox.done }()
	eventually(t, func() bool {
		n, _ := s.Count("queued")
		sending, _ := s.Count("sending")
		return answers.Load() == 1 && n+sending == 0
	})
}

type testWire struct {
	in, out   chan []byte
	done      chan struct{}
	dropProof atomic.Bool
}

func (w *testWire) Send(b []byte) error {
	p, _ := rns.ParsePacket(b)
	if p != nil && p.PacketType == rns.PacketProof && w.dropProof.Load() {
		return nil
	}
	select {
	case w.in <- append([]byte(nil), b...):
	case <-w.done:
	}
	return nil
}
func (w *testWire) Inbox() <-chan []byte  { return w.out }
func (w *testWire) Done() <-chan struct{} { return w.done }
func pairedWire(t *testing.T) (*testWire, *testWire) {
	a, b := make(chan []byte, 128), make(chan []byte, 128)
	done := make(chan struct{})
	t.Cleanup(func() { close(done) })
	return &testWire{in: a, out: b, done: done}, &testWire{in: b, out: a, done: done}
}
func wireClient(t *testing.T) (*Client, *storage.DeliveryStore, *rns.Transport, *testWire) {
	c, s := testReliableClient(t)
	a, b := pairedWire(t)
	c.transport = rns.NewTransport(nil)
	c.transport.AddInterface(a)
	if err := c.registerDeliveryLocked(c.initialID); err != nil {
		t.Fatal(err)
	}
	remote := rns.NewTransport(nil)
	remote.AddInterface(b)
	ctx, cancel := context.WithCancel(context.Background())
	go c.transport.Run(ctx)
	go remote.Run(ctx)
	t.Cleanup(cancel)
	return c, s, remote, b
}
func cachePeer(t *testing.T, c *Client, id *rns.Identity, name string, appData []byte) {
	t.Helper()
	pkt, err := rns.BuildAnnounce(id, name, appData, nil)
	if err != nil {
		t.Fatal(err)
	}
	_ = pkt
	// Restore enforces the same destination/public-key binding; no network timing needed.
	c.transport.Restore(&rns.KnownIdentity{DestHash: id.DestinationHashFor(name), PublicKey: id.PublicKey(), AppData: appData})
}
func TestPreparedRealDeliveryOpportunisticAndDirect(t *testing.T) {
	for _, direct := range []bool{false, true} {
		t.Run(map[bool]string{false: "packet", true: "link"}[direct], func(t *testing.T) {
			c, s, remote, _ := wireClient(t)
			peer, _ := rns.NewIdentity()
			dest := peer.DestinationHashFor(wirelxmf.FullName())
			received := make(chan *wirelxmf.Message, 4)
			d, err := wirelxmf.NewDelivery(remote, peer, nil)
			if err != nil {
				t.Fatal(err)
			}
			d.OnMessage = func(m *wirelxmf.Message) { received <- m }
			cachePeer(t, c, peer, wirelxmf.FullName(), nil)
			remote.Restore(&rns.KnownIdentity{DestHash: c.Hash(), PublicKey: c.initialID.PublicKey()})
			id, err := c.QueueMessage(c.Hash(), dest, OutboundMessage{Content: "confirmed", Direct: direct})
			if err != nil {
				t.Fatal(err)
			}
			due, _ := s.Due(time.Now(), 10)
			submitted, err := c.sendPrepared(context.Background(), &due[0])
			if err != nil || submitted {
				t.Fatal("delivery failed", submitted, err)
			}
			select {
			case msg := <-received:
				if !bytes.Equal(msg.MessageID(), id) || string(msg.Content) != "confirmed" {
					t.Fatal("wrong received message")
				}
			case <-time.After(time.Second):
				t.Fatal("no message")
			}
		})
	}
}
func TestLostProofRetainsSameIDForRetry(t *testing.T) {
	c, s, remote, wire := wireClient(t)
	peer, _ := rns.NewIdentity()
	dest := peer.DestinationHashFor(wirelxmf.FullName())
	var received atomic.Int32
	d, _ := wirelxmf.NewDelivery(remote, peer, nil)
	d.OnMessage = func(*wirelxmf.Message) { received.Add(1) }
	cachePeer(t, c, peer, wirelxmf.FullName(), nil)
	remote.Restore(&rns.KnownIdentity{DestHash: c.Hash(), PublicKey: c.initialID.PublicKey()})
	id, err := c.QueueMessage(c.Hash(), dest, OutboundMessage{Content: "proof will be dropped"})
	if err != nil {
		t.Fatal(err)
	}
	due, _ := s.Due(time.Now(), 10)
	m := due[0]
	wire.dropProof.Store(true)
	if _, err := c.sendPrepared(context.Background(), &m); err == nil {
		t.Fatal("missing proof reported as delivery")
	}
	eventually(t, func() bool { return received.Load() > 0 })
	wire.dropProof.Store(false)
	m.Attempts = 1 // next attempt uses a Link, same signed envelope
	if _, err := c.sendPrepared(context.Background(), &m); err != nil {
		t.Fatal(err)
	}
	parsed, _ := wirelxmf.ParseDirectBody(m.Wire)
	if !bytes.Equal(id, parsed.MessageID()) {
		t.Fatal("retry changed ID")
	}
}
func TestPropagationFallbackPreservesIDAndDoesNotClaimRecipientDelivery(t *testing.T) {
	c, s, remote, _ := wireClient(t)
	peer, _ := rns.NewIdentity()
	node, _ := rns.NewIdentity()
	dest := peer.DestinationHashFor(wirelxmf.FullName())
	nodeDest := node.DestinationHashFor(wirelxmf.PropagationFullName())
	c.outbox.opts.PropagationNode = nodeDest
	c.outbox.opts.PropagationAfter = 1
	appData, _ := msgpack.Marshal([]any{nil, time.Now().Unix(), true, 0, 0, []int{0, 0, 0}, nil})
	cachePeer(t, c, peer, wirelxmf.FullName(), nil)
	cachePeer(t, c, node, wirelxmf.PropagationFullName(), appData)
	captured := make(chan []byte, 1)
	if err := remote.RegisterLocal(&rns.LocalDestination{DestHash: nodeDest, Identity: node, OnPacket: func(*rns.Packet) {}, OnLinkPlaintext: func(body []byte) { captured <- body }}); err != nil {
		t.Fatal(err)
	}
	id, err := c.QueueMessage(c.Hash(), dest, OutboundMessage{Content: "for offline peer", Direct: true})
	if err != nil {
		t.Fatal(err)
	}
	due, _ := s.Due(time.Now(), 10)
	m := due[0]
	submitted, err := c.sendPrepared(context.Background(), &m)
	if err != nil || !submitted {
		t.Fatal("fallback failed", submitted, err)
	}
	var bundle []byte
	select {
	case bundle = <-captured:
	case <-time.After(time.Second):
		t.Fatal("no upload")
	}
	var outer []msgpack.RawMessage
	if err := msgpack.Unmarshal(bundle, &outer); err != nil {
		t.Fatal(err)
	}
	var bodies [][]byte
	if err := msgpack.Unmarshal(outer[1], &bodies); err != nil {
		t.Fatal(err)
	}
	plain, err := rns.TokenDecrypt(peer, bodies[0][16:])
	if err != nil {
		t.Fatal(err)
	}
	msg, err := wirelxmf.ParseOpportunisticBody(plain, dest)
	if err != nil {
		t.Fatal(err)
	}
	if err := msg.Verify(c.initialID.PublicKey()[32:]); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(id, msg.MessageID()) {
		t.Fatal("relay repacked the message")
	}
	if err := s.Submitted(m.Seq); err != nil {
		t.Fatal(err)
	}
	n, _ := s.Count("submitted")
	if n != 1 {
		t.Fatal("relay submission erased unconfirmed message")
	}
}
