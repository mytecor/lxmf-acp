package lxmf

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/mytecor/lxmf-acp/internal/storage"
	wirelxmf "github.com/thatSFguy/reticulum-go/lxmf"
	"github.com/thatSFguy/reticulum-go/rns"
	"github.com/vmihailenco/msgpack/v5"
)

type DeliveryOptions struct {
	Workers                                        int
	RetryMin, RetryMax, SendTimeout, NoticeTimeout time.Duration
	PropagationNode                                []byte
	PropagationAfter                               int
}

func (o DeliveryOptions) defaults() DeliveryOptions {
	if o.Workers <= 0 {
		o.Workers = 4
	}
	if o.RetryMin <= 0 {
		o.RetryMin = 15 * time.Second
	}
	if o.RetryMax < o.RetryMin {
		o.RetryMax = max(o.RetryMin, 15*time.Minute)
	}
	if o.SendTimeout <= 0 {
		o.SendTimeout = 45 * time.Second
	}
	if o.NoticeTimeout <= 0 {
		o.NoticeTimeout = 5 * time.Second
	}
	if o.PropagationAfter <= 0 {
		o.PropagationAfter = 3
	}
	return o
}

type deliveryQueue struct {
	store *storage.DeliveryStore
	opts  DeliveryOptions
	wake  chan struct{}
	done  chan struct{}
	send  func(context.Context, *storage.PendingMessage) (bool, error)
	mu    sync.Mutex
	busy  map[string]bool
	wg    sync.WaitGroup
}

func (c *Client) ConfigureDelivery(store *storage.DeliveryStore, opts DeliveryOptions) {
	c.outbox = &deliveryQueue{store: store, opts: opts.defaults(), wake: make(chan struct{}, 1), done: make(chan struct{}), busy: make(map[string]bool)}
	c.outbox.send = c.sendPrepared
}
func (c *Client) DurableDelivery() bool { return c.outbox != nil }
func (q *deliveryQueue) signal() {
	select {
	case q.wake <- struct{}{}:
	default:
	}
}
func (c *Client) WakeDelivery(peer []byte) {
	if c.outbox != nil {
		if err := c.outbox.store.Wake(peer); err != nil {
			log.Printf("[Outbox] Wake failed: %v", err)
		}
		c.outbox.signal()
	}
}

// QueueMessage returns after the signed envelope is committed, before delivery.
// Its ID is fixed even if the peer is offline or has not announced yet.
func (c *Client) QueueMessage(local, peer []byte, m OutboundMessage) ([]byte, error) {
	if c.outbox == nil {
		return c.SendMessage(local, peer, m)
	}
	c.mu.RLock()
	id := c.identities[hex.EncodeToString(local)]
	c.mu.RUnlock()
	if id == nil {
		return nil, errors.New("local identity not registered")
	}
	if _, ok := ParseRenderer(m.Fields); !ok {
		m.Fields = WithRenderer(m.Fields, RendererMarkdown)
	}
	body, msgID, err := wirelxmf.SignAndPackDirect(id, local, peer, []byte(m.Title), []byte(m.Content), m.Fields)
	if err != nil {
		return nil, err
	}
	err = c.outbox.store.Enqueue(storage.PendingMessage{Local: local, Peer: peer, ID: msgID, Wire: body, Direct: m.Direct, Notice: m.Notice, Control: m.Control})
	if err != nil {
		return nil, fmt.Errorf("persist outgoing message: %w", err)
	}
	c.touchIdentity(local)
	c.outbox.signal()
	return msgID, nil
}

func (q *deliveryQueue) run(ctx context.Context) {
	defer close(q.done)
	defer q.wg.Wait()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		if ctx.Err() != nil {
			return
		}
		q.dispatch(ctx)
		select {
		case <-ctx.Done():
			return
		case <-q.wake:
		case <-ticker.C:
		}
	}
}
func (q *deliveryQueue) dispatch(ctx context.Context) {
	// An acknowledgement may have arrived while SQLite was temporarily unable
	// to commit. Reclaim only rows that have no in-flight sender in this process.
	sending, err := q.store.Sending()
	if err != nil {
		log.Printf("[Outbox] Recovery scan failed: %v", err)
		return
	}
	for _, m := range sending {
		key := hex.EncodeToString(m.Local) + ":" + hex.EncodeToString(m.Peer)
		q.mu.Lock()
		active := q.busy[key]
		q.mu.Unlock()
		if !active {
			if err := q.store.Requeue(m.Seq); err != nil {
				log.Printf("[Outbox] Cannot reclaim %x: %v", m.ID, err)
			}
		}
	}

	if err := q.store.ExpireNotices(time.Now()); err != nil {
		log.Printf("[Outbox] Notice cleanup failed: %v", err)
	}
	pending, err := q.store.Due(time.Now(), 256)
	if err != nil {
		log.Printf("[Outbox] Read failed: %v", err)
		return
	}
	for _, m := range pending {
		if ctx.Err() != nil {
			return
		}
		key := hex.EncodeToString(m.Local) + ":" + hex.EncodeToString(m.Peer)
		q.mu.Lock()
		if len(q.busy) >= q.opts.Workers {
			q.mu.Unlock()
			return
		}
		if q.busy[key] {
			q.mu.Unlock()
			continue
		}
		q.busy[key] = true
		q.mu.Unlock()
		claimed, err := q.store.Claim(m.Seq)
		if err != nil || !claimed {
			q.mu.Lock()
			delete(q.busy, key)
			q.mu.Unlock()
			if err != nil {
				log.Printf("[Outbox] Claim failed: %v", err)
			}
			continue
		}
		q.wg.Add(1)
		go func(m storage.PendingMessage) {
			defer q.wg.Done()
			defer func() { q.mu.Lock(); delete(q.busy, key); q.mu.Unlock(); q.signal() }()
			submitted, sendErr := q.send(ctx, &m)
			var saveErr error
			switch {
			case sendErr == nil && submitted:
				saveErr = q.store.Submitted(m.Seq)
				log.Printf("[Outbox] Message %x forwarded to propagation node (recipient delivery unconfirmed)", m.ID)
			case sendErr == nil:
				saveErr = q.store.Delivered(m.Seq)
				log.Printf("[Outbox] Message %x delivered", m.ID)
			case ctx.Err() != nil && !m.Notice:
				saveErr = q.store.Requeue(m.Seq)
			case m.Notice:
				saveErr = q.store.Delivered(m.Seq)
				log.Printf("[Outbox] Dropped optional tool notice %x: %v", m.ID, sendErr)
			default:
				var permanent *permanentDeliveryError
				blocked := errors.As(sendErr, &permanent)
				delay := retryDelay(q.opts, m.Attempts)
				saveErr = q.store.Retry(m.Seq, time.Now().Add(delay), sendErr, blocked)
				log.Printf("[Outbox] Message %x retained; attempt=%d blocked=%v retry=%s: %v", m.ID, m.Attempts+1, blocked, delay, sendErr)
			}
			if saveErr != nil {
				log.Printf("[Outbox] Cannot record outcome for %x (message retained): %v", m.ID, saveErr)
			}
		}(m)
	}
}
func retryDelay(o DeliveryOptions, attempt int) time.Duration {
	d := o.RetryMin
	for i := 0; i < attempt && d < o.RetryMax; i++ {
		if d > o.RetryMax/2 {
			d = o.RetryMax
			break
		}
		d *= 2
	}
	d = min(d, o.RetryMax)
	return d*3/4 + time.Duration(rand.Int64N(max(1, int64(d/4))))
}

type permanentDeliveryError struct{ error }

func (c *Client) sendPrepared(ctx context.Context, m *storage.PendingMessage) (bool, error) {
	c.mu.RLock()
	t := c.transport
	delivery := c.deliveries[hex.EncodeToString(m.Local)]
	c.mu.RUnlock()
	if t == nil || delivery == nil {
		return false, errors.New("local destination unavailable")
	}
	known := t.Recall(m.Peer)
	if known == nil || len(known.PublicKey) != rns.PublicKeyLen {
		_ = t.RequestPath(m.Peer)
		return false, wirelxmf.ErrRecipientUnknown
	}
	if !m.Stamped {
		if err := stampPrepared(m, known, delivery); err != nil {
			return false, err
		}
		if err := c.outbox.store.SavePrepared(*m); err != nil {
			return false, err
		}
	}
	timeout := c.outbox.opts.SendTimeout
	if m.Notice {
		timeout = c.outbox.opts.NoticeTimeout
	}
	attemptCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	// Small standalone messages avoid a handshake. Retrying re-encrypts only the
	// transport envelope; signed LXMF bytes and message ID remain unchanged.
	if !m.Direct && m.Attempts == 0 && len(m.Wire)-96 <= wirelxmf.MaxOpportunisticPayload {
		err := sendPreparedPacket(attemptCtx, t, known, m)
		if err == nil {
			return false, nil
		}
		if attemptCtx.Err() != nil {
			return false, err
		}
	}
	err := boundedLinkSend(attemptCtx, t, m.Peer, m.Wire, timeout)
	if err == nil {
		return false, nil
	}
	if ctx.Err() != nil || m.Notice || len(c.outbox.opts.PropagationNode) == 0 || m.Attempts+1 < c.outbox.opts.PropagationAfter {
		return false, err
	}
	// Relay transfer has a separate budget; it never means DELIVERED to the peer.
	relayCtx, relayCancel := context.WithTimeout(ctx, timeout)
	defer relayCancel()
	if relayErr := c.sendPreparedPropagated(relayCtx, t, known, m, timeout); relayErr != nil {
		return false, fmt.Errorf("direct: %v; propagation: %w", err, relayErr)
	}
	return true, nil
}

func stampPrepared(m *storage.PendingMessage, known *rns.KnownIdentity, d *wirelxmf.Delivery) error {
	if len(m.Wire) < 97 || m.Wire[96] != 0x94 {
		return &permanentDeliveryError{errors.New("invalid saved LXMF envelope")}
	}
	var stamp []byte
	var err error
	if !d.DisableOutboundStamps {
		if d.Tickets != nil {
			if ticket := d.Tickets.Held(m.Peer, time.Now()); ticket != nil {
				stamp, err = wirelxmf.TicketStamp(ticket, m.ID)
			}
		}
		if err != nil {
			return err
		}
		if len(stamp) == 0 {
			cost, decodeErr := rns.DecodeLXMFAppDataStampCost(known.AppData)
			if decodeErr != nil {
				log.Printf("[Outbox] Invalid peer stamp metadata: %v", decodeErr)
			}
			cap := d.MaxStampCost
			if cap <= 0 {
				cap = wirelxmf.MaxDeliveryStampCost
			}
			if cost > cap {
				return &permanentDeliveryError{fmt.Errorf("recipient stamp cost %d exceeds cap %d", cost, cap)}
			}
			if cost > 0 {
				stamp, err = wirelxmf.GenerateDeliveryStamp(m.ID, cost)
				if err != nil {
					return err
				}
			}
		}
	}
	if len(stamp) > 0 {
		encoded, e := msgpack.Marshal(stamp)
		if e != nil {
			return e
		}
		m.Wire = append(append([]byte(nil), m.Wire...), encoded...)
		m.Wire[96] = 0x95
	}
	m.Stamped = true
	return nil
}
func recipientSalt(k *rns.KnownIdentity) []byte {
	h := sha256.Sum256(k.PublicKey)
	return h[:rns.IdentityHashLen]
}
func sendPreparedPacket(ctx context.Context, t *rns.Transport, k *rns.KnownIdentity, m *storage.PendingMessage) error {
	ciphertext, err := rns.TokenEncrypt(m.Wire[16:], k.EncryptionPublic(), recipientSalt(k))
	if err != nil {
		return err
	}
	p := &rns.Packet{HeaderType: rns.HeaderType1, TransportType: rns.BroadcastTransport, DestinationType: rns.DestinationSingle, PacketType: rns.PacketData, DestHash: m.Peer, Context: rns.ContextNone, Data: ciphertext}
	if len(k.TransportID) == rns.IdentityHashLen {
		p.HeaderType = rns.HeaderType2
		p.TransportType = rns.NetworkTransport
		p.TransportID = k.TransportID
	}
	hashable, err := p.HashablePart()
	if err != nil {
		return err
	}
	digest := sha256.Sum256(hashable)
	proof, cancel, err := t.RegisterPacketProofWaiter(digest[:], k.Ed25519Public())
	if err != nil {
		return err
	}
	defer cancel()
	if err = t.Broadcast(p); err != nil {
		return err
	}
	select {
	case err := <-proof:
		return err
	case <-ctx.Done():
		return fmt.Errorf("opportunistic delivery unconfirmed: %w", ctx.Err())
	}
}
func boundedLinkSend(ctx context.Context, t *rns.Transport, peer, body []byte, timeout time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	result := make(chan error, 1)
	go func() { result <- t.SendOverLink(peer, body, timeout) }()
	select {
	case err := <-result:
		return err
	case <-ctx.Done():
		if link := t.LinkManager().ActiveTo(peer); link != nil {
			t.LinkManager().CloseLink(link.ID)
		}
		return ctx.Err()
	}
}
func (c *Client) sendPreparedPropagated(ctx context.Context, t *rns.Transport, recipient *rns.KnownIdentity, m *storage.PendingMessage, timeout time.Duration) error {
	nodeHash := c.outbox.opts.PropagationNode
	node := t.Recall(nodeHash)
	if node == nil {
		_ = t.RequestPath(nodeHash)
		return wirelxmf.ErrPropagationNodeUnknown
	}
	info, err := wirelxmf.ParsePropagationNodeAppData(node.AppData)
	if err != nil {
		return err
	}
	if !info.Enabled {
		return wirelxmf.ErrPropagationNodeDisabled
	}
	if len(m.Relay) == 0 {
		token, err := rns.TokenEncrypt(m.Wire[16:], recipient.EncryptionPublic(), recipientSalt(recipient))
		if err != nil {
			return err
		}
		data := append(append([]byte(nil), m.Peer...), token...)
		if info.StampCost > 0 {
			hash := sha256.Sum256(data)
			stamp, e := wirelxmf.GeneratePropagationStamp(hash[:], info.StampCost)
			if e != nil {
				return e
			}
			data = append(data, stamp...)
		}
		m.Relay = data
		if err = c.outbox.store.SavePrepared(*m); err != nil {
			return err
		}
	}
	bundle, err := wirelxmf.PackPropagationBundle(time.Now(), m.Relay)
	if err != nil {
		return err
	}
	if info.PerTransferLimitKB > 0 && int64(len(bundle)) > info.PerTransferLimitKB*1000 {
		return wirelxmf.ErrPropagationTransferTooLarge
	}
	return boundedLinkSend(ctx, t, nodeHash, bundle, timeout)
}
