package lxmf

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/thatSFguy/reticulum-go/lxmf"
	"github.com/thatSFguy/reticulum-go/rns"
)

type InboundMessage struct {
	LocalHash  []byte
	SenderHash []byte
	MessageID  []byte
	Title      string
	Content    string
	// Fields carries the decoded LXMF fields dictionary (may be nil).
	Fields map[any]any
	// Multipart is set by the bridge when Content is the reassembled text
	// of a multipart LXMF chain rather than a single wire message.
	Multipart bool
	// Parts lists all component IDs of an assembled prompt for durable deduplication.
	Parts [][]byte
	// Renderer indicates the content renderer hint (e.g. RendererMarkdown).
	// Set to -1 if no renderer field was present.
	Renderer int
	// ReplyTo is the MessageID (hash) of the message this message is replying to,
	// if FIELD_REPLY_TO (0x30) was present.
	ReplyTo []byte
	// ReplyQuote is the quoted text snippet if FIELD_REPLY_QUOTE (0x31) was present.
	ReplyQuote string
}

// IsMarkdown reports whether the message explicitly specifies Markdown rendering.
func (m *InboundMessage) IsMarkdown() bool {
	return m.Renderer == RendererMarkdown
}

// IsReply reports whether the message references another message via ReplyTo or ReplyQuote.
func (m *InboundMessage) IsReply() bool {
	return len(m.ReplyTo) > 0 || m.ReplyQuote != ""
}

// OutboundMessage describes a single outbound LXMF message.
type OutboundMessage struct {
	Title   string
	Content string
	Fields  map[any]any
	// Direct forces LXMF DIRECT delivery (over a Reticulum Link) instead of
	// letting the library pick opportunistic delivery for small payloads.
	// It never falls back to PROPAGATED.
	Direct bool
	// Notice marks expendable tool progress, which cannot block durable answers.
	Notice bool
	// Control is a durable command response that can bypass a blocked answer chain.
	Control bool
}

// Direct-delivery retry policy. A packed message is retransmitted with the
// exact same bytes, so its LXMF message hash stays stable across retries
// and a duplicate arriving at the recipient is recognisable by hash.
const (
	directSendAttempts = 3
	directRetryBackoff = 3 * time.Second
)

type MessageHandler func(msg *InboundMessage)

type Client struct {
	rnsAddr     string
	displayName string
	initialID   *rns.Identity

	mu              sync.RWMutex
	transport       *rns.Transport
	tcpIface        *rns.ReconnectingTCPClient
	deliveries      map[string]*lxmf.Delivery
	identities      map[string]*rns.Identity
	lastActivity    map[string]time.Time
	linkIdleTimeout time.Duration
	chatIdleTimeout time.Duration
	cancel          context.CancelFunc
	outbox          *deliveryQueue
	outboxStarted   bool

	OnMessage  MessageHandler
	OnError    func(error)
	OnAnnounce func(*rns.Announce)
}

func NewClient(rnsAddr string, displayName string, identity *rns.Identity) (*Client, error) {
	if identity == nil {
		return nil, errors.New("identity cannot be nil")
	}

	c := &Client{
		rnsAddr:         rnsAddr,
		displayName:     displayName,
		initialID:       identity,
		deliveries:      make(map[string]*lxmf.Delivery),
		identities:      make(map[string]*rns.Identity),
		lastActivity:    make(map[string]time.Time),
		linkIdleTimeout: 2 * time.Minute,
		chatIdleTimeout: 24 * time.Hour,
	}

	initialHash := identity.DestinationHashFor(lxmf.FullName())
	c.identities[hex.EncodeToString(initialHash)] = identity
	c.lastActivity[hex.EncodeToString(initialHash)] = time.Now()

	return c, nil
}

// ConfigureLifecycle must be called before Start. A zero chat timeout keeps
// periodic announces enabled for every identity.
func (c *Client) ConfigureLifecycle(linkIdle, chatIdle time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if linkIdle > 0 {
		c.linkIdleTimeout = linkIdle
	}
	c.chatIdleTimeout = chatIdle
}

func (c *Client) SetIdentityLastActivity(destHash []byte, at time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lastActivity[hex.EncodeToString(destHash)] = at
}

func (c *Client) touchIdentity(destHash []byte) {
	c.SetIdentityLastActivity(destHash, time.Now())
}

func (c *Client) SetMessageHandler(h MessageHandler) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.OnMessage = h
}

func (c *Client) SetAnnounceHandler(h func(*rns.Announce)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.OnAnnounce = h
}

func (c *Client) Start(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	c.cancel = cancel
	log.Printf("[LXMF] Connecting to Reticulum instance at %s...", c.rnsAddr)

	tcpIface, err := rns.DialReconnectingTCP(c.rnsAddr, 5*time.Second, nil)
	if err != nil {
		cancel()
		return fmt.Errorf("dial reticulum %s: %w", c.rnsAddr, err)
	}
	c.tcpIface = tcpIface

	c.transport = rns.NewTransport(log.New(log.Writer(), "[RNS] ", log.Flags()))
	c.transport.AddInterface(tcpIface)
	// The library's keepalives refresh its idle clock. Put keepalive after
	// the idle deadline so unused outbound links expire instead of pinging forever.
	sweep := min(15*time.Second, c.linkIdleTimeout)
	c.transport.SetLinkLifetime(c.linkIdleTimeout*2, c.linkIdleTimeout, sweep)

	c.transport.RegisterAnnounceHandler(&clientAnnounceHandler{
		onAnnounce: func(a *rns.Announce) {
			c.mu.RLock()
			handler := c.OnAnnounce
			c.mu.RUnlock()
			if handler != nil {
				handler(a)
			}
		},
	})

	c.mu.Lock()
	for _, id := range c.identities {
		if err := c.registerDeliveryLocked(id); err != nil {
			c.mu.Unlock()
			cancel()
			tcpIface.Close()
			return fmt.Errorf("register delivery: %w", err)
		}
	}
	c.mu.Unlock()

	go c.transport.Run(ctx)
	if c.outbox != nil {
		c.outboxStarted = true
		go c.outbox.run(ctx)
	}
	go c.transport.RunLinkSweeper(ctx)

	// Restored dormant identities must also stay quiet across restarts.
	c.announceActive()

	// Periodic announce loop
	go func() {
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				c.announceActive()
			}
		}
	}()

	log.Printf("[LXMF] Client started on destination: %s (name: %q)", c.HexHash(), c.displayName)
	return nil
}

func (c *Client) registerDeliveryLocked(id *rns.Identity) error {
	destHash := id.DestinationHashFor(lxmf.FullName())
	hexKey := hex.EncodeToString(destHash)

	if _, exists := c.deliveries[hexKey]; exists {
		return nil
	}

	delivery, err := lxmf.NewDelivery(c.transport, id, func(context byte) (*rns.Packet, error) {
		appData, _ := rns.EncodeLXMFAppData([]byte(c.displayName), nil)
		return rns.BuildAnnounce(id, lxmf.FullName(), appData, nil)
	})
	if err != nil {
		return fmt.Errorf("create lxmf delivery: %w", err)
	}

	delivery.OnMessage = func(m *lxmf.Message) {
		c.mu.RLock()
		handler := c.OnMessage
		c.mu.RUnlock()

		if handler != nil {
			locHash := m.DestHash
			if len(locHash) == 0 {
				locHash = destHash
			}
			renderer, _ := ParseRenderer(m.Fields)
			replyTo, replyQuote := ParseReplyFields(m.Fields)
			msg := &InboundMessage{
				LocalHash:  append([]byte(nil), locHash...),
				SenderHash: append([]byte(nil), m.SourceHash...),
				MessageID:  append([]byte(nil), m.MessageID()...),
				Title:      string(m.Title),
				Content:    string(m.Content),
				Fields:     m.Fields,
				Renderer:   renderer,
				ReplyTo:    replyTo,
				ReplyQuote: replyQuote,
			}
			handler(msg)
		}
	}

	delivery.OnError = func(err error) {
		c.mu.RLock()
		errHandler := c.OnError
		c.mu.RUnlock()

		if errHandler != nil {
			errHandler(err)
		} else {
			log.Printf("[LXMF] Delivery error on %s: %v", hexKey, err)
		}
	}

	c.deliveries[hexKey] = delivery
	c.identities[hexKey] = id
	if _, ok := c.lastActivity[hexKey]; !ok {
		c.lastActivity[hexKey] = time.Now()
	}
	return nil
}

// RegisterIdentity dynamically adds a new LXMF identity and registers its delivery destination.
func (c *Client) RegisterIdentity(id *rns.Identity) error {
	if id == nil {
		return errors.New("identity cannot be nil")
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	destHash := id.DestinationHashFor(lxmf.FullName())
	hexKey := hex.EncodeToString(destHash)
	c.identities[hexKey] = id

	if c.transport != nil {
		if err := c.registerDeliveryLocked(id); err != nil {
			return err
		}
		go c.AnnounceIdentity(destHash)
	}
	return nil
}

func (c *Client) AnnounceIdentity(destHash []byte) {
	c.mu.RLock()
	hexKey := hex.EncodeToString(destHash)
	id, ok := c.identities[hexKey]
	transport := c.transport
	c.mu.RUnlock()

	if !ok || id == nil || transport == nil {
		return
	}

	appData, _ := rns.EncodeLXMFAppData([]byte(c.displayName), nil)
	pkt, err := rns.BuildAnnounce(id, lxmf.FullName(), appData, nil)
	if err != nil {
		log.Printf("[LXMF] BuildAnnounce error for %s: %v", hexKey, err)
		return
	}
	if err := transport.Broadcast(pkt); err != nil {
		log.Printf("[LXMF] Broadcast announce error for %s: %v", hexKey, err)
		return
	}
	log.Printf("[LXMF] Broadcast announce sent for %s", hexKey)
}

func (c *Client) AnnounceAll() {
	c.mu.RLock()
	hashes := make([][]byte, 0, len(c.identities))
	for _, id := range c.identities {
		hashes = append(hashes, id.DestinationHashFor(lxmf.FullName()))
	}
	c.mu.RUnlock()

	for _, h := range hashes {
		c.AnnounceIdentity(h)
	}
}

// Dormant identities remain registered and answer Reticulum path requests.
// Only unsolicited periodic announcements stop; the root stays discoverable.
func (c *Client) activeAnnounceHashes(now time.Time) [][]byte {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var hashes [][]byte
	for key, id := range c.identities {
		if id == c.initialID || c.chatIdleTimeout == 0 || now.Sub(c.lastActivity[key]) < c.chatIdleTimeout {
			hashes = append(hashes, id.DestinationHashFor(lxmf.FullName()))
		}
	}
	return hashes
}

func (c *Client) announceActive() {
	for _, hash := range c.activeAnnounceHashes(time.Now()) {
		c.AnnounceIdentity(hash)
	}
}

// Announce broadcasts an announce packet for the initial identity.
func (c *Client) Announce() {
	c.AnnounceIdentity(c.Hash())
}

// SendFrom sends an LXMF message originating from the specified local destination identity.
func (c *Client) SendFrom(localDestHash, recipientHash []byte, title, content string) error {
	if c.outbox != nil {
		_, err := c.QueueMessage(localDestHash, recipientHash, OutboundMessage{Title: title, Content: content, Control: true})
		return err
	}
	c.touchIdentity(localDestHash)
	defer c.touchIdentity(localDestHash)
	c.mu.RLock()
	localHex := hex.EncodeToString(localDestHash)
	delivery, ok := c.deliveries[localHex]
	transport := c.transport
	c.mu.RUnlock()

	if !ok || delivery == nil {
		return fmt.Errorf("delivery destination %s not found", localHex)
	}

	fields := map[any]any{
		FieldRenderer: RendererMarkdown,
	}
	err := delivery.Send(recipientHash, []byte(title), []byte(content), fields)
	if err != nil {
		if errors.Is(err, lxmf.ErrRecipientUnknown) && transport != nil {
			_ = transport.RequestPath(recipientHash)
		}
		return fmt.Errorf("send lxmf message: %w", err)
	}

	return nil
}

// SendMessage sends an LXMF message (with optional fields) from the given
// local destination. With durable delivery it returns the fixed hash after
// committing to disk; the dispatcher records delivery separately. Without an
// outbox it waits for the recipient's delivery proof.
func (c *Client) SendMessage(localDestHash, recipientHash []byte, m OutboundMessage) ([]byte, error) {
	if c.outbox != nil {
		return c.QueueMessage(localDestHash, recipientHash, m)
	}
	c.touchIdentity(localDestHash)
	defer c.touchIdentity(localDestHash)
	c.mu.RLock()
	localHex := hex.EncodeToString(localDestHash)
	delivery, ok := c.deliveries[localHex]
	identity := c.identities[localHex]
	transport := c.transport
	c.mu.RUnlock()

	if !ok || delivery == nil || identity == nil || transport == nil {
		return nil, fmt.Errorf("delivery destination %s not found", localHex)
	}

	if _, hasRenderer := ParseRenderer(m.Fields); !hasRenderer {
		m.Fields = WithRenderer(m.Fields, RendererMarkdown)
	}

	if !m.Direct {
		msgID, err := delivery.SendWithID(recipientHash, []byte(m.Title), []byte(m.Content), m.Fields)
		if err != nil {
			if errors.Is(err, lxmf.ErrRecipientUnknown) {
				_ = transport.RequestPath(recipientHash)
			}
			return nil, fmt.Errorf("send lxmf message: %w", err)
		}
		return msgID, nil
	}

	return c.sendDirect(delivery, identity, transport, localDestHash, recipientHash, m)
}

// sendDirect packs the message once in LXMF direct form and delivers it over
// a Reticulum Link, waiting for the link-level proof (DELIVERED). Retries
// reuse the same packed bytes, so the message hash is identical every time.
func (c *Client) sendDirect(delivery *lxmf.Delivery, identity *rns.Identity, transport *rns.Transport, localDestHash, recipientHash []byte, m OutboundMessage) ([]byte, error) {
	if len(recipientHash) != rns.IdentityHashLen {
		return nil, fmt.Errorf("recipient dest_hash must be %d bytes", rns.IdentityHashLen)
	}

	var (
		body    []byte
		msgID   []byte
		lastErr error
	)
	for attempt := 1; attempt <= directSendAttempts; attempt++ {
		if attempt > 1 {
			time.Sleep(directRetryBackoff * time.Duration(attempt-1))
		}

		if body == nil {
			known := transport.Recall(recipientHash)
			if known == nil {
				_ = transport.RequestPath(recipientHash)
				lastErr = fmt.Errorf("%w: %x", lxmf.ErrRecipientUnknown, recipientHash[:4])
				continue
			}

			opts := lxmf.StampOptions{}
			if !delivery.DisableOutboundStamps {
				if cost, err := rns.DecodeLXMFAppDataStampCost(known.AppData); err == nil {
					opts.Cost = cost
				}
				opts.MaxCost = delivery.MaxStampCost
				if delivery.Tickets != nil {
					opts.Ticket = delivery.Tickets.Held(recipientHash, time.Now())
				}
			}

			var err error
			body, msgID, err = lxmf.SignAndPackDirectStamped(identity, localDestHash, recipientHash,
				[]byte(m.Title), []byte(m.Content), m.Fields, opts)
			if err != nil {
				return nil, fmt.Errorf("pack direct: %w", err)
			}
		}

		timeout := delivery.LinkSendTimeout
		if timeout <= 0 {
			timeout = rns.DefaultLinkSendTimeout
		}
		if err := transport.SendOverLink(recipientHash, body, timeout); err != nil {
			lastErr = fmt.Errorf("link send: %w", err)
			log.Printf("[LXMF] Direct delivery attempt %d/%d to %x failed: %v", attempt, directSendAttempts, recipientHash, err)
			continue
		}
		return msgID, nil
	}
	return nil, fmt.Errorf("direct lxmf delivery failed: %w", lastErr)
}

// Send sends an LXMF message originating from the initial identity.
func (c *Client) Send(recipientHash []byte, title, content string) error {
	return c.SendFrom(c.Hash(), recipientHash, title, content)
}

func (c *Client) Hash() []byte {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.initialID != nil {
		return c.initialID.DestinationHashFor(lxmf.FullName())
	}
	for _, id := range c.identities {
		return id.DestinationHashFor(lxmf.FullName())
	}
	return nil
}

func (c *Client) HexHash() string {
	h := c.Hash()
	if h == nil {
		return ""
	}
	return hex.EncodeToString(h)
}

func (c *Client) Close() error {
	c.mu.Lock()
	cancel, iface := c.cancel, c.tcpIface
	c.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	var err error
	if iface != nil {
		err = iface.Close()
	}
	if c.outboxStarted {
		<-c.outbox.done
	}
	return err
}

type clientAnnounceHandler struct {
	onAnnounce func(*rns.Announce)
}

func (h *clientAnnounceHandler) AspectMatch(nameHash []byte) bool {
	return true
}

func (h *clientAnnounceHandler) OnAnnounce(a *rns.Announce) {
	if h.onAnnounce != nil {
		h.onAnnounce(a)
	}
}

func (c *Client) RestoreKnownIdentities(identities []*rns.KnownIdentity) {
	c.mu.RLock()
	t := c.transport
	c.mu.RUnlock()
	if t == nil {
		return
	}
	for _, k := range identities {
		t.Restore(k)
	}
}

func (c *Client) KnownSnapshot() []*rns.KnownIdentity {
	c.mu.RLock()
	t := c.transport
	c.mu.RUnlock()
	if t == nil {
		return nil
	}
	return t.KnownSnapshot()
}

func (c *Client) PinDestinations(destHashes [][]byte) {
	c.mu.RLock()
	t := c.transport
	c.mu.RUnlock()
	if t != nil {
		if c.outbox != nil && len(c.outbox.opts.PropagationNode) > 0 {
			destHashes = append(append([][]byte(nil), destHashes...), c.outbox.opts.PropagationNode)
		}
		t.PinDestinations(destHashes)
	}
}

func (c *Client) RequestPath(destHash []byte) error {
	c.mu.RLock()
	t := c.transport
	c.mu.RUnlock()
	if t == nil {
		return errors.New("transport not started")
	}
	return t.RequestPath(destHash)
}
