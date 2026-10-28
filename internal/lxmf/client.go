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

	mu         sync.RWMutex
	transport  *rns.Transport
	tcpIface   *rns.ReconnectingTCPClient
	deliveries map[string]*lxmf.Delivery
	identities map[string]*rns.Identity

	OnMessage MessageHandler
	OnError   func(error)
}

func NewClient(rnsAddr string, displayName string, identity *rns.Identity) (*Client, error) {
	if identity == nil {
		return nil, errors.New("identity cannot be nil")
	}

	c := &Client{
		rnsAddr:     rnsAddr,
		displayName: displayName,
		initialID:   identity,
		deliveries:  make(map[string]*lxmf.Delivery),
		identities:  make(map[string]*rns.Identity),
	}

	initialHash := identity.DestinationHashFor(lxmf.FullName())
	c.identities[hex.EncodeToString(initialHash)] = identity

	return c, nil
}

func (c *Client) SetMessageHandler(h MessageHandler) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.OnMessage = h
}

func (c *Client) Start(ctx context.Context) error {
	log.Printf("[LXMF] Connecting to Reticulum instance at %s...", c.rnsAddr)

	tcpIface, err := rns.DialReconnectingTCP(c.rnsAddr, 5*time.Second, nil)
	if err != nil {
		return fmt.Errorf("dial reticulum %s: %w", c.rnsAddr, err)
	}
	c.tcpIface = tcpIface

	c.transport = rns.NewTransport(nil)
	c.transport.AddInterface(tcpIface)

	c.mu.Lock()
	for _, id := range c.identities {
		if err := c.registerDeliveryLocked(id); err != nil {
			c.mu.Unlock()
			tcpIface.Close()
			return fmt.Errorf("register delivery: %w", err)
		}
	}
	c.mu.Unlock()

	go c.transport.Run(ctx)

	// Send initial announce for all registered identities
	c.AnnounceAll()

	// Periodic announce loop
	go func() {
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				c.AnnounceAll()
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
			msg := &InboundMessage{
				LocalHash:  append([]byte(nil), locHash...),
				SenderHash: append([]byte(nil), m.SourceHash...),
				MessageID:  append([]byte(nil), m.MessageID()...),
				Title:      string(m.Title),
				Content:    string(m.Content),
				Fields:     m.Fields,
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

// Announce broadcasts an announce packet for the initial identity.
func (c *Client) Announce() {
	c.AnnounceIdentity(c.Hash())
}

// SendFrom sends an LXMF message originating from the specified local destination identity.
func (c *Client) SendFrom(localDestHash, recipientHash []byte, title, content string) error {
	c.mu.RLock()
	localHex := hex.EncodeToString(localDestHash)
	delivery, ok := c.deliveries[localHex]
	transport := c.transport
	c.mu.RUnlock()

	if !ok || delivery == nil {
		return fmt.Errorf("delivery destination %s not found", localHex)
	}

	err := delivery.Send(recipientHash, []byte(title), []byte(content), nil)
	if err != nil {
		if errors.Is(err, lxmf.ErrRecipientUnknown) && transport != nil {
			_ = transport.RequestPath(recipientHash)
		}
		return fmt.Errorf("send lxmf message: %w", err)
	}

	return nil
}

// SendMessage sends an LXMF message (with optional fields) from the given
// local destination and returns its LXMF message hash. It returns only after
// the recipient's delivery proof has arrived (state DELIVERED); a message
// that merely left the local interface (SENT) is not reported as success.
func (c *Client) SendMessage(localDestHash, recipientHash []byte, m OutboundMessage) ([]byte, error) {
	c.mu.RLock()
	localHex := hex.EncodeToString(localDestHash)
	delivery, ok := c.deliveries[localHex]
	identity := c.identities[localHex]
	transport := c.transport
	c.mu.RUnlock()

	if !ok || delivery == nil || identity == nil || transport == nil {
		return nil, fmt.Errorf("delivery destination %s not found", localHex)
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
	defer c.mu.Unlock()
	if c.tcpIface != nil {
		return c.tcpIface.Close()
	}
	return nil
}
