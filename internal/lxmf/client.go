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
}

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
