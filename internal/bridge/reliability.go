package bridge

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/mytecor/lxmf-acp/internal/lxmf"
	"github.com/mytecor/lxmf-acp/internal/storage"
	"github.com/vmihailenco/msgpack/v5"
)

// ConfigureReliability must be called before Start. The database is owned by main.
func (b *Bridge) ConfigureReliability(s *storage.DeliveryStore) { b.delivery = s }

func (b *Bridge) persistIncoming(msg *lxmf.InboundMessage) (bool, error) {
	if b.delivery == nil {
		return true, nil
	}
	payload, err := msgpack.Marshal(msg)
	if err != nil {
		return false, err
	}
	return b.delivery.AcceptIncoming(chatKeyOf(msg), msg.MessageID, payload)
}
func (b *Bridge) readyIncoming(msg *lxmf.InboundMessage) error {
	if b.delivery == nil {
		return nil
	}
	payload, err := msgpack.Marshal(msg)
	if err != nil {
		return err
	}
	return b.delivery.ReadyIncoming(chatKeyOf(msg), msg.MessageID, payload, msg.Parts)
}
func (b *Bridge) recoverIncoming() error {
	chats, err := b.delivery.BufferedChats()
	if err != nil {
		return err
	}
	for _, chat := range chats {
		if err := b.assembleSavedChat(chat); err != nil {
			return err
		}
	}
	uncertain, err := b.delivery.Incoming("uncertain")
	if err != nil {
		return err
	}
	if len(uncertain) > 0 {
		log.Printf("[Inbox] %d interrupted requests will NOT be re-executed; use /bridge recover", len(uncertain))
	}
	return nil
}
func (b *Bridge) pumpIncoming(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		rows, err := b.delivery.Incoming("queued")
		if err != nil {
			log.Printf("[Inbox] Read failed: %v", err)
		} else {
			for _, row := range rows {
				if ctx.Err() != nil {
					return
				}
				var msg lxmf.InboundMessage
				if err := decodeIncoming(row.Payload, &msg); err != nil {
					log.Printf("[Inbox] Decode failed for %x: %v", row.ID, err)
					continue
				}
				b.queue.Enqueue(&msg)
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
func (q *peerQueue) deliveryCommand(text string) bool {
	if q.bridge.delivery == nil {
		return false
	}
	switch text {
	case "/delivery":
		summary, err := q.bridge.delivery.Summary(q.localHash, q.peerHash, q.localHex+":"+q.peerHex)
		if err != nil {
			log.Printf("[Delivery] Status failed: %v", err)
			return true
		}
		_, _ = q.bridge.lxmfClient.SendMessage(q.localHash, q.peerHash, lxmf.OutboundMessage{Title: "Delivery", Content: summary, Control: true})
		return true
	case "/retry":
		if err := q.bridge.delivery.RetryChat(q.localHash, q.peerHash); err != nil {
			log.Printf("[Delivery] Retry failed: %v", err)
		}
		if w, ok := q.bridge.lxmfClient.(interface{ WakeDelivery([]byte) }); ok {
			w.WakeDelivery(q.peerHash)
		}
		return true
	case "/recover":
		output, err := q.bridge.delivery.RecoverOutput(q.localHex + ":" + q.peerHex)
		if err != nil {
			log.Printf("[Inbox] Recovery lookup failed: %v", err)
			return true
		}
		if strings.TrimSpace(output) == "" {
			output = "No saved text from an interrupted request. Commands interrupted by a restart are never automatically repeated."
		} else {
			output = "Recovered text from an interrupted request (execution may be incomplete):\n\n" + output
		}
		_ = q.bridge.lxmfClient.SendFrom(q.localHash, q.peerHash, "Recovery", output)
		return true
	}
	return false
}
func incomingChat(local, peer []byte) string {
	return hex.EncodeToString(local) + ":" + hex.EncodeToString(peer)
}

// LXMF dictionaries use numeric keys, including nested custom metadata.
func decodeIncoming(raw []byte, msg *lxmf.InboundMessage) error {
	d := msgpack.NewDecoder(bytes.NewReader(raw))
	d.SetMapDecoder(func(d *msgpack.Decoder) (interface{}, error) { return d.DecodeUntypedMap() })
	return d.Decode(msg)
}

// Rebuild from disk in dependency order. Missing predecessors remain journaled,
// so even >64 out-of-order parts and long offline periods need no volatile stash.
func (b *Bridge) assembleSavedChat(chat string) error {
	b.assemblyMu.Lock()
	defer b.assemblyMu.Unlock()
	rows, err := b.delivery.BufferedChat(chat)
	if err != nil {
		return err
	}
	children := map[string][]*lxmf.InboundMessage{}
	var roots []*lxmf.InboundMessage
	for _, row := range rows {
		var msg lxmf.InboundMessage
		if err := decodeIncoming(row.Payload, &msg); err != nil {
			return fmt.Errorf("decode saved fragment: %w", err)
		}
		info := lxmf.ParseMultipartFields(msg.Fields)
		if len(info.Prev) == 0 {
			roots = append(roots, &msg)
		} else {
			key := hex.EncodeToString(info.Prev)
			children[key] = append(children[key], &msg)
		}
	}
	assembler := NewAssembler(DefaultAssemblyTimeout)
	for len(roots) > 0 {
		msg := roots[0]
		roots = roots[1:]
		ready := assembler.Process(msg)
		roots = append(roots, children[hex.EncodeToString(msg.MessageID)]...)
		if ready != nil {
			if err := b.readyIncoming(ready); err != nil {
				return err
			}
			b.queue.Enqueue(ready)
		}
	}
	return nil
}
