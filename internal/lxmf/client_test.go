package lxmf

import (
	"encoding/hex"
	"testing"

	reticulumLxmf "github.com/thatSFguy/reticulum-go/lxmf"
	"github.com/thatSFguy/reticulum-go/rns"
)

func TestClientIdentityManagement(t *testing.T) {
	id1, err := rns.NewIdentity()
	if err != nil {
		t.Fatalf("NewIdentity failed: %v", err)
	}

	c, err := NewClient("127.0.0.1:37428", "Test Client", id1)
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	h1 := id1.DestinationHashFor(reticulumLxmf.FullName())
	if c.HexHash() != hex.EncodeToString(h1) {
		t.Errorf("HexHash = %s, want %s", c.HexHash(), hex.EncodeToString(h1))
	}

	id2, err := rns.NewIdentity()
	if err != nil {
		t.Fatalf("NewIdentity id2 failed: %v", err)
	}

	if err := c.RegisterIdentity(id2); err != nil {
		t.Fatalf("RegisterIdentity failed: %v", err)
	}

	h2 := id2.DestinationHashFor(reticulumLxmf.FullName())
	hex2 := hex.EncodeToString(h2)

	c.mu.RLock()
	_, found1 := c.identities[hex.EncodeToString(h1)]
	_, found2 := c.identities[hex2]
	c.mu.RUnlock()

	if !found1 || !found2 {
		t.Errorf("expected both identities registered, found1=%v, found2=%v", found1, found2)
	}
}
