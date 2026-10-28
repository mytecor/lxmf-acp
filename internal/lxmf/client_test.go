package lxmf

import (
	"bytes"
	"context"
	"encoding/hex"
	"io"
	"net"
	"testing"
	"time"

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

func TestDormantAddressesRemainRegisteredAndWakeOnActivity(t *testing.T) {
	root, _ := rns.NewIdentity()
	dynamic, _ := rns.NewIdentity()
	c, err := NewClient("", "test", root)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.RegisterIdentity(dynamic); err != nil {
		t.Fatal(err)
	}
	dest := dynamic.DestinationHashFor(reticulumLxmf.FullName())
	now := time.Now()
	c.SetIdentityLastActivity(dest, now.Add(-48*time.Hour))
	if hashes := c.activeAnnounceHashes(now); len(hashes) != 1 || !bytes.Equal(hashes[0], c.Hash()) {
		t.Fatal("dormant identity is still periodically announced, or root disappeared")
	}
	if len(c.identities) != 2 {
		t.Fatal("dormant identity was destroyed")
	}
	c.SetIdentityLastActivity(dest, now)
	if len(c.activeAnnounceHashes(now)) != 2 {
		t.Fatal("incoming activity did not wake identity")
	}
	c.SetIdentityLastActivity(dest, now.Add(-48*time.Hour))
	c.ConfigureLifecycle(time.Minute, 0)
	if len(c.activeAnnounceHashes(now)) != 2 {
		t.Fatal("zero timeout should disable dormancy")
	}
}

// Exercise Start, not just the library: without launching RunLinkSweeper,
// these real negotiated links would remain active forever.
func TestClientClosesIdleLinksAndCanNegotiateAgain(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		_, _ = io.Copy(io.Discard, conn)
	}()
	root, _ := rns.NewIdentity()
	peer, _ := rns.NewIdentity()
	c, err := NewClient(listener.Addr().String(), "test", root)
	if err != nil {
		t.Fatal(err)
	}
	c.ConfigureLifecycle(50*time.Millisecond, time.Hour)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	dest := peer.DestinationHashFor(reticulumLxmf.FullName())
	negotiate := func() *rns.Link {
		t.Helper()
		link, request, err := c.transport.LinkManager().StartLinkAsInitiator(dest, nil)
		if err != nil {
			t.Fatal(err)
		}
		_, proof, err := rns.NewLinkManager().AcceptIncomingLinkRequest(request, peer, nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := c.transport.LinkManager().HandleLRProof(proof, peer.PublicKey()[32:]); err != nil {
			t.Fatal(err)
		}
		return link
	}
	first := negotiate()
	deadline := time.Now().Add(time.Second)
	for first.IsActive() && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if first.IsActive() || c.transport.LinkManager().ActiveCount() != 0 {
		t.Fatal("idle link was not cleaned up")
	}
	second := negotiate()
	if !second.IsActive() || bytes.Equal(first.ID, second.ID) {
		t.Fatal("link did not reopen with a fresh handshake")
	}
}
