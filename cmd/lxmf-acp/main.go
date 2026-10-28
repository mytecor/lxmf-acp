package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/mytecor/lxmf-acp/internal/acp"
	"github.com/mytecor/lxmf-acp/internal/bridge"
	"github.com/mytecor/lxmf-acp/internal/config"
	"github.com/mytecor/lxmf-acp/internal/lxmf"
	"github.com/mytecor/lxmf-acp/internal/storage"
)

func main() {
	cfg := config.Parse()

	log.Println("==================================================")
	log.Println("       LXMF-ACP: Autonomous Agent Adapter         ")
	log.Println("==================================================")

	// 1. Identity
	identity, err := lxmf.LoadOrCreateIdentity(cfg.IdentityPath)
	if err != nil {
		log.Fatalf("Fatal: failed to load or create identity: %v", err)
	}
	log.Printf("Identity file: %s", cfg.IdentityPath)

	// 2. State Store
	store, err := storage.NewStore(cfg.StatePath)
	if err != nil {
		log.Fatalf("Fatal: failed to open state store: %v", err)
	}
	log.Printf("State file:    %s", cfg.StatePath)

	// Pending traffic is isolated from the small identity/config state file.
	deliveryPath := cfg.DeliveryPath
	if deliveryPath == "" {
		deliveryPath = cfg.StatePath + ".delivery.db"
	}
	deliveries, err := storage.OpenDeliveryStore(deliveryPath)
	if err != nil {
		log.Fatalf("Fatal: failed to open delivery database: %v", err)
	}
	defer deliveries.Close()
	deliveries.MaxPendingBytes = cfg.DeliveryMaxBytes
	log.Printf("Delivery database: %s", deliveryPath)
	// 3. ACP Client
	acpClient := acp.NewClient(cfg.ACPEndpoint)
	log.Printf("ACP Endpoint:  %s", cfg.ACPEndpoint)

	// 4. LXMF Client
	lxmfClient, err := lxmf.NewClient(cfg.RNSAddr, cfg.DisplayName, identity)
	if err != nil {
		log.Fatalf("Fatal: failed to create LXMF client: %v", err)
	}
	lxmfClient.ConfigureLifecycle(cfg.LinkIdleTimeout, cfg.ChatIdleTimeout)
	node, _ := hex.DecodeString(cfg.PropagationNode)
	lxmfClient.ConfigureDelivery(deliveries, lxmf.DeliveryOptions{Workers: cfg.DeliveryWorkers, RetryMin: cfg.DeliveryRetryMin, RetryMax: cfg.DeliveryRetryMax, SendTimeout: cfg.DeliveryTimeout, PropagationNode: node})
	log.Printf("RNS Shared:    %s", cfg.RNSAddr)
	log.Printf("Display Name:  %q", cfg.DisplayName)
	log.Printf("Agent Address: %s", lxmfClient.HexHash())
	if cfg.Admin != "" {
		log.Printf("Admin Address: %s (from config)", cfg.Admin)
	} else if admin := store.GetAdmin(); admin != "" {
		log.Printf("Admin Address: %s (from store)", admin)
	} else {
		log.Printf("Admin Address: Unassigned (first user will claim admin)")
	}
	if len(cfg.Allowlist) > 0 {
		log.Printf("Allowlist:     %d peers configured", len(cfg.Allowlist))
	} else {
		log.Printf("Allowlist:     Disabled (public access)")
	}

	// 5. Bridge
	b := bridge.NewBridge(cfg, lxmfClient, acpClient, store)
	b.ConfigureReliability(deliveries)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := b.Start(ctx); err != nil {
		log.Fatalf("Fatal: failed to start bridge: %v", err)
	}

	fmt.Printf("\n[READY] lxmf-acp is active and listening for messages!\n")
	fmt.Printf("[READY] Send LXMF messages to destination: <%s>\n\n", lxmfClient.HexHash())

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	<-sigChan
	log.Println("\nReceived shutdown signal. Stopping...")
	_ = b.Close()
	log.Println("Shutdown complete.")
}
