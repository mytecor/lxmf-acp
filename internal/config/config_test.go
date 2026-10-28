package config

import (
	"flag"
	"testing"
	"time"
)

func TestIsAllowed(t *testing.T) {
	cfg := &Config{
		Allowlist: []string{"deadbeef", "1234abcd"},
	}

	if !cfg.IsAllowed("deadbeef") {
		t.Errorf("expected deadbeef to be allowed")
	}
	if !cfg.IsAllowed("DEADBEEF") {
		t.Errorf("expected case-insensitive matching")
	}
	if cfg.IsAllowed("cafebabe") {
		t.Errorf("expected cafebabe to be rejected")
	}

	openCfg := &Config{Allowlist: nil}
	if !openCfg.IsAllowed("anyhex") {
		t.Errorf("expected empty allowlist to allow all")
	}
}

func TestLifecycleEnvironmentAndExplicitFlagPrecedence(t *testing.T) {
	old := flag.CommandLine
	defer func() { flag.CommandLine = old }()
	flag.CommandLine = flag.NewFlagSet("test", flag.ContinueOnError)
	cfg := &Config{LinkIdleTimeout: 2 * time.Minute, QueueIdleTimeout: 5 * time.Minute, ChatIdleTimeout: 24 * time.Hour}
	flag.DurationVar(&cfg.LinkIdleTimeout, "link-idle-timeout", cfg.LinkIdleTimeout, "")
	flag.DurationVar(&cfg.QueueIdleTimeout, "queue-idle-timeout", cfg.QueueIdleTimeout, "")
	flag.DurationVar(&cfg.ChatIdleTimeout, "chat-idle-timeout", cfg.ChatIdleTimeout, "")
	t.Setenv("LXMF_ACP_LINK_IDLE_TIMEOUT", "3m")
	t.Setenv("LXMF_ACP_QUEUE_IDLE_TIMEOUT", "1h")
	t.Setenv("LXMF_ACP_CHAT_IDLE_TIMEOUT", "0")
	// An explicit default value must still override the environment.
	if err := flag.CommandLine.Parse([]string{"--queue-idle-timeout=5m"}); err != nil {
		t.Fatal(err)
	}
	if err := applyLifecycleEnv(cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.LinkIdleTimeout != 3*time.Minute || cfg.QueueIdleTimeout != 5*time.Minute || cfg.ChatIdleTimeout != 0 {
		t.Fatalf("unexpected config: %+v", cfg)
	}
	t.Setenv("LXMF_ACP_LINK_IDLE_TIMEOUT", "0")
	if err := applyLifecycleEnv(cfg); err == nil {
		t.Fatal("zero link timeout accepted")
	}
	t.Setenv("LXMF_ACP_LINK_IDLE_TIMEOUT", "invalid")
	if err := applyLifecycleEnv(cfg); err == nil {
		t.Fatal("invalid timeout accepted")
	}
}

func TestAdminConfig(t *testing.T) {
	cfg := &Config{
		Admin: "626e3c20bc183c6e9257a3b908c4181e",
	}
	if cfg.Admin != "626e3c20bc183c6e9257a3b908c4181e" {
		t.Errorf("unexpected admin: %s", cfg.Admin)
	}
}

func TestDeliveryOptionsRespectExplicitFlagsAndRejectInvalidNode(t *testing.T) {
	old := flag.CommandLine
	defer func() { flag.CommandLine = old }()
	flag.CommandLine = flag.NewFlagSet("delivery", flag.ContinueOnError)
	cfg := &Config{}
	registerDeliveryFlags(cfg)
	t.Setenv("LXMF_ACP_DELIVERY_TIMEOUT", "3m")
	t.Setenv("LXMF_ACP_DELIVERY_RETRY_MIN", "20s")
	t.Setenv("LXMF_ACP_PROPAGATION_NODE", "<00112233445566778899aabbccddeeff>")
	if err := flag.CommandLine.Parse([]string{"--delivery-timeout=45s"}); err != nil {
		t.Fatal(err)
	}
	if err := applyDeliveryEnv(cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.DeliveryTimeout != 45*time.Second || cfg.DeliveryRetryMin != 20*time.Second || cfg.PropagationNode != "00112233445566778899aabbccddeeff" {
		t.Fatal("delivery precedence failed", cfg)
	}
	if err := flag.Set("propagation-node", "short"); err != nil {
		t.Fatal(err)
	}
	if err := applyDeliveryEnv(cfg); err == nil {
		t.Fatal("invalid node accepted")
	}
}
