package config

import (
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	IdentityPath         string
	StatePath            string
	ACPEndpoint          string
	RNSAddr              string
	DisplayName          string
	Admin                string
	Allowlist            []string
	MaxIdentitiesPerUser int
	NewChatCooldownSec   int
	OutboundReplies      bool
	LinkIdleTimeout      time.Duration
	QueueIdleTimeout     time.Duration
	ChatIdleTimeout      time.Duration
	DeliveryPath         string
	PropagationNode      string
	DeliveryTimeout      time.Duration
	DeliveryRetryMin     time.Duration
	DeliveryRetryMax     time.Duration
	DeliveryWorkers      int
	DeliveryMaxBytes     int64
}

func Parse() *Config {
	cfg := &Config{}

	flag.StringVar(&cfg.IdentityPath, "identity", "./identity", "Path to Reticulum identity file")
	flag.StringVar(&cfg.StatePath, "state", "./state.json", "Path to session state file")
	flag.StringVar(&cfg.ACPEndpoint, "acp", "ws://localhost:8080/acp", "ACP agent WebSocket endpoint URL")
	flag.StringVar(&cfg.RNSAddr, "rns", "127.0.0.1:37428", "Reticulum shared instance TCP address")
	flag.StringVar(&cfg.DisplayName, "name", "LXMF Agent", "Display name announced in LXMF")
	flag.StringVar(&cfg.Admin, "admin", "", "Administrator destination hash (has permissions for /bridge allow, /bridge revoke, /bridge whitelist)")
	flag.IntVar(&cfg.MaxIdentitiesPerUser, "max-user-identities", 20, "Maximum number of chats/identities allowed per user (0 for unlimited)")
	flag.IntVar(&cfg.NewChatCooldownSec, "new-chat-cooldown", 5, "Minimum cooldown in seconds between /bridge new commands per user")
	flag.BoolVar(&cfg.OutboundReplies, "outbound-replies", true, "Include reply-to and quote metadata when responding to user replies")
	flag.DurationVar(&cfg.LinkIdleTimeout, "link-idle-timeout", 2*time.Minute, "Reticulum link inactivity timeout (must be positive)")
	flag.DurationVar(&cfg.QueueIdleTimeout, "queue-idle-timeout", 5*time.Minute, "Release idle conversation workers and detach Hydra sessions (0 disables)")
	flag.DurationVar(&cfg.ChatIdleTimeout, "chat-idle-timeout", 24*time.Hour, "Stop periodic announces for inactive dynamic chats (0 disables)")

	registerDeliveryFlags(cfg)
	var allowlistStr string
	flag.StringVar(&allowlistStr, "allowlist", "", "Comma-separated list of allowed peer destination hashes (empty allows all)")

	flag.Parse()
	if err := applyDeliveryEnv(cfg); err != nil {
		log.Fatal(err)
	}
	if err := applyLifecycleEnv(cfg); err != nil {
		log.Fatal(err)
	}

	// Environment variable overrides if flag is default
	if env := os.Getenv("LXMF_ACP_IDENTITY"); env != "" && cfg.IdentityPath == "./identity" {
		cfg.IdentityPath = env
	}
	if env := os.Getenv("LXMF_ACP_STATE"); env != "" && cfg.StatePath == "./state.json" {
		cfg.StatePath = env
	}
	if env := os.Getenv("LXMF_ACP_ENDPOINT"); env != "" && cfg.ACPEndpoint == "ws://localhost:8080/acp" {
		cfg.ACPEndpoint = env
	}
	if env := os.Getenv("LXMF_ACP_RNS"); env != "" && cfg.RNSAddr == "127.0.0.1:37428" {
		cfg.RNSAddr = env
	}
	if env := os.Getenv("LXMF_ACP_NAME"); env != "" && cfg.DisplayName == "LXMF Agent" {
		cfg.DisplayName = env
	}
	if env := os.Getenv("LXMF_ACP_ADMIN"); env != "" && cfg.Admin == "" {
		cfg.Admin = env
	}
	if env := os.Getenv("LXMF_ACP_ALLOWLIST"); env != "" && allowlistStr == "" {
		allowlistStr = env
	}
	if env := os.Getenv("LXMF_ACP_MAX_USER_IDENTITIES"); env != "" && cfg.MaxIdentitiesPerUser == 20 {
		if val, err := strconv.Atoi(env); err == nil {
			cfg.MaxIdentitiesPerUser = val
		}
	}
	if env := os.Getenv("LXMF_ACP_NEW_CHAT_COOLDOWN"); env != "" && cfg.NewChatCooldownSec == 5 {
		if val, err := strconv.Atoi(env); err == nil {
			cfg.NewChatCooldownSec = val
		}
	}
	if env := os.Getenv("LXMF_ACP_OUTBOUND_REPLIES"); env != "" && cfg.OutboundReplies {
		if val, err := strconv.ParseBool(env); err == nil {
			cfg.OutboundReplies = val
		}
	}

	if cfg.Admin != "" {
		cfg.Admin = strings.ToLower(strings.Trim(cfg.Admin, "<> \t\r\n"))
	}

	if allowlistStr != "" {
		parts := strings.Split(allowlistStr, ",")
		for _, p := range parts {
			trimmed := strings.ToLower(strings.TrimSpace(p))
			if trimmed != "" {
				cfg.Allowlist = append(cfg.Allowlist, trimmed)
			}
		}
	}

	return cfg
}

func applyLifecycleEnv(cfg *Config) error {
	for _, setting := range []struct {
		flag   string
		env    string
		target *time.Duration
	}{
		{"link-idle-timeout", "LXMF_ACP_LINK_IDLE_TIMEOUT", &cfg.LinkIdleTimeout},
		{"queue-idle-timeout", "LXMF_ACP_QUEUE_IDLE_TIMEOUT", &cfg.QueueIdleTimeout},
		{"chat-idle-timeout", "LXMF_ACP_CHAT_IDLE_TIMEOUT", &cfg.ChatIdleTimeout},
	} {
		explicit := false
		flag.Visit(func(f *flag.Flag) {
			if f.Name == setting.flag {
				explicit = true
			}
		})
		if !explicit {
			if value := os.Getenv(setting.env); value != "" {
				duration, err := time.ParseDuration(value)
				if err != nil {
					return fmt.Errorf("invalid %s: %w", setting.env, err)
				}
				*setting.target = duration
			}
		}
		if *setting.target < 0 || (setting.flag == "link-idle-timeout" && *setting.target == 0) {
			return fmt.Errorf("invalid --%s: duration must be positive (queue/chat allow 0 to disable)", setting.flag)
		}
	}
	return nil
}

func (c *Config) IsAllowed(peerHex string) bool {
	if len(c.Allowlist) == 0 {
		return true
	}
	peerHex = strings.ToLower(strings.TrimSpace(peerHex))
	for _, allowed := range c.Allowlist {
		if allowed == peerHex {
			return true
		}
	}
	return false
}

func applyDeliveryEnv(c *Config) error {
	for _, name := range []string{"delivery-db", "propagation-node", "delivery-timeout", "delivery-retry-min", "delivery-retry-max", "delivery-workers", "delivery-max-bytes"} {
		explicit := false
		flag.Visit(func(f *flag.Flag) {
			if f.Name == name {
				explicit = true
			}
		})
		if !explicit {
			env := "LXMF_ACP_" + strings.ToUpper(strings.ReplaceAll(name, "-", "_"))
			if value := os.Getenv(env); value != "" {
				if err := flag.Set(name, value); err != nil {
					return fmt.Errorf("%s: %w", env, err)
				}
			}
		}
	}
	if c.DeliveryTimeout <= 0 || c.DeliveryRetryMin <= 0 || c.DeliveryRetryMax < c.DeliveryRetryMin || c.DeliveryWorkers < 1 || c.DeliveryMaxBytes < 1 {
		return fmt.Errorf("delivery timeout, retry intervals, workers and byte budget must be positive; retry-max must be >= retry-min")
	}
	c.PropagationNode = strings.ToLower(strings.Trim(c.PropagationNode, "<> \t\r\n"))
	if c.PropagationNode != "" {
		raw, err := hex.DecodeString(c.PropagationNode)
		if err != nil || len(raw) != 16 {
			return fmt.Errorf("propagation-node must be a 16-byte destination hash")
		}
	}
	return nil
}

func registerDeliveryFlags(cfg *Config) {
	flag.StringVar(&cfg.DeliveryPath, "delivery-db", "", "Pending delivery database (default: <state>.delivery.db)")
	flag.StringVar(&cfg.PropagationNode, "propagation-node", "", "Optional LXMF propagation destination hash for offline delivery")
	flag.DurationVar(&cfg.DeliveryTimeout, "delivery-timeout", 45*time.Second, "Per-attempt delivery budget; increase for slow radio routes")
	flag.DurationVar(&cfg.DeliveryRetryMin, "delivery-retry-min", 15*time.Second, "Initial outgoing retry interval")
	flag.DurationVar(&cfg.DeliveryRetryMax, "delivery-retry-max", 15*time.Minute, "Maximum outgoing retry interval")
	flag.IntVar(&cfg.DeliveryWorkers, "delivery-workers", 4, "Maximum concurrent outgoing conversations")
	flag.Int64Var(&cfg.DeliveryMaxBytes, "delivery-max-bytes", 128<<20, "Maximum retained outgoing envelope bytes")
}
