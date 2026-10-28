package config

import (
	"flag"
	"os"
	"strconv"
	"strings"
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
}

func Parse() *Config {
	cfg := &Config{}

	flag.StringVar(&cfg.IdentityPath, "identity", "./identity", "Path to Reticulum identity file")
	flag.StringVar(&cfg.StatePath, "state", "./state.json", "Path to session state file")
	flag.StringVar(&cfg.ACPEndpoint, "acp", "ws://localhost:8080/acp", "ACP agent WebSocket endpoint URL")
	flag.StringVar(&cfg.RNSAddr, "rns", "127.0.0.1:37428", "Reticulum shared instance TCP address")
	flag.StringVar(&cfg.DisplayName, "name", "LXMF Agent", "Display name announced in LXMF")
	flag.StringVar(&cfg.Admin, "admin", "", "Administrator destination hash (has permissions for /allow, /revoke, /whitelist)")
	flag.IntVar(&cfg.MaxIdentitiesPerUser, "max-user-identities", 20, "Maximum number of chats/identities allowed per user (0 for unlimited)")
	flag.IntVar(&cfg.NewChatCooldownSec, "new-chat-cooldown", 5, "Minimum cooldown in seconds between /new commands per user")
	flag.BoolVar(&cfg.OutboundReplies, "outbound-replies", true, "Include reply-to and quote metadata when responding to user replies")

	var allowlistStr string
	flag.StringVar(&allowlistStr, "allowlist", "", "Comma-separated list of allowed peer destination hashes (empty allows all)")

	flag.Parse()

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
