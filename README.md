# LXMF-ACP: Autonomous Agent Adapter

An autonomous Go adapter bridging any ACP-compliant agent (Codex, Pi, etc.) into an LXMF participant reachable from Sideband, Columba, and any Reticulum/LXMF client.

```
Sideband / Columba
      │
      ▼ (LXMF / Reticulum)
   lxmf-acp
      │  (Messages ↔ ACP Sessions, State, Queue)
      ▼ (ACP over WebSocket / JSON-RPC)
  ACP Agent (Codex, Pi, Custom Agent)
```

## Features

- **Fully Native Go Stack**: Built with `thatSFguy/reticulum-go` (pure Go without CGo) and `ironpark/acp-go` (official ACP v1 protocol).
- **Reticulum Compatibility**: Connects to an existing `rnsd` instance via TCP shared instance (`127.0.0.1:37428`), automatically reconnecting on daemon restart.
- **Dynamic LXMF Identities**: Create independent chats via the `/new` command without parent/child hierarchies. All identities operate as peers over a single RNS connection.
- **Session Model**: Context isolation keyed by `(local_lxmf_destination, remote_lxmf_destination) ↔ sessionId`. New identities only accept messages from their creator; different chats of the same user never share ACP context.
- **Message Queue**: Sequential per-dialogue message processing preventing race conditions and conflicting requests.
- **Built-in Chat Commands**:
  - `/new` — Spawn a new independent dialogue with a new LXMF identity (sends a welcome message from the newly generated address).
  - `/cancel` — Cancel active generation for the current session.
  - `/help` — Show the command help menu.
- **Access Control**: Optional allowlist support for authorized LXMF destination hashes.
- **Persistent State**: Persists private keys of generated identities, ownership records, and session mappings with atomic writes.
- **Multipart LXMF Streaming**: Transparently handles arbitrary-length responses via standard `FIELD_CONTINUATION (0x42)` and namespaced `FIELD_CUSTOM_META (0xFD)`. Segments text into parts under 10 000 UTF-16 units without breaking unicode/whitespace. Delivers sequentially via `LXMF DIRECT` during generation and reassembles incoming multipart chains from peer instances before invoking ACP. Compatible with Sideband, Columba, and Ratspeak.
- **Markdown Rendering**: Sets LXMF `FIELD_RENDERER (0x0F)` to `RENDERER_MARKDOWN (0x02)` across all agent responses, notifications, and commands. Compatible clients (such as Columba, Sideband, and MeshChat) render formatted markdown, lists, code blocks, and emphasis natively.

---

## Build

Requires Go 1.26+:

```bash
go build -o lxmf-acp ./cmd/lxmf-acp
```

---

## Usage

```bash
./lxmf-acp \
  --identity ./identity \
  --state ./state.json \
  --acp ws://localhost:8080/acp \
  --rns 127.0.0.1:37428 \
  --name "My AI Agent"
```

### CLI Parameters & Environment Variables

| Flag | Environment Variable | Default | Description |
|---|---|---|---|
| `--identity` | `LXMF_ACP_IDENTITY` | `./identity` | Path to Reticulum identity file (64-byte keypair) |
| `--state` | `LXMF_ACP_STATE` | `./state.json` | Path to JSON state file for session and identity persistence |
| `--acp` | `LXMF_ACP_ENDPOINT` | `ws://localhost:8080/acp` | WebSocket URL of ACP agent |
| `--rns` | `LXMF_ACP_RNS` | `127.0.0.1:37428` | TCP address of Reticulum shared instance (`rnsd`) |
| `--name` | `LXMF_ACP_NAME` | `LXMF Agent` | Agent display name for LXMF announces |
| `--allowlist` | `LXMF_ACP_ALLOWLIST` | *(empty)* | Comma-separated list of allowed LXMF destination hashes (empty allows all) |
| `--max-user-identities` | `LXMF_ACP_MAX_USER_IDENTITIES` | `20` | Maximum dynamic identities allowed per user (0 for unlimited) |
| `--new-chat-cooldown` | `LXMF_ACP_NEW_CHAT_COOLDOWN` | `5` | Minimum cooldown in seconds between `/new` commands per user |

---

## Project Structure

```text
lxmf-acp/
├── cmd/
│   └── lxmf-acp/
│       └── main.go       # Entrypoint, CLI flag parsing, execution and graceful shutdown
├── internal/
│   ├── config/           # Configuration and allowlist validation
│   ├── storage/          # Atomic JSON store for identities and peer ↔ ACP sessions
│   ├── lxmf/
│   │   ├── client.go     # LXMF client (Transport, Delivery, Announce, Reconnecting TCP)
│   │   └── identity.go   # Reticulum identity loader / generator (64-byte keypair)
│   ├── acp/
│   │   └── client.go     # ACP WebSocket client (v1, Initialize, Prompt, Cancel, Safe Permissions)
│   └── bridge/
│       ├── bridge.go     # Message dispatch between LXMF and ACP
│       ├── sessions.go   # Agent session management (create/load/reset)
│       └── queue.go      # Per-peer queue and command handling (/new, /cancel)
└── go.mod
```

---

## License

MIT
