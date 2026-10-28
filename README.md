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
- **Dynamic LXMF Identities**: Create independent chats via the `/bridge new` command without parent/child hierarchies. All identities operate as peers over a single RNS connection.
- **Unified Chat Model & Author Isolation**: Every chat (both root and dynamic) has a single author (`Owner`); only the author can send messages to that chat. The initial chat belongs to the administrator, and invited users receive their own private chats.
- **Message Queue**: Sequential per-dialogue message processing preventing race conditions and conflicting requests.
- **Automatic Idle Cleanup**: Releases idle conversation workers and Hydra subscriptions, expires unused DIRECT links, and stops periodically announcing inactive dynamic addresses. Identity keys, ownership, and ACP session mappings remain available for automatic resume.
- **Built-in Chat Commands**:
  - `/bridge new` — Spawn a new independent dialogue with a new LXMF identity (sends a welcome message from the newly generated address).
  - `/bridge cancel` — Cancel active generation for the current session.
  - `/bridge help` — Show the command help menu.
  - `/bridge allow <hash>` — *(Admin only)* Add a user to the persistent whitelist, automatically create a dedicated personal chat identity for them, and proactively send a welcome message directly to the user.
  - `/bridge revoke <hash>` — *(Admin only)* Remove a user from the whitelist.
  - `/bridge whitelist` — *(Admin only)* Display currently whitelisted destination hashes and the administrator.
- **Access Control & Dynamic Whitelist**: Persistent whitelist stored in `state.json` with CLI `--allowlist` seeding. Unauthorized users are blocked, and when an admin adds a user, the bot initiates the conversation directly.
- **Persistent State**: Stores each chat’s private key, owner, activity, and optional ACP session together in `chats`, with atomic writes. The peer cache is stored separately in `knownIdentities`.
- **Multipart LXMF Streaming**: Transparently handles arbitrary-length responses via standard `FIELD_CONTINUATION (0x42)` and namespaced `FIELD_CUSTOM_META (0xFD)`. Segments text into parts under 10 000 UTF-16 units without breaking unicode/whitespace. Delivers sequentially via `LXMF DIRECT` during generation and reassembles incoming multipart chains from peer instances before invoking ACP. Compatible with Sideband, Columba, and Ratspeak.
- **Tool Call Display**: Forwards ACP tool calls as plain-text activity messages readable in Ratspeak and Columba. Compact display is enabled by default; `/bridge tools off|compact|verbose` selects a persistent per-chat mode, and `/bridge tool <number>` retrieves details of a recent call. Fast calls are batched, long calls show progress, and tool messages share the answer delivery queue without joining its multipart chain.
- **Markdown Rendering**: Sets LXMF `FIELD_RENDERER (0x0F)` to `RENDERER_MARKDOWN (0x02)` across all agent responses, notifications, and commands. Compatible clients (such as Columba, Sideband, and MeshChat) render formatted markdown, lists, code blocks, and emphasis natively.
- **Native Reply & Threading Support**: Supports standard LXMF reply fields (`FIELD_REPLY_TO 0x30` and `FIELD_REPLY_QUOTE 0x31`). When you reply to any message in client interfaces like Sideband, Columba, or MeshChat, the bridge passes the client-provided quote to the ACP agent and threads the agent's response back to your reply. Without a quote, the agent receives only the parent message ID. Message text is neither cached nor saved in the state file.

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
| `--admin` | `LXMF_ACP_ADMIN` | *(empty)* | Administrator destination hash (grants rights for `/bridge allow`, `/bridge revoke`, `/bridge whitelist`) |
| `--allowlist` | `LXMF_ACP_ALLOWLIST` | *(empty)* | Comma-separated list of allowed LXMF destination hashes (empty allows all) |
| `--max-user-identities` | `LXMF_ACP_MAX_USER_IDENTITIES` | `20` | Maximum dynamic identities allowed per user (0 for unlimited) |
| `--new-chat-cooldown` | `LXMF_ACP_NEW_CHAT_COOLDOWN` | `5` | Minimum cooldown in seconds between `/bridge new` commands per user |
| `--outbound-replies` | `LXMF_ACP_OUTBOUND_REPLIES` | `true` | Include reply-to and quote metadata when responding to user replies |
| `--link-idle-timeout` | `LXMF_ACP_LINK_IDLE_TIMEOUT` | `2m` | Close Reticulum links after network inactivity; must be positive |
| `--queue-idle-timeout` | `LXMF_ACP_QUEUE_IDLE_TIMEOUT` | `5m` | Release idle conversation workers and detach Hydra subscriptions; `0` disables |
| `--chat-idle-timeout` | `LXMF_ACP_CHAT_IDLE_TIMEOUT` | `24h` | Stop periodic announces for inactive dynamic chat addresses; `0` disables |

Durations use Go syntax such as `30s`, `5m`, or `48h`. Explicit lifecycle flags override environment values, including when the flag equals its default.

## Idle chats and Hydra session commands

The bridge keeps one shared TCP connection to Reticulum so it can receive messages. Individual DIRECT links expire after network inactivity; an outgoing link is negotiated again when needed. Cleanup runs at most every 15 seconds. Local keepalives do not prolong unused outbound links; incoming traffic, including peer keepalives, refreshes the library's network inactivity clock.

A conversation worker is released after its queue has been empty for `--queue-idle-timeout`, measured after processing and reply delivery finish. Active generation and delivery are not retired. With Hydra, the bridge sends `session/detach` for that conversation without closing the shared ACP WebSocket or other conversations. Hydra owns its agent-process idle policy (`sessionIdleTimeoutSeconds`; the pinned 0.1.183 default is one hour) and retains prompted conversations for resurrection. Other ACP agents do not receive Hydra-specific detach requests.

After `--chat-idle-timeout`, a dynamic address stops sending unsolicited periodic announces, including after a bridge restart. The root address remains discoverable. Dormant addresses stay registered and answer Reticulum path requests, so a peer that knows an address can resume the chat. Accepted incoming messages and outgoing replies refresh activity. Older state files recover activity from creation dates, session dates, and retained messages. Legacy message history is removed from the state file on load after recovering activity timestamps. No identity keys, ownership records, or session mappings are automatically deleted; inactive chats still count toward the identity quota. Deleting a chat in an LXMF client does not notify the bridge.

Hydra commands pass through to Hydra as ordinary ACP prompts:

- `/hydra` — show Hydra's available session commands.
- `/hydra kill` — stop the current agent while preserving the conversation for resume.
- `/hydra restart` — restart the agent while preserving history.
- `/hydra compact status` — inspect compaction state.

All local commands require `/bridge`: `/bridge help`, `/bridge new`, `/bridge cancel`, `/bridge tools`, `/bridge tool`, `/bridge allow`, `/bridge revoke`, and `/bridge whitelist` (also `/bridge allowlist`). `/bridge` alone shows help. Bare commands such as `/new`, `/help`, `/cancel`, `/tools`, and `/allow` go upstream to Hydra or the connected ACP agent. Unknown or malformed `/bridge` commands return local help rather than becoming agent prompts.

Use `/bridge forward <text or command>` to explicitly forward text, including an upstream command that itself starts with `/bridge`. Other commands need no escape. Forwarded slash commands remain at the beginning of the ACP prompt even when the LXMF message quotes another message. Prompts, including Hydra commands, are serialized in the conversation queue and run after an active turn and its delivery finish. Hydra lifecycle commands are handled by Hydra; the bridge does not implement competing `/kill` or `/restart` commands.

---

## Tool calls

Tool activity uses ordinary text messages, so it works without client modifications:

```text
Tools:
✓ #1 Read configuration [completed] (0.2s) — Loaded 8 settings
⏳ #2 Run tests [in_progress]
Details: /bridge tool <number>
```

- `/bridge tools` shows the current mode.
- `/bridge tools compact` (default) shows titles, statuses, observed elapsed time, and short result previews.
- `/bridge tools verbose` also includes bounded input and longer output previews.
- `/bridge tools off` disables automatic activity messages; details remain available on request.
- `/bridge tool 2` shows retained input, content (including textual file changes), and raw output for call #2.

The bridge flushes changed calls every 3 seconds, before subsequent agent text, and when a turn ends. Each activity batch contains up to 8 calls. Updates are new messages; existing bubbles are not edited. The last 50 calls and the display mode are saved per conversation in the state file, and display numbers survive restarts. Each input/content/output field is limited to 32 KiB with an explicit truncation marker. At most 256 calls are tracked per turn. Non-text media is represented by a placeholder; terminal references do not contain terminal output unless the agent supplies it separately. Calls without a terminal result when the turn ends are marked `interrupted`, or `cancelled` on cancellation.

Activity messages use the plain-text LXMF renderer. Their `FIELD_CUSTOM_META (0xFD)` contains an `lxmf-acp` namespace for future client rendering:

```text
{
  "lxmf-acp": {
    "version": 1,
    "type": "tool_calls",
    "turn_id": "<unique turn identifier>",
    "sequence": 1,
    "calls": [
      {"number": 1, "tool_call_id": "call_1", "title": "Read configuration",
       "name": "read_file", "kind": "read", "status": "completed", "duration_ms": 200}
    ]
  }
}
```

Sequence numbers increase within a turn. Tool activity carries no `FIELD_CONTINUATION` or multipart completion markers; answer parts continue referencing the previous **answer** part. Existing access control also applies to tool details. Tool commands run in the conversation queue and take effect after an active turn finishes.

---

## Project Structure

```text
lxmf-acp/
├── cmd/
│   └── lxmf-acp/
│       └── main.go       # Entrypoint, CLI flag parsing, execution and graceful shutdown
├── internal/
│   ├── config/           # Configuration and allowlist validation
│   ├── storage/          # Atomic JSON store for chats and known peer identities
│   ├── lxmf/
│   │   ├── client.go     # LXMF client (Transport, Delivery, Announce, Reconnecting TCP)
│   │   └── identity.go   # Reticulum identity loader / generator (64-byte keypair)
│   ├── acp/
│   │   └── client.go     # ACP WebSocket client (v1, Initialize, Prompt, Cancel, Safe Permissions)
│   └── bridge/
│       ├── bridge.go     # Message dispatch between LXMF and ACP
│       ├── sessions.go   # Agent session management (create/load/reset)
│       └── queue.go      # Per-peer queue and command handling (/bridge new, /bridge cancel)
└── go.mod
```

---

## License

MIT
