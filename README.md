# heartbeat-hermes

Thin Hermes adapter for the house heartbeat Core.

The plugin owns registration and trusted binding only. One `heartbeat-core`
child process over stdio owns scheduling, durable policy (dedupe, DND,
budgets, retry, dead-letter) and the agent tool semantics. The connector
never composes chat text: a wake is one internal synthetic event carrying a
bounded `<<<BEGIN HEARTBEAT DATA>>>` envelope, and the agent's own reply
leaves through the normal egress path.

## How it works

```
1. The plugin registers at gateway startup — no inbound message needed.
   A bounded waiter polls the pinned private runner seam until the runner,
   adapters and event loop are ready; exhaustion fails visibly.
2. The Core child offers a frozen envelope when the agent is idle.
3. The adapter resolves the current session for the bound route, builds an
   internal MessageEvent (synthetic author, allow_gateway_control=False,
   strict session fence) and hands it to the captured runner.
4. The gateway's delivery receipt — not a None return — decides acceptance:
   admitted / rejected / uncertain are recorded in Core as receipts.
5. pre_llm_call correlates the envelope at the model-input boundary; the
   wrapped egress of the bound route records the outbound message id.
```

## Configuration

Under `plugins.entries.heartbeat-hermes` (or the matching env vars):

| Key | Meaning |
|---|---|
| `core_binary` / `HEARTBEAT_CORE_BINARY` | Path to the pinned heartbeat-core binary. |
| `core_config` / `HEARTBEAT_CORE_CONFIG` | Path to the generated Core config (owner database, limits, policy). |
| `deliver_platform`, `deliver_chat_id`, `deliver_chat_type`, `deliver_thread_id` (or `HEARTBEAT_DELIVER_*`) | Explicit Gate 0 route. Without them the first routed conversation binds the route. |
| `startup_timeout_seconds` | Bounded readiness wait (default 120 s). |

Without `core_binary`/`core_config` the adapter stays inactive with a
visible warning; nothing schedules silently.

## Migration

`heartbeat_hermes.migrate` translates an owner-produced export of the
retired plugin's `watches.json` into Core tool calls. It is dry-run by
default, refuses the whole import on any malformed entry, never edits the
original file, and is tested on synthetic data only. The live cutover keeps
one active emitter and is a separate, approved step.

## Tools

The agent tool surface is exactly Core's `tool_schema`
(`heartbeat_watch`, `heartbeat_unwatch`, `heartbeat_list`,
`heartbeat_journal`, `heartbeat_dnd`, `heartbeat_status`, `heartbeat_ack`).
The connector registers and forwards; nothing about a call is interpreted
locally.
