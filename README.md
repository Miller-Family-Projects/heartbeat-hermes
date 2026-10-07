# heartbeat-hermes

Generic wake-on-done watcher plugin for Hermes Agent.

Think of it as an egg timer for anything: start a task, get woken when it's
done. A watch can be a one-shot timer ("boil the egg for 8 minutes") or a
recurring check command (a session going busy→idle, a PID exiting, a build
finishing, a file appearing). When a watch fires, the agent is woken in its
main session through the gateway's own synthetic-event pipeline and decides
itself what to do — the plugin never posts to any chat.

## How it works

```
1. A watch fires (timer deadline reached, or check command prints a finding)
2. The plugin builds a synthetic internal MessageEvent with the finding
3. The event enters the gateway's normal dispatch, into the agent's session
4. The agent wakes, investigates with its own tools, answers if it matters
```

No deliveries, no bot messages, no chat noise from the system. The only
thing that ever appears in a conversation is the agent's own voice.

## Watches

| Type    | Behavior |
|---------|----------|
| `timer`   | One-shot. Fires once `seconds` have elapsed, then is removed. |
| `command` | Recurring. Runs a shell check command every `interval` seconds. Empty stdout = nothing; non-empty stdout = the finding. With `once: true`, removed after the first fire. |

The check command owns the "what does done mean" logic entirely: state
files, transitions, thresholds. The plugin only wakes.

## Tools

### `heartbeat_watch`

Create or replace a watch.

```
# One-shot timer
heartbeat_watch(name="tea", seconds=240, note="Tea is ready")

# Recurring check command (fires when the command prints something)
heartbeat_watch(
    name="build",
    command="test -f /tmp/build.done && echo 'build finished'",
    interval=30,
    once=true,
)
```

### `heartbeat_unwatch`

Remove a watch by name.

### `heartbeat_list`

List all watches with type, interval, and state.

## State

Watches live in `$HERMES_HOME/heartbeat/watches.json` and survive restarts.
The scheduler is guarded by a cross-process file lock, so only one process
runs it even if multiple Hermes surfaces load the plugin.

The Hermes CLI starts plugin discovery in the background before importing
`gateway.run`. Registration can therefore happen before that module exists,
not just before the gateway publishes its runner or binds its loop. A
`gateway run` invocation starts one lightweight daemon waiter even in that
early discovery order; an already-loaded gateway module also enables waiting
for embedded gateway startup. The waiter checks only already-loaded modules,
so it never imports gateway machinery from the plugin loader. It checks
readiness at the scheduler's existing
5-second poll interval and claims the scheduler lock once the runner has a
running loop, without waiting for an inbound message. An already-ready gateway
claims immediately. Repeated registrations reuse the same waiter, scheduler,
and lock. Non-starting CLI processes without `gateway.run` loaded (such as
dashboard, gateway status, and plugin management) never start the waiter.
An incidental gateway import alone cannot claim the scheduler lock: its
runner must exist and have a running loop.

Set `plugins.entries.heartbeat-hermes.startup_timeout_seconds` in `config.yaml`
to a positive, finite number of seconds (default: `120`). The waiter stops at
that deadline, logging one warning; repeated registration does not restart the
wait. Invalid values use the default with a warning. The existing
`pre_gateway_dispatch` capture path still attempts to claim the scheduler,
including after startup timeout or lock contention.
Delivery prefers the runner captured by `pre_gateway_dispatch`, with a compatibility
fallback to Hermes' private `gateway.run._gateway_runner_ref` and the runner's
`_gateway_loop`. An explicit delivery route is needed before the first inbound
message; otherwise the first captured conversation remains the target. Findings
are kept pending while the runner or route is unavailable and retried on the next
tick, with one warning per unavailable episode; delivery still uses the same
synthetic internal event through `runner._handle_message()`.

## Install

Copy the `heartbeat_hermes/` directory into `$HERMES_HOME/plugins/heartbeat-hermes/`
and add `heartbeat-hermes` to `plugins.enabled` in the Hermes config.

The plugin uses only the Python standard library at runtime.

## Validate

```bash
uv run pytest
```

## License

MIT. See `LICENSE`.
