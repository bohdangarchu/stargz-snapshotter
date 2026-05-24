# Subscription-based auto-refresh

Lets a workload track a moving image tag without an external orchestrator. The daemon polls the registry on its own and dispatches a delta refresh when the tag points at new content.

## Surface

- `ctr-remote watch <ref> [--interval] [--with-background-fetch]` — register.
- `ctr-remote unwatch <ref>` — remove.
- `ctr-remote watch-list` — list active subscriptions.

The watch RPC carries the ref plus the layer-digest list the client currently holds. The daemon stores that list in subscription state; it doesn't need an `image-ref → layers` index of its own.

## Poll loop

Each subscription runs a goroutine on a `time.Ticker` (default 30 s). Per tick:

1. `Resolve` the ref — one HEAD on a well-behaved registry.
2. If the returned top-level digest matches the last one seen, stop. No body fetch.
3. Otherwise pull the manifest body (and walk one index level for multi-platform refs), diff layers against the stored list, and dispatch the same per-layer delta refresh path used by `ctr-remote refresh`.

So an unchanged poll costs one HEAD; a changed poll costs HEAD + manifest GET (+ child manifest GET for index refs).

Subscription state tracks two digests: the top-level resolved digest (drives the short-circuit) and the platform manifest digest (shown by `watch-list`). They diverge for index refs.

## Lifecycle

A subscription ends on:

- explicit `unwatch`,
- layer unmount — any subscription whose layer set references the unmounted layer is dropped,
- 10 consecutive poll failures — dropped with a warning,
- daemon restart — state is in-memory only, so all subscriptions are lost. Clients re-register after restart.

A second `watch` for the same ref replaces the existing subscription.

## Out of scope

- Workload quiesce / coordinating the refresh with in-flight work.
- Persistence across daemon restart.
- Registry push / long-poll (no common registry supports it).
- Cross-image fan-out.
- Reconciling refreshed state back into containerd's image record (same drift as the one-shot path; see `delta-refresh.md`).
