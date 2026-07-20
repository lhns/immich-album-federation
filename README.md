# immich-album-federation

Two-way album mirroring between independent [Immich](https://immich.app) instances.

Immich itself has no federation (the maintainers consider it out of scope), so this tool
runs on the side: it periodically reconciles linked albums across servers over the
regular Immich HTTP API, tracking state in its own database. Photos are identified by
their content checksum (SHA-1, the same identity Immich uses for duplicate detection),
so the same photo uploaded independently on both sides never conflicts.

Targets Immich **v3.x**. Runs on JDK 21+ (Scala 3, direct style with
[Ox](https://github.com/softwaremill/ox), sttp-client4, Postgres via magnum/Flyway).

## Quick start (docker compose)

1. On each instance, create a sync user and API key (two minutes — see
   [Setting up a sync user](#setting-up-a-sync-user-once-per-instance)).
2. Create a `sync.yaml` naming your instances (see `doc/sync.example.yaml`):

   ```yaml
   peers:
     - name: source
       baseUrl: https://immich.example.org
       apiKeyEnv: IMMICH_SOURCE_API_KEY
     - name: target
       baseUrl: https://immich.example.net
       apiKeyEnv: IMMICH_TARGET_API_KEY
   ```

3. Copy `doc/docker-compose.yml` next to it, fill in the API keys and allowed
   hostnames, and `docker compose up -d`.
4. Share an album with the sync user on each instance, wait a cycle, and copy the
   generated `[sync <id>]` token from one album's description into the other's.

The compose file starts in **`DRY_RUN: "true"`** — the logs show exactly what would
happen without writing anything. When the plan looks right, set it to `"false"` and the
service reconciles continuously (`IMMICH_SYNC_INTERVAL`, default in the example: 15m).

## The model: sync users

The tool never uses your personal account. On **each** instance you create a dedicated
user (e.g. `immich-sync`) and the tool logs in with that user's API key. An album
participates in syncing **only if you share it with the sync user** — the share is the
opt-in and the security boundary:

- The tool can only ever see albums you explicitly shared with it.
- Everything the tool uploads is owned by the sync user, so cleanup can never touch
  photos owned by real users.
- Revoking the share (or deleting the sync user) instantly cuts the tool off.

### Setting up a sync user (once per instance)

1. Admin panel → create user `immich-sync`. Give it a storage quota large enough for
   the content you expect to mirror (mirrored uploads land in its account; `0` =
   unlimited).
2. Log in as that user → Account Settings → API Keys → create a key with permissions:
   `album.read`, `album.update`, `albumAsset.create`, `albumAsset.delete`,
   `asset.read`, `asset.upload`, `asset.download`, `asset.delete`, `asset.update`.
3. Put the key into the environment variable your config names for that peer.

### Registering instances

Peers are declared in a YAML file (see `doc/sync.example.yaml`), loaded via
`--config=path` or `IMMICH_SYNC_CONFIG`:

```yaml
peers:
  - name: source
    baseUrl: http://192.168.1.10:2283
    apiKeyEnv: IMMICH_SOURCE_API_KEY
  - name: target
    baseUrl: https://immich.example.org
    apiKeyEnv: IMMICH_TARGET_API_KEY
```

Peers get stable database ids keyed by `name` — changing a `baseUrl` later is safe and
never orphans sync state. API keys stay in environment variables, never in the file.
Base URLs are the one thing that must be configured; everything album-related happens
in the Immich UI.

## Linking albums: `[sync <group>]`

1. Share an album with the instance's sync user (**editor** role).
2. On the next apply run, the tool automatically stamps the album's description with a
   fresh unique group id, e.g. `[sync mfrggzdfmztwq]`.
3. Do the same on the other instance's album — it gets its own id.
4. Copy one album's id into the other album's annotation (edit the description and
   overwrite the token). Albums carrying the **same** group id sync with each other.

That's the whole linking flow — no album UUIDs, no server-side config. It generalizes:
**any number of albums on any number of instances** carrying the same group id form an
n-way sync group. You can also just type a name yourself (`[sync family]` on both) —
hand-picked names and generated ids are the same mechanism.

Options inside the annotation (defaults: `deletes=on`, `direction=both`):

```
[sync family deletes=off direction=push]
```

- `deletes=on|off` — mirror album-membership removals. On by default; **either**
  member saying `off` disables propagation for that pairing.
- `direction=push|pull|both` — relative to the annotating album: `push` = my content
  flows out only, `pull` = I only receive.

Removing the annotation or unsharing the album unlinks it (the pair is disabled, all
history kept; a still-shared album simply gets a fresh unlinked id on the next run).
Re-linking later always restarts with a safe additive baseline.

## How syncing works

Each linked album pair is reconciled with a **baseline 3-way merge**:

1. The last successful apply run's observations are the *baseline* (the state both
   sides agreed on).
2. Each run fetches both albums and diffs each side against the baseline — per-side
   *adds* and *removals*, no clocks or timestamps involved.
3. Adds flow to the other side (bounded by direction).
4. Removals mirror only when `deletes` is active, and only removals detected in the
   same run — they never resurrect later.
5. Conflicts default to **add-wins**: a photo removed on one side but newly added on
   the other survives everywhere.

### What "deletion" means

Removal from the album — never destroying an asset outright. When a removal propagates,
the photo is removed from the mirrored album on the other side; if the tool itself
originally uploaded that copy (tracked per peer in `uploaded_asset`, recorded only when
Immich confirmed a genuinely new upload) and it is not favorited, not archived, and in
no other album, it is additionally moved to the **trash** (reversible). Every
destructive action is recorded permanently in `deletion_log`.

**Sync-user fine print**: Immich only lets an album *owner* remove other users' assets.
The sync user (an editor) can remove the copies it uploaded itself — which is all
cross-instance mirrored content — but not a photo the local owner natively added to
their own album. Such removals are skipped with a `removal_skipped` event and a
warning, and the photo is protected from being copied back to the side that deleted it
(the tombstone stays active).

### Safety rails

- **Preview mode on demand**: `DRY_RUN=true` (or `--dry-run`) logs every planned action
  without writing anything — to Immich or to album descriptions. The compose example
  starts with it enabled.
- **First run after linking is always an additive union** — an empty album linked to a
  full one gets filled, never the other way around.
- **API failures can never look like an empty album**: a failed fetch fails the run and
  records nothing.
- **Mass-removal circuit breaker**: losing more than `IMMICH_SYNC_MAX_REMOVALS`
  (default 25) or `IMMICH_SYNC_MAX_REMOVAL_FRACTION` (default 30%) of a baseline in one
  run quarantines the pair — nothing is applied until you `--rearm=<pair-name>`.
- With `deletes=off`, removals are *suppressed*, not mirrored: the photo stays on the
  other side but is not copied back either (a tombstone remembers the removal until the
  photo is deliberately re-added).
- Host allowlist/blocklist plus a private-network guard on every peer URL.

## Running without docker

Requirements: JDK 21+, [sbt](https://www.scala-sbt.org), Postgres.

```sh
sbt test                                        # full suite (in-memory + H2 integration)
sbt "run --config=./sync.yaml"                  # one full reconciliation
sbt "run --config=./sync.yaml --dry-run"        # preview only, writes nothing
sbt "run --discover"                            # annotation discovery only
sbt "run --rearm=<pair-name>"                   # clear quarantine, re-baseline additively
sbt "run --pair=<pair-name>"                    # sync a single pair
sbt assembly                                    # build a runnable fat jar
```

Without `IMMICH_SYNC_INTERVAL` the tool runs one reconciliation and exits (cron-style);
with it (e.g. `15m`) it loops as a long-running service. Environment variables are
documented in `doc/sync.local.env.example`. Migrations run automatically (Flyway,
bundled in the jar).

## State, audit, and retention

| table | purpose | lifecycle |
|---|---|---|
| `sync_peer` | registered instances (stable ids, mutable base URLs) | permanent |
| `sync_group` / `sync_album` | group tokens (surrogate-keyed) and per-album membership | permanent, one row per album |
| `album_pair` | derived sync pairs, mode/deletes config, quarantine state | permanent (disabled, never deleted) |
| `sync_run` / `sync_event` | one row per reconciliation + event ledger | pruned after `IMMICH_SYNC_AUDIT_RETENTION_DAYS` (default 90, `0`=keep) |
| `asset_observation` | per-run album snapshots (the baseline) | newest `IMMICH_SYNC_OBSERVATION_KEEP_RUNS` runs (default 5); the baseline run is never pruned |
| `tombstone` | removal lifecycle (active = suppressing; resolved = audit) | active rows never deleted; resolved rows pruned with audit retention |
| `uploaded_asset` | provenance of every asset the tool created | **never deleted** (drives trash-eligibility) |
| `deletion_log` | permanent record of every album removal / trash the tool performed | **never deleted** |
