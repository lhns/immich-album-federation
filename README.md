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
2. Create a `sync.yaml` with your instances and their sync users' API keys (see
   `doc/sync.example.yaml`; more instances are just more list entries):

   ```yaml
   peers:
     - name: alpha
       baseUrl: https://immich.example.org
       apiKey: <alpha sync user's api key>
     - name: beta
       baseUrl: https://immich.example.net
       apiKey: <beta sync user's api key>
   ```

3. Put this `docker-compose.yml` next to it (also in `doc/docker-compose.yml`) and
   `docker compose up -d` — everything instance-specific lives in `sync.yaml`:

   ```yaml
   services:
     db:
       image: postgres:17-alpine
       environment:
         POSTGRES_DB: immich_sync
         POSTGRES_USER: immich_sync
         POSTGRES_PASSWORD: immich_sync
       volumes:
         - db-data:/var/lib/postgresql/data
       healthcheck:
         test: ["CMD-SHELL", "pg_isready -U immich_sync"]
         interval: 5s
         timeout: 5s
         retries: 10
       restart: unless-stopped

     sync:
       image: ghcr.io/lhns/immich-album-federation:latest
       depends_on:
         db:
           condition: service_healthy
       environment:
         IMMICH_SYNC_DB_URL: jdbc:postgresql://db:5432/immich_sync
         IMMICH_SYNC_DB_USER: immich_sync
         IMMICH_SYNC_DB_PASSWORD: immich_sync
         IMMICH_SYNC_CONFIG: /config/sync.yaml
         # Reconcile continuously on this interval (30s / 15m / 1h).
         IMMICH_SYNC_INTERVAL: 15m
         # Preview mode: logs what would happen without writing anything.
         # Set to "false" once the plan looks right.
         DRY_RUN: "true"
         # Re-arm after a circuit-breaker quarantine: paste the one-shot rearm key
         # from the logs; it is consumed on use, so leaving it set is harmless.
         #IMMICH_SYNC_REARM: <rearm-key-from-logs>
       volumes:
         - ./sync.yaml:/config/sync.yaml:ro
       restart: unless-stopped

   volumes:
     db-data:
   ```

4. Share an album with the sync user on each instance, wait a cycle, and copy the
   generated `[sync <id>]` token from one album's description into the other's.

The compose file starts in **`DRY_RUN: "true"`** — the logs show exactly what would
happen without writing anything. When the plan looks right, set it to `"false"` and the
service reconciles continuously.

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
   `asset.read`, `asset.upload`, `asset.download`, `asset.delete`, `asset.update`,
   `server.about` (used for the startup version check; without it the check degrades
   to a logged warning).
3. Put the key into that peer's `apiKey` in `sync.yaml`.

### Registering instances

Peers are declared in one YAML file (see `doc/sync.example.yaml`), loaded via
`--config=path` or `IMMICH_SYNC_CONFIG`:

```yaml
peers:
  - name: alpha
    baseUrl: http://192.168.1.10:2283
    apiKey: <alpha sync user's api key>
  - name: beta
    baseUrl: https://immich.example.org
    apiKey: <beta sync user's api key>
```

Peers get stable database ids keyed by `name` — changing a `baseUrl` later is safe and
never orphans sync state. API keys live only in this file and in process memory; they
are never written to the database. The file is therefore the one secret to protect:
keep it out of version control and mount it read-only. This file is the only
instance-specific configuration; everything album-related happens in the Immich UI.

## Linking albums: `[sync <group>]`

1. Share an album with the instance's sync user (**editor** role).
2. On the next apply run, the tool automatically stamps the album's description with a
   fresh unique group id, e.g. `[sync mfrggzdfmztwq]`.
3. Do the same on the other instance's album — it gets its own id.
4. Copy one album's id into the other album's annotation (edit the description and
   overwrite the token). Albums carrying the **same** group id sync with each other.

That's the whole linking flow — no album UUIDs, no server-side config. It generalizes:
**any number of albums on any number of instances** carrying the same group id form an
n-way sync group (pairs sharing an album are automatically serialized, never synced
concurrently). You can also just type a name yourself (`[sync family]` on both) —
hand-picked names and generated ids are the same mechanism.

Note on same-instance groups (two albums on one server): Immich deduplicates content
per *user*, so mirrored photos become copies owned by the sync user — it works, but
budgets the sync user's quota accordingly.

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

### Cleaning up the sync user's library

Deleting or unlinking an album never destroys assets — the tool's uploaded copies stay
in the sync user's library as orphans. The opt-in cleanup pass reclaims them: set
`cleanupOrphans: true` on a peer in `sync.yaml` and each cycle trashes assets that pass
**every** guard:

- uploaded by this tool (`uploaded_asset` provenance) — user content is structurally
  out of scope;
- uploaded more than `IMMICH_SYNC_CLEANUP_AFTER_DAYS` ago (default 1 — deliberately
  short: cleanup should act while you're watching it, not surprise you weeks after
  enabling the flag);
- currently in **no** album (checked against a live scan of all visible albums and
  per-asset), not favorited, not archived, not already trashed;
- not the motion video of any album-resident live photo;
- the peer has no quarantined pair (frozen state stays frozen).

Trash only, never hard delete; every action is recorded in `deletion_log` as
`cleanup_trash`; `DRY_RUN=true` previews the pass. Extra net: if a still-wanted asset
is ever trashed, the next sync cycle finds its checksum in the trash and restores it
automatically. `IMMICH_SYNC_CLEANUP_MAX` caps a single pass (default 0 = unlimited,
oldest first).

### Safety rails

- **Preview mode on demand**: `DRY_RUN=true` (or `--dry-run`) logs every planned action
  without writing anything — to Immich or to album descriptions. The compose example
  starts with it enabled.
- **First run after linking is always an additive union** — an empty album linked to a
  full one gets filled, never the other way around.
- **API failures can never look like an empty album**: a failed fetch fails the run and
  records nothing.
- **Mass-removal circuit breaker**: losing more than the allowed number or fraction of
  a baseline in one run quarantines the pair — nothing is applied until it is re-armed.
  Thresholds resolve per side: pair override → per-server override (`maxRemovalCount`
  / `maxRemovalFraction` on the peer in `sync.yaml`) → global env defaults
  (`IMMICH_SYNC_MAX_REMOVALS` 25, `IMMICH_SYNC_MAX_REMOVAL_FRACTION` 0.3).
  When it trips, the log prints a one-shot **rearm key** (also listed again on every
  restart). Re-arming in docker: paste the key into `IMMICH_SYNC_REARM` (comma-separate
  several) and `docker compose up -d` — the key is consumed on use and a new trip
  issues a new key, so a leftover variable is harmless. Standalone: `--rearm=<pair-name>`
  (repeatable). A re-armed pair restarts with a safe additive baseline.
- **Server-level pause**: set `enabled: false` on a peer in `sync.yaml` — all its pairs
  are skipped (not unlinked) until you re-enable it.
- With `deletes=off`, removals are *suppressed*, not mirrored: the photo stays on the
  other side but is not copied back either (a tombstone remembers the removal until the
  photo is deliberately re-added).

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
with it (e.g. `15m`) it loops as a long-running service. Run exactly one instance of
the tool per database (single writer); within a run, cycles never overlap and pairs
sharing an album are serialized. The config file is read once
at startup, so changes to `sync.yaml` (new peers, `enabled: false`, thresholds) take
effect on the next restart. Environment variables are documented in
`doc/sync.local.env.example`. Migrations run automatically (Flyway, bundled in the jar).

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
