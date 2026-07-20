package immichsync

import com.augustnagro.magnum.*
import sttp.client4.DefaultSyncBackend

import java.net.URI
import scala.util.control.NonFatal

def requiredEnv(name: String): String =
  sys.env.get(name).filter(_.nonEmpty).getOrElse {
    throw new RuntimeException(s"Missing required environment variable: $name")
  }

def envOrDefault(name: String, defaultValue: String): String =
  sys.env.get(name).filter(_.nonEmpty).getOrElse(defaultValue)

def parseBoolValue(raw: String): Option[Boolean] =
  raw.trim.toLowerCase match {
    case "1" | "true" | "yes" | "on"  => Some(true)
    case "0" | "false" | "no" | "off" => Some(false)
    case _                            => None
  }

// A typo in a safety-relevant flag (DRY_RUN=ture) must fail loudly, never silently
// fall back to the default.
def parseBoolEnv(name: String, defaultValue: Boolean): Boolean =
  sys.env.get(name).filter(_.trim.nonEmpty) match {
    case None => defaultValue
    case Some(raw) =>
      parseBoolValue(raw).getOrElse(
        throw new RuntimeException(s"Invalid boolean value '$raw' for $name (use true/false/yes/no/on/off/1/0)")
      )
  }

def parseCsvSet(raw: String): Set[String] =
  raw
    .split(',')
    .map(_.trim)
    .filter(_.nonEmpty)
    .map(_.toLowerCase)
    .toSet

def parseArgs(args: Array[String]): CliConfig =
  args.foldLeft(CliConfig(dryRun = false, pairFilter = None, rearmPairs = List.empty, discoverOnly = false, configFile = None)) {
    case (acc, "--dry-run") => acc.copy(dryRun = true)
    case (acc, "--discover") => acc.copy(discoverOnly = true)
    case (acc, arg) if arg.startsWith("--config=") =>
      acc.copy(configFile = Some(arg.stripPrefix("--config=").trim).filter(_.nonEmpty))
    case (acc, arg) if arg.startsWith("--pair=") =>
      acc.copy(pairFilter = Some(arg.stripPrefix("--pair=").trim).filter(_.nonEmpty))
    case (acc, arg) if arg.startsWith("--rearm=") =>
      val name = arg.stripPrefix("--rearm=").trim
      if (name.isEmpty) acc else acc.copy(rearmPairs = acc.rearmPairs :+ name)
    case (_, arg) =>
      throw new RuntimeException(s"Unknown argument: $arg")
  }

def loadThresholds(): Thresholds =
  Thresholds(
    maxRemovalCount = envOrDefault("IMMICH_SYNC_MAX_REMOVALS", Thresholds.Default.maxRemovalCount.toString).toInt,
    maxRemovalFraction = envOrDefault("IMMICH_SYNC_MAX_REMOVAL_FRACTION", Thresholds.Default.maxRemovalFraction.toString).toDouble,
  )

def loadRetentionConfig(): RetentionConfig =
  RetentionConfig(
    observationKeepRuns = envOrDefault("IMMICH_SYNC_OBSERVATION_KEEP_RUNS", RetentionConfig.Default.observationKeepRuns.toString).toInt,
    auditRetentionDays = envOrDefault("IMMICH_SYNC_AUDIT_RETENTION_DAYS", RetentionConfig.Default.auditRetentionDays.toString).toInt,
  )

def loadCleanupConfig(): CleanupConfig =
  CleanupConfig(
    afterDays = envOrDefault("IMMICH_SYNC_CLEANUP_AFTER_DAYS", CleanupConfig.Default.afterDays.toString).toInt,
    maxPerPass = envOrDefault("IMMICH_SYNC_CLEANUP_MAX", CleanupConfig.Default.maxPerPass.toString).toInt,
  )

// "30s" / "15m" / "1h" (whitespace between number and unit is fine), or a plain
// number of seconds. Empty/absent = run once and exit.
def parseIntervalSeconds(raw: String): Option[Long] =
  val trimmed = raw.trim.toLowerCase
  if (trimmed.isEmpty) None
  else {
    val (digits, unitRaw) = trimmed.span(_.isDigit)
    val unit = unitRaw.trim
    val factor = unit match {
      case "" | "s" => 1L
      case "m"      => 60L
      case "h"      => 3600L
      case other    => throw new RuntimeException(s"Invalid IMMICH_SYNC_INTERVAL '$raw' (use e.g. 30s, 15m, 1h)")
    }
    if (digits.isEmpty) throw new RuntimeException(s"Invalid IMMICH_SYNC_INTERVAL '$raw' (use e.g. 30s, 15m, 1h)")
    Some(digits.toLong * factor)
  }

// Peer URLs come exclusively from the operator-controlled config (file/env): anyone
// who can edit those already holds the API keys, so no host allow/deny machinery.
def validateBaseUrl(baseUrl: String): Unit =
  if (Option(URI.create(baseUrl).getHost).forall(_.isBlank))
    throw new RuntimeException(s"Invalid base URL, missing host: $baseUrl")

def checkPeerVersions(
    peers: Seq[SyncPeer],
    api: ImmichApi,
    resolveApiKey: SyncPeer => String,
): Unit =
  val versions = peers.flatMap { peer =>
    try {
      val version = api.serverVersion(ImmichServer(peer.baseUrl, resolveApiKey(peer)))
      println(s"[peer=${peer.name}] immich $version")
      Some(peer.name -> version)
    } catch {
      case NonFatal(e) =>
        System.err.println(s"[peer=${peer.name}] version check failed: ${Option(e.getMessage).getOrElse(e.toString)}")
        None
    }
  }
  val majors = versions.map(_._2.dropWhile(!_.isDigit).takeWhile(_.isDigit)).distinct
  if (majors.size > 1) {
    System.err.println(
      s"[warn] peers run different Immich major versions (${versions.map((n, v) => s"$n=$v").mkString(", ")}); " +
        "DTO drift between majors can break mirroring"
    )
  }

@main
def main(args: String*): Unit =
  val cli = parseArgs(args.toArray)
  val thresholds = loadThresholds()
  val retention = loadRetentionConfig()
  val cleanupConfig = loadCleanupConfig()
  val pairConcurrency = envOrDefault("IMMICH_SYNC_PAIR_CONCURRENCY", "2").toInt
  val transferConcurrency = envOrDefault("IMMICH_SYNC_TRANSFER_CONCURRENCY", "3").toInt
  // The tool applies by default; DRY_RUN=true (or --dry-run) previews without writing.
  // The safety rails (additive first run, circuit breaker, trash-only, deletion log)
  // are what make applying safe, not a flag.
  val applyWrites = !(cli.dryRun || parseBoolEnv("DRY_RUN", defaultValue = false))
  val intervalSeconds = sys.env.get("IMMICH_SYNC_INTERVAL").flatMap(parseIntervalSeconds)

  val backend = DefaultSyncBackend()
  val api = LiveImmichApi(backend)
  val db = createDbRuntimeFromEnv()
  try {
    runMigrations(db)
    val abandoned = transact(db.xa)(markAbandonedRuns())
    if (abandoned > 0) println(s"[startup] marked $abandoned abandoned run(s) from a previous process as aborted")

    // The config file is the only source of peers and their API keys. Keys stay in
    // memory for this process; the database never sees them.
    val syncConfig = cli.configFile.orElse(sys.env.get("IMMICH_SYNC_CONFIG").filter(_.nonEmpty)).map { path =>
      val config = loadSyncConfigFile(path)
      config.peers.foreach(p => validateBaseUrl(p.baseUrl))
      applySyncConfig(db, config)
      config
    }
    val apiKeyByPeerName: Map[String, String] =
      syncConfig.map(_.peers.map(p => p.name.toLowerCase -> p.apiKey).toMap).getOrElse(Map.empty)
    def resolveApiKey(peer: SyncPeer): String =
      apiKeyByPeerName.getOrElse(
        peer.name.toLowerCase,
        throw new RuntimeException(s"No API key for peer '${peer.name}': add the peer to the config file (--config / IMMICH_SYNC_CONFIG)"),
      )

    // Docker-friendly re-arm: when the circuit breaker quarantines a pair, it logs a
    // one-shot rearm key. Pasting that key into IMMICH_SYNC_REARM re-arms exactly that
    // incident; the key is consumed on use and a new trip generates a new key, so a
    // leftover variable is harmless. Dry runs only report what would happen.
    sys.env.get("IMMICH_SYNC_REARM").map(_.trim).filter(_.nonEmpty).foreach { value =>
      parseCsvSet(value).toSeq.sorted.foreach { key =>
        if (!applyWrites) {
          println(s"[rearm] dry run: key '$key' left untouched")
        } else {
          transact(db.xa)(rearmByKey(key)) match {
            case Some(name) => println(s"[rearm] re-armed pair '$name', next apply run is additive-only")
            case None       => println(s"[rearm] key '$key' is unknown or already used")
          }
        }
      }
    }

    // Always list outstanding quarantines with their rearm keys at startup.
    connect(db.xa)(selectQuarantinedPairs()).foreach { (name, reason, key) =>
      val hint = key.map(k => s"set IMMICH_SYNC_REARM=$k and restart").getOrElse(s"run --rearm=$name")
      println(s"[rearm] pair '$name' is quarantined (${reason.getOrElse("unknown reason")}). To re-arm: $hint")
    }

    def syncCycle(): Unit =
      runAnnotationDiscovery(db, api, resolveApiKey, applyWrites = applyWrites)

      val (peers, pairs) = connect(db.xa):
        (loadEnabledPeers(), loadEnabledPairs(cli.pairFilter))

      if (cli.discoverOnly) {
        println(s"Discovery complete. ${pairs.size} enabled pair(s).")
      } else if (pairs.isEmpty) {
        println("No enabled album pairs found. Share albums with the sync users and give them matching [sync <group>] annotations.")
      } else {
        checkPeerVersions(peers, api, resolveApiKey)

        val peerById = peers.map(peer => peer.id -> peer).toMap
        // Pairs whose peer is disabled (enabled: false in the config = server-level
        // pause) are skipped, not disabled: they resume untouched when re-enabled.
        val (runnable, skipped) = pairs.partition(p => peerById.contains(p.leftPeerId) && peerById.contains(p.rightPeerId))
        skipped.foreach(p => println(s"[pair=${p.name}] skipped: peer disabled or missing"))
        // Pairs sharing an album endpoint (same-instance groups) serialize on it;
        // disjoint pairs run in parallel. The same pair can never overlap with itself:
        // cycles are sequential (the interval sleep starts after all pairs joined).
        val endpointLocks = new EndpointLocks
        parMap(runnable, pairConcurrency) { pair =>
          val leftPeer = peerById(pair.leftPeerId)
          val rightPeer = peerById(pair.rightPeerId)

          try endpointLocks.withLocks(Seq(pair.leftPeerId -> pair.leftAlbumId, pair.rightPeerId -> pair.rightAlbumId)) {
            executePairSync(
              db = db,
              api = api,
              pair = pair,
              leftPeer = leftPeer,
              rightPeer = rightPeer,
              resolveApiKey = resolveApiKey,
              applyWrites = applyWrites,
              thresholds = thresholds,
              transferConcurrency = transferConcurrency,
              retention = retention,
            )
            println(s"[pair=${pair.name}] completed")
          } catch {
            case NonFatal(e) =>
              println(s"[pair=${pair.name}] failed: ${Option(e.getMessage).getOrElse(e.toString)}")
          }
        }

        // Audit retention: prune old runs (cascades events + leftover observations)
        // and resolved tombstones. Baseline runs, active tombstones, uploaded_asset
        // and deletion_log are never touched. Dry runs prune nothing.
        if (applyWrites) {
          val (prunedRuns, prunedTombstones) = transact(db.xa):
            pruneAuditData(retention.auditRetentionDays)
          if (prunedRuns > 0 || prunedTombstones > 0) {
            println(s"[maintenance] pruned $prunedRuns old runs and $prunedTombstones resolved tombstones")
          }
        }
      }

      if (!cli.discoverOnly) {
        runOrphanCleanup(db, api, peers, resolveApiKey, applyWrites, cleanupConfig)
      }

    if (cli.rearmPairs.nonEmpty) {
      cli.rearmPairs.foreach { name =>
        if (!applyWrites) {
          println(s"[rearm] dry run: pair '$name' left untouched")
        } else {
          val (rearmedPairs, clearedTombstones) = transact(db.xa)(rearmPairByName(name))
          if (rearmedPairs == 0) {
            println(s"No album pair named '$name' found.")
          } else {
            println(s"Re-armed pair '$name': quarantine cleared, next apply run is additive-only ($clearedTombstones tombstones cleared).")
          }
        }
      }
    } else {
      if (!applyWrites) println("[mode] DRY RUN: nothing will be written to any Immich instance, album description, or sync state")
      intervalSeconds match {
        case _ if cli.discoverOnly =>
          // Discovery-only is a one-shot inspection command, interval or not.
          syncCycle()
        case None =>
          syncCycle()
        case Some(seconds) =>
          // Service mode (e.g. docker compose): reconcile forever on a fixed interval.
          // A failing cycle is logged and retried on the next tick, never fatal.
          while (true) {
            try syncCycle()
            catch {
              case NonFatal(e) =>
                System.err.println(s"[cycle] failed: ${Option(e.getMessage).getOrElse(e.toString)}")
            }
            println(s"[cycle] next run in ${seconds}s")
            Thread.sleep(seconds * 1000)
          }
      }
    }
  } finally {
    db.dataSource.close()
    backend.close()
  }
