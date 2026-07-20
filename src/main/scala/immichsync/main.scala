package immichsync

import com.augustnagro.magnum.*
import sttp.client4.DefaultSyncBackend

import java.net.{InetAddress, URI}
import scala.util.control.NonFatal

def requiredEnv(name: String): String =
  sys.env.get(name).filter(_.nonEmpty).getOrElse {
    throw new RuntimeException(s"Missing required environment variable: $name")
  }

def envOrDefault(name: String, defaultValue: String): String =
  sys.env.get(name).filter(_.nonEmpty).getOrElse(defaultValue)

def parseBoolEnv(name: String, defaultValue: Boolean): Boolean =
  sys.env.get(name).map(_.trim.toLowerCase).flatMap {
    case "1" | "true" | "yes" | "on"  => Some(true)
    case "0" | "false" | "no" | "off" => Some(false)
    case _                                  => None
  }.getOrElse(defaultValue)

def parseCsvSet(raw: String): Set[String] =
  raw
    .split(',')
    .map(_.trim)
    .filter(_.nonEmpty)
    .map(_.toLowerCase)
    .toSet

def parseArgs(args: Array[String]): CliConfig =
  args.foldLeft(CliConfig(dryRun = false, pairFilter = None, extraAllowedHosts = Set.empty, rearmPair = None, discoverOnly = false, configFile = None)) {
    case (acc, "--dry-run") => acc.copy(dryRun = true)
    case (acc, "--discover") => acc.copy(discoverOnly = true)
    case (acc, arg) if arg.startsWith("--config=") =>
      acc.copy(configFile = Some(arg.stripPrefix("--config=").trim).filter(_.nonEmpty))
    case (acc, arg) if arg.startsWith("--pair=") =>
      acc.copy(pairFilter = Some(arg.stripPrefix("--pair=").trim).filter(_.nonEmpty))
    case (acc, arg) if arg.startsWith("--rearm=") =>
      acc.copy(rearmPair = Some(arg.stripPrefix("--rearm=").trim).filter(_.nonEmpty))
    case (acc, arg) if arg.startsWith("--allow-host=") =>
      val host = arg.stripPrefix("--allow-host=").trim.toLowerCase
      if (host.isEmpty) acc else acc.copy(extraAllowedHosts = acc.extraAllowedHosts + host)
    case (_, arg) =>
      throw new RuntimeException(s"Unknown argument: $arg")
  }

def loadSafetyConfig(cli: CliConfig): SafetyConfig =
  val defaultAllowed = Set("localhost", "127.0.0.1", "::1", "host.docker.internal")
  val fromEnvAllowed = parseCsvSet(envOrDefault("IMMICH_SYNC_ALLOWED_HOSTS", ""))
  val fromEnvBlocked = parseCsvSet(envOrDefault("IMMICH_SYNC_BLOCKED_HOSTS", ""))
  SafetyConfig(
    allowedHosts = defaultAllowed ++ fromEnvAllowed ++ cli.extraAllowedHosts,
    blockedHosts = fromEnvBlocked,
    allowPrivateNetworks = parseBoolEnv("IMMICH_SYNC_ALLOW_PRIVATE_NETWORKS", defaultValue = true),
  )

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

def isPrivateAddress(address: InetAddress): Boolean =
  val host = address.getHostAddress.toLowerCase
  address.isAnyLocalAddress ||
  address.isLoopbackAddress ||
  address.isSiteLocalAddress ||
  address.isLinkLocalAddress ||
  host.startsWith("fc") ||
  host.startsWith("fd")

def assertSafeHost(baseUrl: String, safety: SafetyConfig): Unit =
  val host = Option(URI.create(baseUrl).getHost)
    .map(_.trim.toLowerCase)
    .getOrElse(throw new RuntimeException(s"Invalid base URL, missing host: $baseUrl"))
  if (safety.blockedHosts.contains(host)) {
    throw new RuntimeException(s"Blocked host '$host' is not allowed for sync execution")
  }
  if (safety.allowedHosts.contains(host)) {
    ()
  } else {
    val addresses = InetAddress.getAllByName(host).toVector
    val allPrivate = addresses.nonEmpty && addresses.forall(isPrivateAddress)
    if (!(safety.allowPrivateNetworks && allPrivate)) {
      throw new RuntimeException(
        s"Host '$host' is not in allowlist and does not resolve to private/local addresses"
      )
    }
  }

def checkPeerVersions(
    peers: Seq[SyncPeer],
    api: ImmichApi,
    safety: SafetyConfig,
    resolveApiKey: String => String,
): Unit =
  val versions = peers.flatMap { peer =>
    try {
      assertSafeHost(peer.baseUrl, safety)
      val version = api.serverVersion(ImmichServer(peer.baseUrl, resolveApiKey(peer.apiKeyEnv)))
      println(s"[peer=${peer.name}] immich $version")
      Some(peer.name -> version)
    } catch {
      case NonFatal(e) =>
        println(s"[peer=${peer.name}] version check failed: ${Option(e.getMessage).getOrElse(e.toString)}")
        None
    }
  }
  val majors = versions.map(_._2.dropWhile(!_.isDigit).takeWhile(_.isDigit)).distinct
  if (majors.size > 1) {
    println(
      s"[warn] peers run different Immich major versions (${versions.map((n, v) => s"$n=$v").mkString(", ")}); " +
        "DTO drift between majors can break mirroring"
    )
  }

@main
def main(args: String*): Unit =
  val cli = parseArgs(args.toArray)
  val safety = loadSafetyConfig(cli)
  val thresholds = loadThresholds()
  val retention = loadRetentionConfig()
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

    cli.configFile.orElse(sys.env.get("IMMICH_SYNC_CONFIG").filter(_.nonEmpty)).foreach { path =>
      applySyncConfig(db, loadSyncConfigFile(path))
    }

    def syncCycle(): Unit =
      runAnnotationDiscovery(db, api, safety, requiredEnv, applyWrites = applyWrites)

      val (peers, pairs) = connect(db.xa):
        (loadEnabledPeers(), loadEnabledPairs(cli.pairFilter))

      if (cli.discoverOnly) {
        println(s"Discovery complete. ${pairs.size} enabled pair(s).")
      } else if (pairs.isEmpty) {
        println("No enabled album pairs found. Share albums with the sync users and give them matching [sync <group>] annotations.")
      } else {
        checkPeerVersions(peers, api, safety, requiredEnv)

        val peerById = peers.map(peer => peer.id -> peer).toMap
        parMap(pairs, pairConcurrency) { pair =>
          val leftPeer = peerById.getOrElse(
            pair.leftPeerId,
            throw new RuntimeException(s"Pair '${pair.name}' references missing left peer id ${pair.leftPeerId}")
          )
          val rightPeer = peerById.getOrElse(
            pair.rightPeerId,
            throw new RuntimeException(s"Pair '${pair.name}' references missing right peer id ${pair.rightPeerId}")
          )

          try {
            executePairSync(
              db = db,
              api = api,
              pair = pair,
              leftPeer = leftPeer,
              rightPeer = rightPeer,
              safety = safety,
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
        // and deletion_log are never touched.
        val (prunedRuns, prunedTombstones) = transact(db.xa):
          pruneAuditData(retention.auditRetentionDays)
        if (prunedRuns > 0 || prunedTombstones > 0) {
          println(s"[maintenance] pruned $prunedRuns old runs and $prunedTombstones resolved tombstones")
        }
      }

    cli.rearmPair match {
      case Some(name) =>
        val (rearmedPairs, clearedTombstones) = transact(db.xa):
          (rearmPair(name), rearmTombstones(name))
        if (rearmedPairs == 0) {
          println(s"No album pair named '$name' found.")
        } else {
          println(s"Re-armed pair '$name': quarantine cleared, next apply run is additive-only ($clearedTombstones tombstones cleared).")
        }

      case None =>
        if (!applyWrites) println("[mode] DRY RUN: nothing will be written to any Immich instance or album description")
        intervalSeconds match {
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
