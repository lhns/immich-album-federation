package immichsync

// Declarative configuration file (YAML) for peers and optional manual pairs.
//
// The database stays the source of truth for runtime state (runs, baselines,
// tombstones, deletion history); the config file declares the topology and is
// upserted into sync_peer / album_pair at startup. Peers are keyed by name, so
// a changed base_url updates the existing peer row and its stable id — hostname
// changes never orphan sync state. API keys are never stored in the file; each
// peer names the environment variable that holds its DEDICATED SYNC USER's key.
//
//   peers:
//     - name: source
//       baseUrl: http://192.168.1.10:2283
//       apiKeyEnv: IMMICH_SOURCE_API_KEY
//     - name: target
//       baseUrl: https://immich.example.org
//       apiKeyEnv: IMMICH_TARGET_API_KEY
//
//   pairs:                       # optional; [sync <group>] annotations are the primary UX
//     - name: family
//       left: { peer: source, album: aaaaaaaa-1111-2222-3333-444444444444 }
//       right: { peer: target, album: bbbbbbbb-1111-2222-3333-444444444444 }
//       mode: bidirectional      # bidirectional | left_to_right | right_to_left
//       deletes: true            # default true

import com.augustnagro.magnum.*
import org.virtuslab.yaml.*

import java.nio.file.{Files, Path}

case class PeerConfig(
  name: String,
  baseUrl: String,
  apiKeyEnv: String,
  enabled: Option[Boolean] = None,
  maxRemovalCount: Option[Int] = None,
  maxRemovalFraction: Option[Double] = None,
) derives YamlCodec

case class PairEndpointConfig(peer: String, album: String) derives YamlCodec

case class PairConfig(
  name: String,
  left: PairEndpointConfig,
  right: PairEndpointConfig,
  mode: Option[String] = None,
  deletes: Option[Boolean] = None,
  trashOrphans: Option[Boolean] = None,
  maxRemovalCount: Option[Int] = None,
  maxRemovalFraction: Option[Double] = None,
) derives YamlCodec

case class SyncConfig(
  peers: List[PeerConfig],
  pairs: Option[List[PairConfig]] = None,
) derives YamlCodec {
  def allPairs: List[PairConfig] = pairs.getOrElse(List.empty)
}

def parseSyncConfig(content: String): Either[String, SyncConfig] =
  content.as[SyncConfig].left.map(_.msg)

def validateSyncConfig(config: SyncConfig): Seq[String] =
  val errors = Seq.newBuilder[String]
  val validModes = Set("bidirectional", "left_to_right", "right_to_left")

  val duplicatePeerNames = config.peers.groupBy(_.name.toLowerCase).filter(_._2.size > 1).keys
  duplicatePeerNames.foreach(n => errors += s"duplicate peer name '$n'")
  config.peers.filter(p => p.name.isEmpty || p.baseUrl.isEmpty || p.apiKeyEnv.isEmpty)
    .foreach(p => errors += s"peer '${p.name}' has empty name, baseUrl or apiKeyEnv")

  config.peers.foreach { peer =>
    peer.maxRemovalFraction.foreach { f =>
      if (f <= 0 || f > 1) errors += s"peer '${peer.name}' has invalid maxRemovalFraction $f"
    }
    peer.maxRemovalCount.foreach { c =>
      if (c < 0) errors += s"peer '${peer.name}' has invalid maxRemovalCount $c"
    }
  }

  val peerNames = config.peers.map(_.name.toLowerCase).toSet
  val duplicatePairNames = config.allPairs.groupBy(_.name).filter(_._2.size > 1).keys
  duplicatePairNames.foreach(n => errors += s"duplicate pair name '$n'")
  config.allPairs.foreach { pair =>
    if (pair.name.trim.isEmpty) errors += "a pair has an empty name"
    Seq(pair.left, pair.right).foreach { endpoint =>
      if (!peerNames.contains(endpoint.peer.toLowerCase))
        errors += s"pair '${pair.name}' references unknown peer '${endpoint.peer}'"
    }
    if (pair.left.peer.equalsIgnoreCase(pair.right.peer) && pair.left.album == pair.right.album)
      errors += s"pair '${pair.name}' links an album to itself"
    pair.mode.foreach { m =>
      if (!validModes.contains(m)) errors += s"pair '${pair.name}' has invalid mode '$m'"
    }
    pair.maxRemovalFraction.foreach { f =>
      if (f <= 0 || f > 1) errors += s"pair '${pair.name}' has invalid maxRemovalFraction $f"
    }
    pair.maxRemovalCount.foreach { c =>
      if (c < 0) errors += s"pair '${pair.name}' has invalid maxRemovalCount $c"
    }
  }
  errors.result()

def loadSyncConfigFile(path: String): SyncConfig =
  val file = Path.of(path)
  if (!Files.isRegularFile(file))
    throw new RuntimeException(s"Config file not found: $path")
  val parsed = parseSyncConfig(Files.readString(file)) match {
    case Left(error)   => throw new RuntimeException(s"Failed to parse $path: $error")
    case Right(config) => config
  }
  val errors = validateSyncConfig(parsed)
  if (errors.nonEmpty)
    throw new RuntimeException(s"Invalid config $path:\n${errors.map("  - " + _).mkString("\n")}")
  parsed

// ---------------------------------------------------------------------------
// DB application
// ---------------------------------------------------------------------------

// Update-then-insert instead of ON CONFLICT DO UPDATE: H2 (PG mode) supports only
// DO NOTHING, and the tool is single-writer so the pattern is race-free in practice.
// Name matching is case-insensitive (consistent with peersByName lookups): changing
// the case in the config updates the same row instead of forking a second peer.
def upsertPeer(peer: PeerConfig)(using DbTx): Unit =
  val enabled = peer.enabled.getOrElse(true)
  val updated =
    sql"""
        UPDATE sync_peer
        SET name = ${peer.name},
            base_url = ${peer.baseUrl},
            api_key_env = ${peer.apiKeyEnv},
            enabled = $enabled,
            max_removal_count = ${peer.maxRemovalCount},
            max_removal_fraction = ${peer.maxRemovalFraction}
        WHERE LOWER(name) = LOWER(${peer.name})
      """.update.run()
  if (updated == 0) {
    sql"""
        INSERT INTO sync_peer(name, base_url, api_key_env, enabled, max_removal_count, max_removal_fraction)
        VALUES (${peer.name}, ${peer.baseUrl}, ${peer.apiKeyEnv}, $enabled, ${peer.maxRemovalCount}, ${peer.maxRemovalFraction})
      """.update.run()
  }

def selectPairByName(name: String)(using DbCon): Option[AlbumPair] =
  sql"""
      SELECT id, name, left_peer_id, left_album_id, right_peer_id, right_album_id, mode, propagate_deletes, enabled, trash_orphaned_assets, force_additive, max_removal_count, max_removal_fraction, link_source
      FROM album_pair
      WHERE name = $name
    """.query[PairRow].run().headOption.map(pairFromRow)

def insertManualPair(
    name: String,
    leftPeerId: Long,
    leftAlbumId: String,
    rightPeerId: Long,
    rightAlbumId: String,
    pair: PairConfig,
)(using DbTx): Unit =
  sql"""
      INSERT INTO album_pair(
        name, left_peer_id, left_album_id, right_peer_id, right_album_id,
        mode, propagate_deletes, enabled, trash_orphaned_assets,
        max_removal_count, max_removal_fraction, force_additive, link_source
      )
      VALUES (
        $name, $leftPeerId, $leftAlbumId, $rightPeerId, $rightAlbumId,
        ${pair.mode.getOrElse("bidirectional")}, ${pair.deletes.getOrElse(true)}, TRUE, ${pair.trashOrphans.getOrElse(true)},
        ${pair.maxRemovalCount}, ${pair.maxRemovalFraction}, TRUE, 'manual'
      )
    """.update.run()

def updateManualPair(
    pairId: Long,
    leftPeerId: Long,
    leftAlbumId: String,
    rightPeerId: Long,
    rightAlbumId: String,
    pair: PairConfig,
    forceAdditive: Boolean,
)(using DbTx): Unit =
  sql"""
      UPDATE album_pair
      SET left_peer_id = $leftPeerId,
          left_album_id = $leftAlbumId,
          right_peer_id = $rightPeerId,
          right_album_id = $rightAlbumId,
          mode = ${pair.mode.getOrElse("bidirectional")},
          propagate_deletes = ${pair.deletes.getOrElse(true)},
          trash_orphaned_assets = ${pair.trashOrphans.getOrElse(true)},
          max_removal_count = ${pair.maxRemovalCount},
          max_removal_fraction = ${pair.maxRemovalFraction},
          enabled = TRUE,
          force_additive = CASE WHEN $forceAdditive THEN TRUE ELSE force_additive END,
          updated_at = now()
      WHERE id = $pairId AND link_source = 'manual'
    """.update.run()

def applySyncConfig(db: DbRuntime, config: SyncConfig): Unit =
  transact(db.xa):
    config.peers.foreach(upsertPeer)

  val peersByName = connect(db.xa)(loadAllPeers()).map(p => p.name.toLowerCase -> p).toMap

  config.allPairs.foreach { pairConfig =>
    val leftPeer = peersByName(pairConfig.left.peer.toLowerCase)
    val rightPeer = peersByName(pairConfig.right.peer.toLowerCase)
    val existing = connect(db.xa)(selectPairByName(pairConfig.name))
    existing match {
      case None =>
        transact(db.xa):
          insertManualPair(pairConfig.name, leftPeer.id, pairConfig.left.album, rightPeer.id, pairConfig.right.album, pairConfig)
        println(s"[config] created pair '${pairConfig.name}'")
      case Some(pair) if pair.linkSource == "annotation" =>
        println(s"[config] pair name '${pairConfig.name}' collides with an annotation-managed pair, skipping")
      case Some(pair) =>
        val endpointsChanged =
          pair.leftPeerId != leftPeer.id || pair.leftAlbumId != pairConfig.left.album ||
            pair.rightPeerId != rightPeer.id || pair.rightAlbumId != pairConfig.right.album
        if (endpointsChanged)
          println(s"[config] pair '${pairConfig.name}' endpoints changed, next apply run is additive-only")
        transact(db.xa):
          // Re-pointing a pair at different albums invalidates its baseline: force additive.
          updateManualPair(pair.id, leftPeer.id, pairConfig.left.album, rightPeer.id, pairConfig.right.album, pairConfig, forceAdditive = endpointsChanged)
    }
  }
