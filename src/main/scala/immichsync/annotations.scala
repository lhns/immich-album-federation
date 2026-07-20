package immichsync

// Sync-group linking via album descriptions.
//
// Albums participate when they are shared with (or owned by) the instance's dedicated
// sync user — the share is the security boundary. Any album visible to the sync user
// that lacks a sync annotation is automatically stamped with a fresh one:
//
//   [sync mfrggzdfmztwq]
//
// All albums carrying the same group token (across any number of registered instances,
// n-way) sync with each other. Linking is therefore: share both albums with their sync
// users, then copy one album's generated token into the other album's annotation.
// Options (defaults: deletes ON, direction both):
//
//   [sync family deletes=off direction=push]
//
// direction is relative to the annotating album: push = my content flows out only,
// pull = I only receive. A flow between two members is enabled when the sender says
// push|both and the receiver says pull|both. Removal propagation is active unless
// EITHER member says deletes=off.

import com.augustnagro.magnum.*

import java.security.SecureRandom
import scala.collection.mutable

case class SyncAnnotation(
  group: String,
  deletes: Option[Boolean],
  direction: Option[String],
  allowedPeers: Option[Set[String]] = None,
  allowedOwners: Option[Set[String]] = None,
)

case class ParsedAnnotations(annotations: Seq[SyncAnnotation], warnings: Seq[String])

private val SyncAnnotationRe =
  """\[sync\s+([A-Za-z0-9._-]{3,64})((?:\s+[^\s\]=]+=[^\s\]]+)*)\s*\]""".r
private val LegacyMirrorRe = """\[mirror\s[^\]]*\]""".r

def parseSyncAnnotations(description: String): ParsedAnnotations =
  val annotations = mutable.ArrayBuffer.empty[SyncAnnotation]
  val warnings = mutable.ArrayBuffer.empty[String]
  if (LegacyMirrorRe.findFirstIn(description).isDefined) {
    warnings += "legacy [mirror ...] annotation is no longer supported, use [sync <group>]"
  }
  SyncAnnotationRe.findAllMatchIn(description).foreach { m =>
    // Tokens are case-insensitive ([sync Family] links with [sync family]).
    val group = m.group(1).toLowerCase
    val optsRaw = Option(m.group(2)).getOrElse("").trim
    val opts = if (optsRaw.isEmpty) Seq.empty else optsRaw.split("\\s+").toSeq
    var deletes: Option[Boolean] = None
    var direction: Option[String] = None
    var allowedPeers: Option[Set[String]] = None
    var allowedOwners: Option[Set[String]] = None
    var valid = true
    def parseSet(value: String): Option[Set[String]] =
      Some(value.split(',').map(_.trim).filter(_.nonEmpty).toSet).filter(_.nonEmpty)
    opts.foreach { opt =>
      opt.toLowerCase.split("=", 2) match {
        case Array("deletes", "on")                             => deletes = Some(true)
        case Array("deletes", "off")                            => deletes = Some(false)
        case Array("direction", d @ ("push" | "pull" | "both")) => direction = Some(d)
        case Array("peers", value) if parseSet(value).isDefined  => allowedPeers = parseSet(value)
        case Array("owners", value) if parseSet(value).isDefined => allowedOwners = parseSet(value)
        case _ =>
          warnings += s"invalid option '$opt' in sync annotation, annotation ignored"
          valid = false
      }
    }
    if (valid) annotations += SyncAnnotation(group, deletes, direction, allowedPeers, allowedOwners)
  }
  ParsedAnnotations(annotations.toSeq, warnings.toSeq)

private val Base32Alphabet = "abcdefghijklmnopqrstuvwxyz234567"

private[immichsync] def base32(bytes: Array[Byte]): String =
  val sb = new StringBuilder
  var buffer = 0L
  var bits = 0
  bytes.foreach { b =>
    buffer = (buffer << 8) | (b & 0xffL)
    bits += 8
    while (bits >= 5) {
      bits -= 5
      sb.append(Base32Alphabet(((buffer >> bits) & 31).toInt))
    }
  }
  if (bits > 0) sb.append(Base32Alphabet(((buffer << (5 - bits)) & 31).toInt))
  sb.toString

def generateGroupToken(random: SecureRandom = new SecureRandom()): String =
  val bytes = new Array[Byte](8)
  random.nextBytes(bytes)
  base32(bytes)

case class AlbumMembership(
  peerId: Long,
  albumId: String,
  group: Option[String],
  deletes: Option[Boolean],
  direction: Option[String],
  ownerEmail: Option[String] = None,
  allowedPeers: Option[Set[String]] = None,
  allowedOwners: Option[Set[String]] = None,
)

case class AutoAnnotation(peerId: Long, albumId: String, newDescription: String, token: String)

case class DiscoveredLink(
  leftPeerId: Long,
  leftAlbumId: String,
  rightPeerId: Long,
  rightAlbumId: String,
  mode: String,
  propagateDeletes: Boolean,
)

case class DiscoveryPlan(
  autoAnnotations: Seq[AutoAnnotation],
  memberships: Seq[AlbumMembership],
  links: Seq[DiscoveredLink],
  warnings: Seq[String],
)

// Pure: computes annotation stamps, group memberships and pairwise links from the
// scanned album listings. tokenGen is injected for deterministic tests.
def planDiscovery(
    peers: Seq[SyncPeer],
    albumsByPeer: Map[Long, Seq[AlbumSummary]],
    tokenGen: () => String = () => generateGroupToken(),
): DiscoveryPlan =
  val peerNameById = peers.map(p => p.id -> p.name).toMap
  val warnings = mutable.ArrayBuffer.empty[String]
  val autoAnnotations = mutable.ArrayBuffer.empty[AutoAnnotation]
  val memberships = mutable.ArrayBuffer.empty[AlbumMembership]

  albumsByPeer.toSeq.sortBy(_._1).foreach { (peerId, albums) =>
    val peerName = peerNameById.getOrElse(peerId, peerId.toString)
    albums.foreach { album =>
      val parsed = parseSyncAnnotations(album.description)
      parsed.warnings.foreach(w => warnings += s"peer '$peerName' album '${album.albumName}': $w")
      def membershipFor(annotation: SyncAnnotation): AlbumMembership =
        AlbumMembership(
          peerId, album.id, Some(annotation.group), annotation.deletes, annotation.direction,
          ownerEmail = album.ownerEmail,
          allowedPeers = annotation.allowedPeers,
          allowedOwners = annotation.allowedOwners,
        )
      parsed.annotations match {
        case Seq() =>
          // Shared with the sync user but not yet annotated: stamp it with a fresh
          // group token. It becomes a single-member group and links to nothing until
          // someone copies its token to a partner album (or overrides it).
          val token = tokenGen()
          val newDescription =
            if (album.description.isEmpty) s"[sync $token]"
            else s"${album.description}\n[sync $token]"
          autoAnnotations += AutoAnnotation(peerId, album.id, newDescription, token)
          memberships += AlbumMembership(peerId, album.id, None, None, None, ownerEmail = album.ownerEmail)
        case Seq(annotation) =>
          memberships += membershipFor(annotation)
        case multiple =>
          warnings += s"peer '$peerName' album '${album.albumName}': ${multiple.size} sync annotations found, using the first"
          memberships += membershipFor(multiple.head)
      }
    }
  }

  val links = mutable.ArrayBuffer.empty[DiscoveredLink]
  memberships.filter(_.group.isDefined).groupBy(_.group.get).toSeq.sortBy(_._1).foreach { (token, members0) =>
    val members = members0.sortBy(m => (m.peerId, m.albumId)).toSeq
    if (members.size == 1) {
      val m = members.head
      warnings += s"group '$token' has a single member (${peerNameById.getOrElse(m.peerId, m.peerId.toString)}/${m.albumId}), waiting for a partner"
    } else {
      def sends(direction: String) = direction == "push" || direction == "both"
      def receives(direction: String) = direction == "pull" || direction == "both"
      // Join authorization: every restriction on one side must pass for the other.
      // owners= fails closed when the partner's owner identity is unavailable.
      def blockReason(restricted: AlbumMembership, partner: AlbumMembership): Option[String] =
        val partnerPeer = peerNameById.get(partner.peerId).map(_.toLowerCase)
        if (restricted.allowedPeers.exists(allowed => !partnerPeer.exists(allowed.contains)))
          Some(s"${restricted.albumId} does not allow peer '${partnerPeer.getOrElse(partner.peerId.toString)}'")
        else restricted.allowedOwners match {
          case None => None
          case Some(allowed) =>
            partner.ownerEmail match {
              case None => Some(s"${restricted.albumId} requires owners= but the owner of ${partner.albumId} is unknown")
              case Some(email) if !allowed.contains(email.toLowerCase) =>
                Some(s"${restricted.albumId} does not allow owner '$email'")
              case _ => None
            }
        }
      for {
        i <- members.indices
        j <- (i + 1) until members.size
      } {
        val a = members(i)
        val b = members(j)
        val dirA = a.direction.getOrElse("both")
        val dirB = b.direction.getOrElse("both")
        val flowAtoB = sends(dirA) && receives(dirB)
        val flowBtoA = sends(dirB) && receives(dirA)
        val blocked = blockReason(a, b).orElse(blockReason(b, a))
        if (blocked.isDefined) {
          warnings += s"group '$token': ${blocked.get}, not linked"
        } else if (!flowAtoB && !flowBtoA) {
          warnings += s"group '$token': contradictory directions between ${a.albumId} and ${b.albumId} (no enabled flow), not linked"
        } else {
          val mode =
            if (flowAtoB && flowBtoA) "bidirectional"
            else if (flowAtoB) "left_to_right"
            else "right_to_left"
          // Removal propagation is on by default; either member can veto with deletes=off.
          val propagateDeletes = !a.deletes.contains(false) && !b.deletes.contains(false)
          links += DiscoveredLink(a.peerId, a.albumId, b.peerId, b.albumId, mode, propagateDeletes)
        }
      }
    }
  }

  DiscoveryPlan(autoAnnotations.toSeq, memberships.toSeq, links.toSeq, warnings.toSeq)

case class PairUpdate(pairId: Long, mode: String, propagateDeletes: Boolean, reenable: Boolean)

case class PairReconciliation(
  inserts: Seq[DiscoveredLink],
  updates: Seq[PairUpdate],
  disables: Seq[Long],
  warnings: Seq[String],
)

private def flipMode(mode: String): String = mode match {
  case "left_to_right" => "right_to_left"
  case "right_to_left" => "left_to_right"
  case other           => other
}

def reconcilePairs(
    existingPairs: Vector[AlbumPair],
    links: Seq[DiscoveredLink],
    scannedPeerIds: Set[Long],
): PairReconciliation =
  val byEndpoints = existingPairs
    .map(p => (p.leftPeerId, p.leftAlbumId, p.rightPeerId, p.rightAlbumId) -> p)
    .toMap
  val inserts = mutable.ArrayBuffer.empty[DiscoveredLink]
  val updates = mutable.ArrayBuffer.empty[PairUpdate]
  val warnings = mutable.ArrayBuffer.empty[String]
  val matchedIds = mutable.Set.empty[Long]

  links.foreach { link =>
    val direct = byEndpoints.get((link.leftPeerId, link.leftAlbumId, link.rightPeerId, link.rightAlbumId))
    val flipped = byEndpoints.get((link.rightPeerId, link.rightAlbumId, link.leftPeerId, link.leftAlbumId))
    val (existing, effectiveMode) = direct match {
      case Some(pair) => (Some(pair), link.mode)
      case None       => (flipped, flipMode(link.mode))
    }
    existing match {
      case None =>
        inserts += link
      case Some(pair) =>
        matchedIds += pair.id
        if (pair.linkSource == "manual") {
          warnings += s"pair '${pair.name}' is manually configured; annotations on its albums are ignored"
        } else {
          val changed = pair.mode != effectiveMode || pair.propagateDeletes != link.propagateDeletes || !pair.enabled
          if (changed) {
            updates += PairUpdate(pair.id, effectiveMode, link.propagateDeletes, reenable = !pair.enabled)
          }
        }
    }
  }

  // Annotation-created pairs whose group membership disappeared are disabled, never
  // deleted: run history, tombstones and the deletion log stay intact. Re-grouping
  // later re-enables (or re-creates) with force_additive so a stale baseline cannot
  // drive removals. Only pairs whose BOTH peers were scanned can be judged: a peer
  // that is disabled (server-level pause) or unreachable must not look like "all its
  // albums were unlinked".
  val disables = existingPairs
    .filter(p =>
      p.linkSource == "annotation" && p.enabled && !matchedIds.contains(p.id) &&
        scannedPeerIds.contains(p.leftPeerId) && scannedPeerIds.contains(p.rightPeerId)
    )
    .map(_.id)

  PairReconciliation(inserts.toSeq, updates.toSeq, disables, warnings.toSeq)

// ---------------------------------------------------------------------------
// DB application
// ---------------------------------------------------------------------------

def loadAllPairs()(using DbCon): Vector[AlbumPair] =
  sql"""
      SELECT id, name, left_peer_id, left_album_id, right_peer_id, right_album_id, mode, propagate_deletes, enabled, trash_orphaned_assets, force_additive, max_removal_count, max_removal_fraction, link_source
      FROM album_pair
    """.query[PairRow].run().map(pairFromRow)

// Returns the number of rows inserted (0 when ON CONFLICT swallowed the insert, e.g. an
// auto-name collision) so the caller can report honestly instead of silently dropping.
def insertAnnotationPair(name: String, link: DiscoveredLink)(using DbTx): Int =
  sql"""
      INSERT INTO album_pair(
        name, left_peer_id, left_album_id, right_peer_id, right_album_id,
        mode, propagate_deletes, enabled, force_additive, link_source
      )
      VALUES (
        $name, ${link.leftPeerId}, ${link.leftAlbumId}, ${link.rightPeerId}, ${link.rightAlbumId},
        ${link.mode}, ${link.propagateDeletes}, TRUE, TRUE, 'annotation'
      )
      ON CONFLICT DO NOTHING
    """.update.run()

def updateAnnotationPair(update: PairUpdate)(using DbTx): Unit =
  sql"""
      UPDATE album_pair
      SET mode = ${update.mode},
          propagate_deletes = ${update.propagateDeletes},
          enabled = TRUE,
          force_additive = CASE WHEN ${update.reenable} THEN TRUE ELSE force_additive END,
          updated_at = now()
      WHERE id = ${update.pairId} AND link_source = 'annotation'
    """.update.run()

def disableAnnotationPair(pairId: Long)(using DbTx): Unit =
  sql"""
      UPDATE album_pair
      SET enabled = FALSE,
          updated_at = now()
      WHERE id = $pairId AND link_source = 'annotation'
    """.update.run()

def autoPairName(peerNameById: Map[Long, String], link: DiscoveredLink): String =
  val leftPeer = peerNameById.getOrElse(link.leftPeerId, link.leftPeerId.toString)
  val rightPeer = peerNameById.getOrElse(link.rightPeerId, link.rightPeerId.toString)
  s"auto/$leftPeer-$rightPeer/${link.leftAlbumId.take(8)}-${link.rightAlbumId.take(8)}"

def runAnnotationDiscovery(
    db: DbRuntime,
    api: ImmichApi,
    resolveApiKey: SyncPeer => String,
    applyWrites: Boolean,
): Unit =
  val peers = connect(db.xa)(loadEnabledPeers())
  if (peers.isEmpty) {
    println("[discover] no enabled peers, skipping annotation discovery")
  } else {
    // Structural dry-run guarantee for the Immich side: stamping goes through the
    // read-only facade when not applying.
    val effectiveApi = if (applyWrites) api else DryRunImmichApi(api)
    val serverByPeerId = peers.map(p => p.id -> ImmichServer(p.baseUrl, resolveApiKey(p))).toMap
    // A failed album listing aborts discovery for ALL peers: an unreachable peer must
    // not look like "no annotations" and disable its pairs.
    val albumsByPeer = peers.map(peer => peer.id -> effectiveApi.listAlbums(serverByPeerId(peer.id))).toMap

    val plan = planDiscovery(peers, albumsByPeer)
    plan.warnings.foreach(w => println(s"[discover] $w"))

    plan.autoAnnotations.foreach { annotation =>
      effectiveApi.updateAlbumDescription(Album(serverByPeerId(annotation.peerId), annotation.albumId), annotation.newDescription)
      val verb = if (applyWrites) "stamped" else "would stamp"
      println(s"[discover] $verb album ${annotation.albumId} with [sync ${annotation.token}]")
    }

    val existing = connect(db.xa)(loadAllPairs())
    val rec = reconcilePairs(existing, plan.links, scannedPeerIds = albumsByPeer.keySet)
    rec.warnings.foreach(w => println(s"[discover] $w"))
    val peerNameById = peers.map(p => p.id -> p.name).toMap

    if (applyWrites) {
      val inserted = transact(db.xa):
        plan.memberships.foreach { membership =>
          val groupId = membership.group.map(upsertSyncGroup)
          upsertSyncAlbum(membership.peerId, membership.albumId, groupId, membership.deletes, membership.direction)
        }
        val insertCounts = rec.inserts.map { link =>
          val name = autoPairName(peerNameById, link)
          val count = insertAnnotationPair(name, link)
          if (count == 0) {
            System.err.println(s"[discover] pair '$name' was NOT created (name or endpoint conflict); rename the colliding pair")
          }
          count
        }.sum
        rec.updates.foreach(updateAnnotationPair)
        rec.disables.foreach(disableAnnotationPair)
        insertCounts
      println(
        s"[discover] groups=${plan.memberships.flatMap(_.group).distinct.size} links=${plan.links.size} " +
          s"inserted=$inserted updated=${rec.updates.size} disabled=${rec.disables.size} " +
          s"stamped=${plan.autoAnnotations.size}"
      )
    } else {
      // Dry runs leave the topology untouched too: report what a real run would do.
      println(
        s"[discover] dry run: would insert ${rec.inserts.size}, update ${rec.updates.size}, " +
          s"disable ${rec.disables.size} pair(s); groups=${plan.memberships.flatMap(_.group).distinct.size} links=${plan.links.size}"
      )
      rec.disables.foreach(id => println(s"[discover] dry run: would disable pair id $id (membership vanished)"))
    }
  }
