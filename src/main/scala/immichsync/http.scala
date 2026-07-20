package immichsync

// Immich HTTP API (targets Immich v3.x), sttp-client4 with the synchronous backend —
// blocking calls are cheap on Ox virtual threads.

import sttp.client4.*
import sttp.model.Uri

import scala.concurrent.duration.*

trait ImmichApi:
  def serverVersion(server: ImmichServer): String
  def listAlbums(server: ImmichServer): Seq[AlbumSummary]
  def updateAlbumDescription(album: Album, description: String): Unit
  def albumGetAssets(album: Album): Seq[AssetResponseDto]
  def assetInfo(server: ImmichServer, assetId: String): AssetResponseDto
  def assetBulkCheck(server: ImmichServer, assets: Seq[AssetResponseDto]): Seq[BulkCheckResp[AssetResponseDto]]
  def untrash(server: ImmichServer, assetIds: Seq[String]): Unit
  def albumAddAssets(album: Album, assetIds: Seq[String]): Unit
  def albumRemoveAssets(album: Album, assetIds: Seq[String]): AlbumRemoveResult
  def albumsContainingAsset(server: ImmichServer, assetId: String): Seq[String]
  def trashAssets(server: ImmichServer, assetIds: Seq[String]): Unit
  def assetGet(server: ImmichServer, assetId: String): Array[Byte]
  def assetUpload(server: ImmichServer, uploadRequest: AssetUploadRequest): UploadResult

class LiveImmichApi(backend: SyncBackend) extends ImmichApi:
  private val ApiTimeout = 60.seconds
  private val TransferTimeout = 10.minutes

  private def base(server: ImmichServer, timeout: FiniteDuration = ApiTimeout) =
    basicRequest
      .header("x-api-key", server.apiKey)
      .header("Accept", "application/json")
      .readTimeout(timeout)
      .response(asStringAlways)

  private def sendJson(request: Request[String], context: String): ujson.Value =
    val response = request.send(backend)
    require(response.code.isSuccess, s"$context: http error ${response.code} ${response.body.take(500)}")
    if (response.body.isEmpty) ujson.Null else ujson.read(response.body)

  override def serverVersion(server: ImmichServer): String =
    withRetry() {
      sendJson(base(server).get(uri"${server.apiBaseUrl}/server/about"), "server about")("version").str
    }

  override def listAlbums(server: ImmichServer): Seq[AlbumSummary] =
    withRetry() {
      // v3: unfiltered listing returns owned + shared-with-me albums. For a dedicated
      // sync user that is exactly the opt-in set.
      sendJson(base(server).get(uri"${server.apiBaseUrl}/albums"), "list albums").arr.map { album =>
        AlbumSummary(
          id = album("id").str,
          albumName = album("albumName").str,
          description = album.obj.get("description").flatMap(_.strOpt).getOrElse(""),
        )
      }.toSeq
    }

  override def updateAlbumDescription(album: Album, description: String): Unit =
    sendJson(
      base(album.server)
        .patch(uri"${album.server.apiBaseUrl}/albums/${album.id}")
        .body(ujson.Obj("description" -> description).render())
        .contentType("application/json"),
      "update album description",
    )

  // v3 removed the embedded assets array from album DTOs; membership is enumerated via
  // the paginated metadata search. The album is fetched first so a missing album fails
  // with a 404 instead of looking like an empty search result.
  override def albumGetAssets(album: Album): Seq[AssetResponseDto] =
    withRetry() {
      val info = sendJson(
        base(album.server).get(uri"${album.server.apiBaseUrl}/albums/${album.id}?withoutAssets=true"),
        "get album",
      )
      info.obj.get("assets").flatMap(_.arrOpt) match {
        // Pre-v3 servers still embed the full asset list; use it directly.
        case Some(embedded) if embedded.nonEmpty =>
          embedded.map(asset => upickle.default.read[AssetResponseDto](asset)).toSeq
        case _ =>
          val results = Seq.newBuilder[AssetResponseDto]
          var page: Option[Int] = Some(1)
          while (page.isDefined) {
            // A failed page throws: a truncated listing must never become an observation set.
            val json = sendJson(
              base(album.server)
                .post(uri"${album.server.apiBaseUrl}/search/metadata")
                .body(ujson.Obj(
                  "albumIds" -> ujson.Arr(album.id),
                  "page" -> page.get,
                  "size" -> 1000,
                ).render())
                .contentType("application/json"),
              "search album assets",
            )
            val assets = json("assets")
            results ++= assets("items").arr.map(asset => upickle.default.read[AssetResponseDto](asset))
            page = assets.obj.get("nextPage").flatMap(_.strOpt).map(_.toInt)
          }
          results.result()
      }
    }

  override def assetInfo(server: ImmichServer, assetId: String): AssetResponseDto =
    withRetry() {
      upickle.default.read[AssetResponseDto](
        sendJson(base(server).get(uri"${server.apiBaseUrl}/assets/$assetId"), "asset info")
      )
    }

  override def assetBulkCheck(server: ImmichServer, assets: Seq[AssetResponseDto]): Seq[BulkCheckResp[AssetResponseDto]] =
    if (assets.isEmpty) Seq.empty
    else withRetry() {
      val assetById = assets.map(asset => asset.id -> asset).toMap
      val json = sendJson(
        base(server)
          .post(uri"${server.apiBaseUrl}/assets/bulk-upload-check")
          .body(ujson.Obj(
            "assets" -> assets.map(asset => ujson.Obj("id" -> asset.id, "checksum" -> asset.checksum))
          ).render())
          .contentType("application/json"),
        "bulk upload check",
      )
      json("results").arr.map { e =>
        val id = e("id").str
        lazy val asset = assetById(id)
        e("action").str match {
          case "accept" => BulkCheckResp(asset, None, false)
          case "reject" =>
            val assetId = e("assetId").str
            val isTrashed = e("isTrashed").bool
            BulkCheckResp(asset, Some(assetId), isTrashed)
        }
      }.toSeq
    }

  override def untrash(server: ImmichServer, assetIds: Seq[String]): Unit =
    if (assetIds.nonEmpty) {
      val json = sendJson(
        base(server)
          .post(uri"${server.apiBaseUrl}/trash/restore/assets")
          .body(ujson.Obj("ids" -> assetIds).render())
          .contentType("application/json"),
        "untrash",
      )
      val count = json("count").num.toLong
      require(count == assetIds.size, "not all items could be untrashed")
    }

  // PUT /api/albums/{id}/assets returns one BulkIdResponseDto per id; an id that is
  // already in the album comes back success=false error="duplicate", which is fine here.
  override def albumAddAssets(album: Album, assetIds: Seq[String]): Unit =
    if (assetIds.nonEmpty) {
      val results = sendJson(
        base(album.server)
          .put(uri"${album.server.apiBaseUrl}/albums/${album.id}/assets")
          .body(ujson.Obj("ids" -> assetIds).render())
          .contentType("application/json"),
        "album add assets",
      ).arr
      val failed = results.filter { e =>
        !e("success").bool && !e.obj.get("error").exists(_.strOpt.contains("duplicate"))
      }
      require(failed.isEmpty, s"failed to add assets to album: $failed")
    }

  // Best-effort: a sync user (album editor) may only remove assets it owns; per-id
  // permission failures are reported as skipped, never thrown.
  override def albumRemoveAssets(album: Album, assetIds: Seq[String]): AlbumRemoveResult =
    if (assetIds.isEmpty) AlbumRemoveResult(Seq.empty, Seq.empty)
    else {
      val results = sendJson(
        base(album.server)
          .delete(uri"${album.server.apiBaseUrl}/albums/${album.id}/assets")
          .body(ujson.Obj("ids" -> assetIds).render())
          .contentType("application/json"),
        "album remove assets",
      ).arr
      val (ok, failed) = results.toSeq.partition { e =>
        e("success").bool || e.obj.get("error").exists(_.strOpt.contains("not_found"))
      }
      AlbumRemoveResult(
        removed = ok.map(_("id").str),
        skipped = failed.map(e => e("id").str -> e.obj.get("error").flatMap(_.strOpt).getOrElse("unknown")),
      )
    }

  override def albumsContainingAsset(server: ImmichServer, assetId: String): Seq[String] =
    withRetry() {
      sendJson(base(server).get(uri"${server.apiBaseUrl}/albums?assetId=$assetId"), "albums containing asset")
        .arr.map(_("id").str).toSeq
    }

  // Reversible: force=false moves assets to the trash, never permanently deletes.
  override def trashAssets(server: ImmichServer, assetIds: Seq[String]): Unit =
    if (assetIds.nonEmpty) {
      val response = base(server)
        .delete(uri"${server.apiBaseUrl}/assets")
        .body(ujson.Obj("ids" -> assetIds, "force" -> false).render())
        .contentType("application/json")
        .send(backend)
      require(response.code.isSuccess, s"trash assets: http error ${response.code}")
    }

  override def assetGet(server: ImmichServer, assetId: String): Array[Byte] =
    withRetry() {
      val response = basicRequest
        .header("x-api-key", server.apiKey)
        .readTimeout(TransferTimeout)
        .get(uri"${server.apiBaseUrl}/assets/$assetId/original")
        .response(asByteArrayAlways)
        .send(backend)
      require(response.code.isSuccess, s"download asset: http error ${response.code}")
      response.body
    }

  override def assetUpload(server: ImmichServer, uploadRequest: AssetUploadRequest): UploadResult =
    // Idempotent thanks to checksum dedup: a retried upload of stored bytes returns
    // the existing asset as a duplicate.
    withRetry() {
      val parts = Seq(
        Some(multipart("assetData", uploadRequest.assetData).fileName(uploadRequest.filename)),
        Some(multipart("filename", uploadRequest.filename)),
        Some(multipart("fileCreatedAt", uploadRequest.fileCreatedAt)),
        Some(multipart("fileModifiedAt", uploadRequest.fileModifiedAt)),
        uploadRequest.duration.map(multipart("duration", _)),
        uploadRequest.isFavorite.map(v => multipart("isFavorite", v.toString)),
        uploadRequest.livePhotoVideoId.map(multipart("livePhotoVideoId", _)),
        uploadRequest.visibility.map(multipart("visibility", _)),
        uploadRequest.sidecarData.map(b => multipart("sidecarData", b).fileName(s"${uploadRequest.filename}.xmp")),
      ).flatten

      var request = basicRequest
        .header("x-api-key", server.apiKey)
        .header("Accept", "application/json")
        .readTimeout(TransferTimeout)
        .post(uri"${server.apiBaseUrl}/assets")
        .multipartBody(parts)
        .response(asStringAlways)
      uploadRequest.checksum.foreach(c => request = request.header("x-immich-checksum", c))

      val response = request.send(backend)
      require(response.code.isSuccess, s"upload asset: http error ${response.code} ${response.body.take(500)}")
      val json = ujson.read(response.body)
      json("status").str match {
        case status @ ("created" | "duplicate" | "replaced") => UploadResult(json("id").str, status)
        case _ => throw new RuntimeException(s"failed to upload: $json")
      }
    }
