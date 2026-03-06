//> using scala 3.7.3
//> using dep com.lihaoyi::requests:0.9.0
//> using dep com.lihaoyi::ujson:4.3.2
//> using dep com.lihaoyi::upickle:4.3.2
//> using dep com.augustnagro::magnum:1.3.1
//> using dep org.xerial:sqlite-jdbc:3.50.3.0
//> using dep com.zaxxer:HikariCP:7.0.2

import geny.Bytes
import upickle.default.{macroRW, ReadWriter as RW}
import upickle.*

import java.io.ByteArrayOutputStream
import java.util.Base64

case class ImmichServer(baseUrl: String, apiKey: String) {
  def apiBaseUrl = s"$baseUrl/api"
}

case class Album(server: ImmichServer, id: String)

case class Asset(id: String, checksum: String)

given RW[Bytes] = upickle.readwriter[String].bimap[Bytes](e => Base64.getEncoder.encodeToString(e.array), e => Bytes(Base64.getDecoder.decode(e)))

case class AssetUploadRequest( // upload req also appends fileSize but it doesn't seem to be used
                               deviceAssetId: String, // upload req
                               deviceId: String, // upload req
                               duration: Option[String],
                               fileCreatedAt: String, // upload req
                               fileModifiedAt: String, // upload req
                               filename: Option[String],
                               isFavorite: Option[Boolean], // upload req const false
                               livePhotoVideoId: Option[String],
                               metadata: Option[Seq[ujson.Obj]],
                               sidecarData: Option[Bytes], // upload req optional
                               visibility: Option[String],
                               assetData: Bytes, // upload req
                             ) derives RW {
  def toMultiPart: requests.MultiPart = requests.MultiPart(List[Option[requests.MultiItem]](
    Some(assetData.array).map(requests.MultiItem("assetData", _, filename.get)),
    Some(deviceAssetId).map(requests.MultiItem("deviceAssetId", _)),
    Some(deviceId).map(requests.MultiItem("deviceId", _)),
    //duration.map(requests.MultiItem("duration", _)),
    Some(fileCreatedAt).map(requests.MultiItem("fileCreatedAt", _)),
    Some(fileModifiedAt).map(requests.MultiItem("fileModifiedAt", _)),
    filename.map(requests.MultiItem("filename", _)),
    //isFavorite.map(_.toString).map(requests.MultiItem("isFavorite", _)),
    //livePhotoVideoId.map(requests.MultiItem("livePhotoVideoId", _)),
    //metadata.map(upickle.write(_)).map(requests.MultiItem("metadata", _)),
    //sidecarData.map(requests.MultiItem("sidecarData", _)),
    //visibility.map(requests.MultiItem("visibility", _)),
  ).flatten *)
}

object AssetUploadRequest {
  def fromAssetResponseDto(assetResponseDto: AssetResponseDto, assetData: Array[Byte]): AssetUploadRequest = AssetUploadRequest(
    assetData = Bytes(assetData),
    deviceAssetId = assetResponseDto.deviceAssetId,
    deviceId = assetResponseDto.deviceId,
    duration = None, //Some(assetResponseDto.duration),
    fileCreatedAt = assetResponseDto.fileCreatedAt,
    fileModifiedAt = assetResponseDto.fileModifiedAt,
    filename = Some(assetResponseDto.originalFileName),
    isFavorite = Some(assetResponseDto.isFavorite),
    livePhotoVideoId = None, // assetResponseDto.livePhotoVideoId,
    metadata = None,
    sidecarData = None,
    visibility = None //Some(assetResponseDto.visibility)
  )
}

case class AssetResponseDto(
                             checksum: String,
                             createdAt: String,
                             deviceAssetId: String,
                             deviceId: String,
                             duplicateId: Option[String],
                             duration: String,
                             exifInfo: Option[ujson.Obj],
                             fileCreatedAt: String,
                             fileModifiedAt: String,
                             hasMetadata: Boolean,
                             id: String,
                             isArchived: Boolean,
                             isFavorite: Boolean,
                             isOffline: Boolean,
                             isTrashed: Boolean,
                             livePhotoVideoId: Option[String],
                             localDateTime: String,
                             originalFileName: String,
                             originalMimeType: Option[String],
                             originalPath: String,
                             ownerId: String,
                             updatedAt: String,
                             visibility: String,
                           ) derives RW


val lhns = ImmichServer("https://immich.example.de", "")
val test = ImmichServer("https://immich.example.de", "")

val source = Album(lhns, "")
val target = Album(test, "")

@main
def main(): Unit = {
  def albumGetAssets(album: Album): Seq[AssetResponseDto] = {
    val r = requests.get(
      url = s"${album.server.apiBaseUrl}/albums/${album.id}",
      headers = Map(
        "x-api-key" -> album.server.apiKey,
        "Accept" -> "application/json"
      )
    )
    require(r.is2xx, s"http error: ${r.statusCode}")
    val json = ujson.read(r.text())
    val assets = json("assets").arr
    assets.map(asset => upickle.read[AssetResponseDto](asset)).toSeq
    //assets.map(asset => Asset(id = asset("id").str, checksum = asset("checksum").str)).toSeq
  }

  case class BulkCheckResp[A](asset: A, assetId: Option[String], isTrashed: Boolean)

  // check if an asset already exists
  def assetBulkCheck[A](
                         server: ImmichServer,
                         assets: Seq[A]
                       )(
                         checksum: A => String
                       ): Seq[BulkCheckResp[A]] = {
    if (assets.nonEmpty) {
      val assetByChecksum = assets.map(asset => checksum(asset) -> asset).toMap
      val data = ujson.Obj(
        "assets" -> assetByChecksum.map { (assetChecksum, _) =>
          ujson.Obj(
            "id" -> assetChecksum, // this is not an asset id but can be any id
            "checksum" -> assetChecksum
          )
        }
      )
      //println(data)
      val r = requests.post(
        url = s"${server.apiBaseUrl}/assets/bulk-upload-check",
        headers = Map(
          "x-api-key" -> server.apiKey,
          "Accept" -> "application/json"
        ),
        data = data
      )
      require(r.is2xx, s"http error: ${r.statusCode}")
      val json = ujson.read(r.text())
      json("results").arr.map { e =>
        val id = e("id").str // contains the checksum
        lazy val asset = assetByChecksum(id)
        val resp = e("action").str match {
          case "accept" => BulkCheckResp(asset, None, false)
          case "reject" =>
            val assetId = e("assetId").str
            val isTrashed = e("isTrashed").bool
            BulkCheckResp(asset, Some(assetId), isTrashed)
        }
        resp
      }.toSeq
    } else Seq.empty
  }

  def untrash(server: ImmichServer, assetIds: Seq[String]): Unit = {
    if (assetIds.nonEmpty) {
      val r = requests.post(
        url = s"${server.apiBaseUrl}/trash/restore/assets",
        headers = Map(
          "x-api-key" -> server.apiKey,
          "Accept" -> "application/json"
        ),
        data = ujson.Obj(
          "ids" -> assetIds
        )
      )
      require(r.is2xx, s"http error: ${r.statusCode}")
      val json = ujson.read(r.text())
      val count = json("count").num.toLong
      require(count == assetIds.size, "not all items could be untrashed")
    }
  }

  def albumAddAssetsDumb(album: Album, assets: Seq[String]): Unit = {
    if (assets.nonEmpty) {
      val r = requests.put(
        url = s"${album.server.apiBaseUrl}/albums/assets",
        headers = Map(
          "x-api-key" -> album.server.apiKey,
          "Accept" -> "application/json"
        ),
        data = ujson.Obj(
          "albumIds" -> List(album.id),
          "assetIds" -> assets
        )
      )
      require(r.is2xx, s"http error: ${r.statusCode}")
      val json = ujson.read(r.text())
      val success = json("success").bool

      require(success, s"failed to add: ${json}")
    }
  }

  def assetGet(server: ImmichServer, assetId: String): Bytes = {
    val r = requests.get(
      url = s"${server.apiBaseUrl}/assets/$assetId/original",
      headers = Map(
        "x-api-key" -> server.apiKey
      )
    )
    require(r.is2xx, s"http error: ${r.statusCode}")
    val bytes = r.data
    bytes
  }

  def assetUpload(server: ImmichServer, uploadRequest: AssetUploadRequest): String = {
    val data = uploadRequest.toMultiPart
    /*val baos = new ByteArrayOutputStream()
    data.write(baos)
    println(new String(baos.toByteArray))*/
    val r = requests.post(
      url = s"${server.apiBaseUrl}/assets",
      headers = Map(
        "x-api-key" -> server.apiKey,
        "Accept" -> "application/json"
      ),
      data = data
    )
    require(r.is2xx, s"http error: ${r.statusCode}")
    val json = ujson.read(r.text())
    json("status").str match {
      case "created" => json("id").str
      case status => throw new RuntimeException(s"failed to upload: $json")
    }
  }

  /*
  1. get src and dst album contents (metadata)
  2. compare results and calculate diff to only upload missing assets
  3. check if the assets already exist on the dst user
  4. download asset bytes of assets that are yet to be uploaded to the dst user
  5. upload assets
  6. add assets to dst album
   */

  /*
  turn album into event stream
  - pull whole album asset list
  - put album list into db
  - compare album list with db
  - if asset was removed, generate removed event
   */

  // TODO
  //case class AlbumEntry(albumId: String, assetId: String, checksum: String)

  val albumAssets = albumGetAssets(source)
  val targetAlbumAssets = albumGetAssets(target)
  val diff = albumAssets.map(_.checksum).distinct.diff(targetAlbumAssets.map(_.checksum).distinct).toSet
  val bulkCheckResults = assetBulkCheck(test, albumAssets.filter(e => diff.contains(e.checksum)))(_.checksum)
  val (existingResults, nonExistingResults) = bulkCheckResults.partition(_.assetId.isDefined)
  val toUntrash = existingResults.filter(_.isTrashed).flatMap(_.assetId)
  println(toUntrash)
  untrash(target.server, toUntrash) // after untrashing we don't know which albums the asset is in, so we might get a duplicate error on add
  val assetsToUpload = nonExistingResults.map { dto =>
    val bytes = assetGet(lhns, dto.asset.id)
    AssetUploadRequest.fromAssetResponseDto(dto.asset, bytes.array)
  }
  val addToAlbum = assetsToUpload.map { a =>
    assetUpload(test, a)
  }
  albumAddAssetsDumb(target, addToAlbum ++ existingResults.filterNot(_.isTrashed).flatMap(_.assetId))
  //albumAddAssets(target, albumGetAssets(source))
}
