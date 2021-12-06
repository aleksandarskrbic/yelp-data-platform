package s3.loader.service

import zio._
import zio.clock._

import java.io.File
import logstage.LogZIO.log
import s3.loader.common.AppConfig
import s3.loader.storage.S3Client
import s3.loader.model.UploadMetadata
import com.amazonaws.services.s3.model._

import java.util.concurrent.TimeUnit
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

final class UploadService(s3Client: S3Client, storageConfig: AppConfig.Storage) {
  def upload(file: File) =
    for {
      started        <- currentTime(TimeUnit.MILLISECONDS)
      metadata       <- ZIO.succeed(createUploadMetadata(file))
      initResponse   <- s3Client.initMultipartUpload(metadata)
      requests       <- ZIO.succeed(createRequests(file, initResponse.getUploadId, metadata))
      responses      <- ZIO.foreachParN(64)(requests)(s3Client.uploadPart)
      completeRequest = createCompleteRequest(metadata, initResponse.getUploadId, responses)
      completed      <- s3Client.completeMultipartUpload(completeRequest)
      finished       <- currentTime(TimeUnit.MILLISECONDS)
      total           = (finished - started) / 1000
      _              <- log.info(s"Upload time for file=${file.getName} is ${total}s")
    } yield completed

  private def createUploadMetadata(file: File): UploadMetadata = {
    val filename    = file.getName
    val bucket      = storageConfig.bucket
    val initRequest = new InitiateMultipartUploadRequest(bucket, filename)
    UploadMetadata(file.getName, storageConfig.bucket, initRequest)
  }

  private def createCompleteRequest(
    metadata: UploadMetadata,
    uploadId: String,
    responses: List[UploadPartResult]
  ): CompleteMultipartUploadRequest =
    new CompleteMultipartUploadRequest(
      metadata.bucketName,
      metadata.filename,
      uploadId,
      responses.map(_.getPartETag).asJava
    )

  private def createRequests(
    file: File,
    uploadId: String,
    uploadMetadata: UploadMetadata
  ): List[UploadPartRequest] = {
    var part = 1
    var pos  = 0L

    val contentLength  = file.length
    var partSize: Long = 5 * 1024 * 1024

    val requests = new ListBuffer[UploadPartRequest]()

    while (pos < contentLength) {
      partSize = Math.min(partSize, contentLength - pos)

      val uploadRequest = new UploadPartRequest()
        .withBucketName(uploadMetadata.bucketName)
        .withKey(uploadMetadata.filename)
        .withUploadId(uploadId)
        .withPartNumber(part)
        .withFileOffset(pos)
        .withFile(file)
        .withPartSize(partSize)

      part += 1
      pos += partSize

      requests.append(uploadRequest)
    }

    requests.toList
  }
}

object UploadService {
  lazy val live = (for {
    s3Client     <- ZIO.service[S3Client]
    appConfig    <- ZIO.service[AppConfig]
    storageConfig = appConfig.storage
  } yield new UploadService(s3Client, storageConfig)).toLayer
}
