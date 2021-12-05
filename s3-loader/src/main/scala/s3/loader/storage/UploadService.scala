package s3.loader.storage

import zio._
import java.io.File
import logstage.LogZIO
import s3.loader.config.AppConfig
import s3.loader.storage.UploadService.UploadMetadata
import collection.JavaConverters._
import com.amazonaws.services.s3.model._
import scala.collection.mutable.ListBuffer

final class UploadService(s3Client: S3Client, storageConfig: AppConfig.Storage) {
  def upload(file: File): ZIO[LogZIO, Throwable, CompleteMultipartUploadResult] =
    for {
      metadata       <- ZIO.succeed(createUploadMetadata(file))
      initResponse   <- s3Client.initMultipartUpload(metadata)
      requests       <- ZIO.succeed(createRequests(file, initResponse.getUploadId, metadata))
      responses      <- ZIO.foreachParN(4)(requests)(s3Client.uploadPart)
      completeRequest = createCompleteRequest(metadata, initResponse.getUploadId, responses)
      completed      <- s3Client.completeMultipartUpload(completeRequest)
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
      partSize = Math.min(partSize, (contentLength - pos))

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
  final case class UploadMetadata(filename: String, bucketName: String, initRequest: InitiateMultipartUploadRequest)

  lazy val live = (for {
    s3Client     <- ZIO.service[S3Client]
    appConfig    <- ZIO.service[AppConfig]
    storageConfig = appConfig.storage
  } yield new UploadService(s3Client, storageConfig)).toLayer
}
