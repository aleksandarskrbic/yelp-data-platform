package s3.loader.storage

import zio._
import logstage.LogZIO
import logstage.LogZIO.log
import s3.loader.model.UploadMetadata
import com.amazonaws.services.s3.model._
import com.amazonaws.services.s3.AmazonS3

final class S3Client(s3: AmazonS3) {
  def initMultipartUpload(
    uploadMetadata: UploadMetadata
  ): ZIO[LogZIO, Throwable, InitiateMultipartUploadResult] =
    (for {
      response <- ZIO.effect(s3.initiateMultipartUpload(uploadMetadata.initRequest))
      _        <- log.info(s"Initiating multipart upload with uploadId=${response.getUploadId}")
    } yield response).tapError(e => log.error(s"Failed to init multipart upload $e"))

  def completeMultipartUpload(
    completeRequest: CompleteMultipartUploadRequest
  ): ZIO[LogZIO, Throwable, CompleteMultipartUploadResult] =
    (for {
      _        <- log.info(s"Sending complete request for a uploadId=${completeRequest.getUploadId}")
      response <- ZIO.effect(s3.completeMultipartUpload(completeRequest))
    } yield response).tapError(e => log.error(s"Failed to complete multipart upload $e"))

  def uploadPart(uploadRequest: UploadPartRequest): Task[UploadPartResult] =
    ZIO.effect(s3.uploadPart(uploadRequest))
}

object S3Client {
  lazy val live = (for {
    s3ClientWrapper <- ZIO.service[S3ClientWrapper]
    amazonS3Client  <- s3ClientWrapper.get
  } yield new S3Client(amazonS3Client)).toLayer
}
