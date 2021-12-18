package trending.businesses.aggregator.storage

import zio._
import zio.clock._
import zio.duration._
import logstage.LogZIO
import logstage.LogZIO.log
import com.amazonaws.services.s3.model._
import com.amazonaws.services.s3.AmazonS3
import trending.businesses.aggregator.common.AppConfig.S3Path
import trending.businesses.aggregator.model.{UploadMetadata, UploadPart}
import zio.blocking.Blocking
import zio.stream._

import collection.JavaConverters._

final class S3Client(s3: AmazonS3) {
  def streamFile(bucket: String, filename: String): ZStream[Blocking, Throwable, String] =
    ZStream.fromEffect {
      ZIO
        .effect(s3.getObject(new GetObjectRequest(bucket, filename)))
    }.flatMap(is =>
      ZStream.fromInputStream(is.getObjectContent).aggregate(ZTransducer.utfDecode >>> ZTransducer.splitLines)
    )

  def list(path: S3Path): Task[List[String]] =
    ZIO.effect(s3.listObjects(path.bucket)).map { objectListing =>
      val objectSummaries = objectListing.getObjectSummaries.asScala.toList
      objectSummaries.map(_.getKey).filter { fullname =>
        fullname.contains(path.folder) && fullname.takeRight(3) == "csv"
      }
    }

  def createBucketIfNotExists(
    bucketName: String
  ): ZIO[LogZIO with Clock, Throwable, Unit] =
    ZIO
      .effect(s3.listBuckets().asScala.toList)
      .map(buckets => buckets.find(_.getName == bucketName))
      .flatMap {
        case Some(_) =>
          log.info(s"bucket=$bucketName already exists")
        case None =>
          ZIO.effect(s3.createBucket(bucketName))
      }
      .retry(Schedule.exponential(50.millis) && Schedule.recurs(3))
      .foldM(
        error => log.error(s"Unable to create bucket=$bucketName") *> ZIO.fail(error),
        _ => log.info(s"Successfully created $bucketName")
      )

  def initMultipartUpload(
    uploadMetadata: UploadMetadata
  ): ZIO[LogZIO with Clock, Throwable, InitiateMultipartUploadResult] =
    ZIO
      .effect(s3.initiateMultipartUpload(uploadMetadata.initRequest))
      .tap(response => log.info(s"Initiated multipart upload. ${response.getUploadId}"))
      .retry(Schedule.exponential(50.millis) && Schedule.recurs(3))
      .foldM(
        error => log.error(s"Failed to init multipart upload $error") *> ZIO.fail(error),
        response => log.info(s"Successfully initiated upload ${response.getUploadId} ") *> ZIO.succeed(response)
      )

  def completeMultipartUpload(
    completeRequest: CompleteMultipartUploadRequest
  ): ZIO[LogZIO with Clock, Throwable, CompleteMultipartUploadResult] =
    (for {
      _        <- log.info(s"Sending complete request for a ${completeRequest.getUploadId}")
      response <- ZIO.effect(s3.completeMultipartUpload(completeRequest))
    } yield response)
      .retry(Schedule.exponential(50.millis) && Schedule.recurs(3))
      .foldM(
        error => log.error(s"Failed to complete multipart upload $error") *> ZIO.fail(error),
        response => log.info(s"Successfully completed upload ${completeRequest.getUploadId} ") *> ZIO.succeed(response)
      )

  def uploadPart(
    uploadPart: UploadPart
  ): ZIO[LogZIO with Clock, Throwable, UploadPartResult] =
    ZIO
      .effect(s3.uploadPart(uploadPart.uploadRequest))
      .retry(Schedule.exponential(25.millis) && Schedule.recurs(10))
      .foldM(
        error => log.error(s"Failed to upload ${uploadPart.part} for a ${uploadPart.filename}") *> ZIO.fail(error),
        result => ZIO.succeed(result)
      )
}

object S3Client {
  lazy val live = (for {
    s3ClientWrapper <- ZIO.service[S3ClientWrapper]
    amazonS3Client  <- s3ClientWrapper.get
  } yield new S3Client(amazonS3Client)).toLayer
}