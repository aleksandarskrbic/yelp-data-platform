package `object`.storage.shared.s3

import zio._
import zio.clock._
import zio.stream._
import zio.blocking._
import zio.duration._
import logstage.LogZIO
import logstage.LogZIO.log
import collection.JavaConverters._
import scala.util.control.NoStackTrace
import com.amazonaws.services.s3.model._
import com.amazonaws.services.s3.AmazonS3
import `object`.storage.shared.s3.model.{UploadMetadata, UploadPart}

class S3Client(s3: AmazonS3) {
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

  def listBucket(bucket: String, folder: String): Task[Option[String]] =
    for {
      listing   <- ZIO.effect(s3.listObjects(bucket))
      filenames <- ZIO.effect(Chunk.fromIterable(listing.getObjectSummaries.asScala.map(_.getKey)))
      filename <- ZIO.filterPar(filenames) { fullname =>
                    ZIO.succeed(fullname.contains(folder) && fullname.takeRight(3) == "csv")
                  }
    } yield filename.headOption

  def deleteFiles(bucket: String, folder: String): Task[Unit] =
    ZIO.effect(s3.listObjects(bucket)).flatMap { objectListing =>
      val objectSummaries = objectListing.getObjectSummaries.asScala.toList
      val objectsToDelete = objectSummaries.map(_.getKey).filter(_.contains(folder))
      ZIO.foreachPar_(objectsToDelete)(objectToDelete =>
        ZIO.effect(s3.deleteObject(new DeleteObjectRequest(bucket, objectToDelete)))
      )
    }

  def streamFile(bucket: String, filename: String): ZStream[Blocking, Throwable, String] =
    ZStream
      .fromEffect(ZIO.effect(s3.getObject(new GetObjectRequest(bucket, filename))))
      .flatMap { inputStream =>
        ZStream
          .fromInputStream(inputStream.getObjectContent)
          .aggregate(ZTransducer.utfDecode >>> ZTransducer.splitLines)
      }
}

object S3Client {
  case class Error(message: String) extends NoStackTrace

  lazy val live: ZLayer[LogZIO with Has[S3ClientWrapper], Nothing, Has[S3Client]] = (for {
    s3ClientWrapper <- ZIO.service[S3ClientWrapper]
    amazonS3Client  <- s3ClientWrapper.get
  } yield new S3Client(amazonS3Client)).toLayer
}
