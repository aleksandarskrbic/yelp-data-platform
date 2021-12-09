package spark.jobs.storage

import zio._
import zio.duration._
import zio.clock.Clock
import logstage.LogZIO
import logstage.LogZIO.log
import spark.jobs.adapter.S3ClientWrapper
import com.amazonaws.services.s3.AmazonS3
import collection.JavaConverters._

final class FileRepository(s3: AmazonS3) {
  def listBucket(bucket: String): Task[List[String]] =
    ZIO.effect(s3.listObjects(bucket)).map { objectListing =>
      val objectSummaries = objectListing.getObjectSummaries.asScala.toList
      val filenames       = objectSummaries.map(_.getKey)
      filenames
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
}

object FileRepository {
  lazy val live = (for {
    s3ClientWrapper <- ZIO.service[S3ClientWrapper]
    amazonS3Client  <- s3ClientWrapper.get
  } yield new FileRepository(amazonS3Client)).toLayer
}
