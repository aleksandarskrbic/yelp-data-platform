package spark.jobs.storage

import zio._
import com.amazonaws.services.s3.AmazonS3
import collection.JavaConverters._

final class FileRepository(s3: AmazonS3) {
  def listBucket(bucket: String): ZIO[Any, Throwable, List[String]] =
    ZIO.effect(s3.listObjects(bucket)).map { objectListing =>
      val objectSummaries = objectListing.getObjectSummaries.asScala.toList
      val filenames       = objectSummaries.map(_.getKey)
      filenames
    }
}

object FileRepository {
  lazy val live = (for {
    s3ClientWrapper <- ZIO.service[S3ClientWrapper]
    amazonS3Client  <- s3ClientWrapper.get
  } yield new FileRepository(amazonS3Client)).toLayer
}
