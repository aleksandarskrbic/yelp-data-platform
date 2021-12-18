package spark.jobs.storage

import zio._
import com.amazonaws.services.s3.{AmazonS3, model}
import com.amazonaws.services.s3.model.DeleteObjectRequest
import spark.jobs.adapter.s3.S3ClientWrapper

import collection.JavaConverters._
import scala.util.control.NoStackTrace

final class FileRepository(s3: AmazonS3) {
  def listBucket(bucket: String, folder: String): Task[String] =
    ZIO.effect(s3.listObjects(bucket)).flatMap { objectListing =>
      val objectSummaries = objectListing.getObjectSummaries.asScala.toList
      objectSummaries
        .map(_.getKey)
        .find { fullname =>
          fullname.contains(folder) && fullname.takeRight(3) == "csv"
        } match {
        case Some(filename) => ZIO.succeed(filename)
        case None           => ZIO.fail(FileRepository.Error("File not found."))
      }
    }

  def deleteFiles(bucket: String, folder: String): Task[Unit] =
    ZIO.effect(s3.listObjects(bucket)).flatMap { objectListing =>
      val objectSummaries = objectListing.getObjectSummaries.asScala.toList
      val objectsToDelete = objectSummaries.map(_.getKey).filter(_.contains(folder))
      ZIO.foreachPar_(objectsToDelete)(objectToDelete =>
        ZIO.effect(s3.deleteObject(new DeleteObjectRequest(bucket, objectToDelete)))
      )
    }
}

object FileRepository {
  case class Error(message: String) extends NoStackTrace

  lazy val live = (for {
    s3ClientWrapper <- ZIO.service[S3ClientWrapper]
    amazonS3Client  <- s3ClientWrapper.get
  } yield new FileRepository(amazonS3Client)).toLayer
}
