package query.service.loader

import zio._
import zio.stream._
import zio.blocking._
import scala.util.control.NoStackTrace
import `object`.storage.shared.s3.S3Client
import query.service.common.AppConfig
import query.service.model.{BusinessByCityCount, OpenedBusinessStats}

final class FileLoader(bucket: String, s3Client: S3Client) {

  def businessByCityStream(): ZStream[Blocking, Throwable, BusinessByCityCount] =
    streamCSV("business_by_city").aggregate(Transducers.businessByCityCount)

  def businessByIsOpen(): ZStream[Blocking, Throwable, OpenedBusinessStats] =
    streamCSV("business_by_is_open").aggregate(Transducers.businessByIsOpen)

  def trendingBusinesses(): ZStream[Blocking, Throwable, OpenedBusinessStats] =
    streamCSV("trending_businesses").aggregate(Transducers.businessByIsOpen)

  private def streamCSV(
    filename: String,
    dropHead: Boolean = true
  ): ZStream[Blocking, FileLoader.Error, String] = {
    val stream = ZStream
      .fromEffect(s3Client.listBucket(bucket, filename))
      .mapError(error => FileLoader.Error(error.message))
      .flatMap {
        case Some(path) => s3Client.streamFile(bucket, path).mapError(error => FileLoader.Error(error.message))
        case None       => ZStream.fail(FileLoader.Error("File does not exists"))
      }

    if (dropHead) stream.drop(1)
    else stream
  }
}

object FileLoader {
  case class Error(message: String) extends NoStackTrace

  lazy val live = (for {
    appConfig <- ZIO.service[AppConfig]
    s3Client  <- ZIO.service[S3Client]
  } yield new FileLoader(appConfig.storage.bucket, s3Client)).toLayer
}
