package spark.jobs.storage

import zio._
import zio.clock._
import logstage.LogZIO
import org.apache.spark.sql.DataFrame
import spark.jobs.adapter.spark.SparkWrapper
import spark.jobs.common.AppConfig

final class DataSource(sparkWrapper: SparkWrapper, source: AppConfig.Source) {
  def users(limit: Option[Int] = None): ZIO[LogZIO with Clock, Throwable, DataFrame] =
    sparkWrapper.read(path = source.users, maybeLimit = limit)

  def reviews(limit: Option[Int] = None): ZIO[LogZIO with Clock, Throwable, DataFrame] =
    sparkWrapper.read(path = source.reviews, maybeLimit = limit)

  def checkins(limit: Option[Int] = None): ZIO[LogZIO with Clock, Throwable, DataFrame] =
    sparkWrapper.read(path = source.checkins, maybeLimit = limit)

  def businesses(limit: Option[Int] = None): ZIO[LogZIO with Clock, Throwable, DataFrame] =
    sparkWrapper.read(path = source.businesses, maybeLimit = limit)
}

object DataSource {
  lazy val live = (for {
    appConfig    <- ZIO.service[AppConfig]
    sparkWrapper <- ZIO.service[SparkWrapper]
  } yield new DataSource(sparkWrapper, appConfig.source)).toLayer
}
