package spark.jobs.storage

import zio._
import zio.clock._
import logstage.LogZIO
import org.apache.spark.sql.DataFrame
import spark.jobs.adapter.spark.SparkWrapper
import spark.jobs.common.AppConfig

final class DataSource(sparkWrapper: SparkWrapper, source: AppConfig.Source) {
  def users: ZIO[LogZIO with Clock, Throwable, DataFrame]      = sparkWrapper.read(source.users)
  def reviews: ZIO[LogZIO with Clock, Throwable, DataFrame]    = sparkWrapper.read(source.reviews)
  def checkins: ZIO[LogZIO with Clock, Throwable, DataFrame]   = sparkWrapper.read(source.checkins)
  def businesses: ZIO[LogZIO with Clock, Throwable, DataFrame] = sparkWrapper.read(source.businesses)
}

object DataSource {
  lazy val live = (for {
    appConfig    <- ZIO.service[AppConfig]
    sparkWrapper <- ZIO.service[SparkWrapper]
  } yield new DataSource(sparkWrapper, appConfig.source)).toLayer
}
