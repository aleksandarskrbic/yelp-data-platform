package spark.jobs.service

import zio._
import zio.clock.Clock
import logstage.LogZIO
import spark.jobs.common.AppConfig
import spark.jobs.adapter.SparkWrapper
import org.apache.spark.sql.DataFrame

final class DataLoader(sparkWrapper: SparkWrapper, source: AppConfig.Source) {
  def users: ZIO[LogZIO with Clock, Throwable, DataFrame]      = sparkWrapper.readJson(source.users)
  def reviews: ZIO[LogZIO with Clock, Throwable, DataFrame]    = sparkWrapper.readJson(source.reviews)
  def checkins: ZIO[LogZIO with Clock, Throwable, DataFrame]   = sparkWrapper.readJson(source.checkins)
  def businesses: ZIO[LogZIO with Clock, Throwable, DataFrame] = sparkWrapper.readJson(source.businesses)
}

object DataLoader {
  lazy val live = (for {
    sparkWrapper <- ZIO.service[SparkWrapper]
    appConfig    <- ZIO.service[AppConfig]
    source        = appConfig.source
  } yield new DataLoader(sparkWrapper, source)).toLayer
}
