package spark.jobs.adapter

import logstage.LogZIO.log
import logstage.LogZIO
import zio._
import zio.duration._
import zio.clock.Clock
import spark.jobs.common.AppConfig
import spark.jobs.common.AppConfig.S3Path
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

final class SparkWrapper(sparkSession: SparkSession) {
  def readJson(path: S3Path): ZIO[LogZIO with Clock, Throwable, DataFrame] =
    ZIO
      .effect(sparkSession.read.format("json").load(path.value))
      .retry(Schedule.exponential(100.millis) && Schedule.recurs(10))
      .foldM(
        error => log.error(s"Unable to load ${path}") *> ZIO.fail(error),
        dataframe => ZIO.succeed(dataframe)
      )
}

object SparkWrapper {
  lazy val live = (for {
    appConfig <- ZIO.service[AppConfig]
    storage    = appConfig.storage
    sparkConf = new SparkConf()
                  .setAppName("MinIO example")
                  .setMaster("local")
                  .set("spark.hadoop.fs.s3a.endpoint", storage.serviceEndpoint)
                  .set("spark.hadoop.fs.s3a.access.key", storage.credentials.accessKey)
                  .set("spark.hadoop.fs.s3a.secret.key", storage.credentials.accessKey)
                  .set("spark.hadoop.fs.s3a.fast.upload", "true")
                  .set("spark.hadoop.fs.s3a.path.style.access", "true")
                  .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    sparkSession <- ZIO.effect(SparkSession.builder.config(sparkConf).getOrCreate())
  } yield new SparkWrapper(sparkSession)).toLayer
}
