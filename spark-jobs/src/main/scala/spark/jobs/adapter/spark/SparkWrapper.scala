package spark.jobs.adapter.spark

import logstage.LogZIO
import logstage.LogZIO.log
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import spark.jobs.common.AppConfig
import spark.jobs.common.AppConfig.S3Path
import zio._
import zio.clock.Clock
import zio.duration._

final class SparkWrapper(sparkSession: SparkSession, sink: AppConfig.Sink) {
  def withSession[A](fn: SparkSession => Task[A]): Task[A] =
    fn(sparkSession)

  def suspend[A](fn: => A): Task[A] =
    ZIO.effectSuspend {
      ZIO.effect(fn)
    }

  def read(
    path: S3Path,
    format: String = "json",
    options: Map[String, String] = Map.empty,
    maybeLimit: Option[Int] = None
  ): ZIO[LogZIO with Clock, Throwable, DataFrame] =
    ZIO.effect {
      maybeLimit match {
        case Some(limit) =>
          sparkSession.read.format(format).options(options).load(path.value).limit(limit).cache()
        case None =>
          sparkSession.read.format(format).options(options).load(path.value).cache()
      }
    }
      .retry(Schedule.exponential(100.millis) && Schedule.recurs(10))
      .foldM(
        error => log.error(s"Unable to load $path") *> ZIO.fail(error),
        dataframe => ZIO.succeed(dataframe)
      )

  def destination(value: String): String =
    s"${sink.bucket}$value"
}

object SparkWrapper {
  lazy val live = (for {
    appConfig <- ZIO.service[AppConfig]
    sparkSession <- ZIO.effectSuspend {
                      ZIO.effect {
                        val sparkSession = SparkSession.builder
                          .config(createSparkConf(appConfig.storage))
                          .getOrCreate()

                        sparkSession.sparkContext.setLogLevel("ERROR")
                        sparkSession
                      }
                    }
  } yield new SparkWrapper(sparkSession, appConfig.sink)).toLayer

  private def createSparkConf(storage: AppConfig.Storage): SparkConf =
    new SparkConf()
      .setAppName("MinIO example")
      .setMaster("local[*]")
      .set("spark.hadoop.fs.s3a.endpoint", storage.serviceEndpoint)
      .set("spark.hadoop.fs.s3a.access.key", storage.credentials.accessKey)
      .set("spark.hadoop.fs.s3a.secret.key", storage.credentials.accessKey)
      .set("spark.hadoop.fs.s3a.fast.upload", "true")
      .set("spark.hadoop.fs.s3a.path.style.access", "true")
      .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .set("spark.executor.memory", "10g")
      .set("spark.yarn.executor.memoryOverhead", "4096")
}
