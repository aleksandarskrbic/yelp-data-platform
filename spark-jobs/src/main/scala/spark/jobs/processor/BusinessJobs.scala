package spark.jobs.processor

import logstage.LogZIO.log
import org.apache.spark.sql
import org.apache.spark.sql.functions.desc
import spark.jobs.adapter.SparkWrapper
import spark.jobs.common.{AppConfig, Logging}
import spark.jobs.storage.DataSource
import zio._
import zio.clock.currentTime
import zio.magic._

import java.util.concurrent.TimeUnit

final class BusinessJobs(sparkWrapper: SparkWrapper, dataSource: DataSource) {
  def start =
    for {
      started    <- currentTime(TimeUnit.MILLISECONDS)
      businessDF <- dataSource.businesses

      businessByReviewFiber <- businessByReview(businessDF).fork
      businessByCityFiber   <- businessByCity(businessDF).fork
      businessByIsOpenFiber <- businessByIsOpen(businessDF).fork

      _ <- businessByReviewFiber.await
      _ <- businessByCityFiber.await
      _ <- businessByIsOpenFiber.await

      finished <- currentTime(TimeUnit.MILLISECONDS)
      total     = (finished - started) / 1000
      _        <- log.info(s"$getClass finished in ${total}s")
    } yield ()

  private def businessByReview(df: sql.DataFrame): Task[Unit] =
    sparkWrapper.suspend {
      df.select("name", "city", "stars", "review_count")
        .sort(desc("stars"), desc("review_count"))
        .coalesce(1)
        .write
        .option("header", "true")
        .csv(sparkWrapper.destination("business_by_reviews"))
    }

  private def businessByCity(df: sql.DataFrame): Task[Unit] =
    sparkWrapper.suspend {
      df.select("name", "city", "stars", "review_count")
        .groupBy("city")
        .count()
        .sort(desc("count"))
        .coalesce(1)
        .write
        .option("header", "true")
        .csv(sparkWrapper.destination("business_by_city"))
    }

  private def businessByIsOpen(df: sql.DataFrame): Task[Unit] =
    sparkWrapper.suspend {
      df.select("name", "city", "is_open")
        .groupBy("is_open")
        .count()
        .coalesce(1)
        .write
        .option("header", "true")
        .csv(sparkWrapper.destination("business_by_is_open"))
    }
}

object BusinessJobs {
  lazy val live = (for {
    sparkWrapper <- ZIO.service[SparkWrapper]
    dataSource   <- ZIO.service[DataSource]
  } yield new BusinessJobs(sparkWrapper, dataSource)).toLayer
}
