package spark.jobs.processor

import zio._
import logstage.LogZIO
import logstage.LogZIO.log
import zio.clock.{Clock, currentTime}
import spark.jobs.storage.DataSource
import spark.jobs.adapter.SparkWrapper
import org.apache.spark.sql
import org.apache.spark.sql.functions.desc
import java.util.concurrent.TimeUnit

final class BusinessJob(sparkWrapper: SparkWrapper, dataSource: DataSource) {
  def start: ZIO[LogZIO with Clock, Throwable, Unit] =
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
      _        <- log.info(s"${getClass.getCanonicalName} finished in ${total}s")
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

object BusinessJob {
  lazy val live = (for {
    sparkWrapper <- ZIO.service[SparkWrapper]
    dataSource   <- ZIO.service[DataSource]
  } yield new BusinessJob(sparkWrapper, dataSource)).toLayer
}
