package spark.jobs.processor

import zio._
import logstage.{LogIO, LogZIO}
import logstage.LogZIO.log
import org.apache.spark.sql
import org.apache.spark.sql.functions.desc
import spark.jobs.adapter.SparkWrapper
import spark.jobs.model.{BusinessCheckin, CheckinStats}
import spark.jobs.storage.DataSource
import zio.clock.{Clock, currentTime}

import java.util.concurrent.TimeUnit

final class BusinessCheckinJobs(sparkWrapper: SparkWrapper, dataSource: DataSource) {
  def start: ZIO[LogZIO with Clock, Throwable, Unit] =
    for {
      started           <- currentTime(TimeUnit.MILLISECONDS)
      checkinsDFFiber   <- dataSource.checkins.fork
      businessesDFFiber <- dataSource.businesses.fork

      checkinsDF   <- checkinsDFFiber.join
      businessesDF <- businessesDFFiber.join

      businessCheckinsFiber <- businessCheckins(businessesDF, checkinsDF).fork
      checkinStatsFiber     <- checkinStats(businessesDF, checkinsDF).fork

      _ <- businessCheckinsFiber.join
      _ <- checkinStatsFiber.join

      finished <- currentTime(TimeUnit.MILLISECONDS)
      total     = (finished - started) / 1000
      _        <- log.info(s"$getClass finished in ${total}s")
    } yield ()

  private def businessCheckins(businessDF: sql.DataFrame, checkinsDF: sql.DataFrame): Task[Unit] =
    sparkWrapper.withSession { sparkSession =>
      sparkWrapper.suspend {
        import sparkSession.implicits._

        businessDF
          .join(checkinsDF, businessDF.col("business_id") === checkinsDF.col("business_id"), "left_outer")
          .select("name", "city", "is_open", "review_count", "stars", "date")
          .filter(!_.anyNull)
          .map(BusinessCheckin.fromRow)
          .sort(desc("checkinCount"))
          .coalesce(1)
          .write
          .option("header", "true")
          .csv(sparkWrapper.destination("business_checkins"))
      }
    }

  private def checkinStats(businessDF: sql.DataFrame, checkinsDF: sql.DataFrame): ZIO[Any, Throwable, Unit] =
    sparkWrapper.withSession { sparkSession =>
      sparkWrapper.suspend {
        import sparkSession.implicits._

        val checkinStatsDF = checkinsDF
          .filter(!_.anyNull)
          .map(CheckinStats.fromRow)
          .withColumnRenamed("businessId", "business_id_c")

        checkinStatsDF
          .join(businessDF, checkinStatsDF.col("business_id_c") === businessDF.col("business_id"), "left_outer")
          .select("business_id", "city", "year", "month", "day", "hour")
          .coalesce(1)
          .write
          .option("header", "true")
          .csv(sparkWrapper.destination("checkin_stats"))
      }
    }
}

object BusinessCheckinJobs {
  lazy val live = (for {
    sparkWrapper <- ZIO.service[SparkWrapper]
    dataSource   <- ZIO.service[DataSource]
  } yield new BusinessCheckinJobs(sparkWrapper, dataSource)).toLayer
}
