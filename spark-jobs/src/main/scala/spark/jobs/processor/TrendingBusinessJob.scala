package spark.jobs.processor

import zio._
import zio.clock._
import logstage.LogZIO
import logstage.LogZIO.log
import org.apache.spark.sql
import spark.jobs.adapter.SparkWrapper
import spark.jobs.storage.DataSource
import spark.jobs.model.{Checkin, Review}
import java.util.concurrent.TimeUnit

final class TrendingBusinessJob(sparkWrapper: SparkWrapper, dataSource: DataSource) {
  def start: ZIO[LogZIO with Clock, Throwable, Unit] =
    for {
      started <- currentTime(TimeUnit.MILLISECONDS)

      reviewsDFFiber  <- dataSource.reviews.fork
      checkinsDFFiber <- dataSource.checkins.fork

      reviewsDF  <- reviewsDFFiber.join
      checkinsDF <- checkinsDFFiber.join

      _        <- trendingBusiness(reviewsDF, checkinsDF)
      finished <- currentTime(TimeUnit.MILLISECONDS)
      total     = (finished - started) / 1000
      _        <- log.info(s"$getClass finished in ${total}s")
    } yield ()

  private def trendingBusiness(
    reviewDF: sql.DataFrame,
    checkinsDF: sql.DataFrame
  ): Task[Unit] =
    for {
      reviewFilteredDFFiber <- sparkWrapper.withSession { sparkSession =>
                                 sparkWrapper.suspend {
                                   import sparkSession.implicits._

                                   reviewDF
                                     .select("business_id", "stars", "date")
                                     .filter(!_.anyNull)
                                     .map(Review.fromRow)
                                     .filter(review => review.stars > 3 && review.monthsAgo < 13)
                                     .coalesce(1)
                                     .write
                                     .option("header", "true")
                                     .csv(sparkWrapper.destination("trending_business_review"))
                                 }
                               }.fork
      checkinsFilteredDFFiber <- sparkWrapper.withSession { sparkSession =>
                                   sparkWrapper.suspend {
                                     import sparkSession.implicits._

                                     checkinsDF
                                       .filter(!_.anyNull)
                                       .map(Checkin.fromRow)
                                       .filter(_.checkins > 25)
                                       .coalesce(1)
                                       .write
                                       .option("header", "true")
                                       .csv(sparkWrapper.destination("trending_business_checkin"))
                                   }
                                 }.fork

      _ <- reviewFilteredDFFiber.join
      _ <- checkinsFilteredDFFiber.join
    } yield ()
}

object TrendingBusinessJob {
  lazy val live = (for {
    sparkWrapper <- ZIO.service[SparkWrapper]
    dataSource   <- ZIO.service[DataSource]
  } yield new TrendingBusinessJob(sparkWrapper, dataSource)).toLayer
}
