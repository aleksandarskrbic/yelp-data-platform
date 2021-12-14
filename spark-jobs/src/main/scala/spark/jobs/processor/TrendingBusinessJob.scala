package spark.jobs.processor

import zio._
import zio.clock._
import logstage.LogZIO
import logstage.LogZIO.log
import org.apache.spark.sql
import org.apache.spark.sql.functions.desc
import spark.jobs.adapter.SparkWrapper
import spark.jobs.common.AppConfig
import spark.jobs.common.AppConfig.S3Path
import spark.jobs.model.{Checkin, Review}
import spark.jobs.storage.{DataSource, FileRepository}
import java.util.concurrent.TimeUnit

final class TrendingBusinessJob(
  sparkWrapper: SparkWrapper,
  dataSource: DataSource,
  fileRepository: FileRepository,
  storage: AppConfig.Storage
) {
  def start: ZIO[LogZIO with Clock, Throwable, Unit] =
    for {
      started <- currentTime(TimeUnit.MILLISECONDS)

      reviewsDFFiber  <- dataSource.reviews.fork
      checkinsDFFiber <- dataSource.checkins.fork
      businessDFFiber <- dataSource.businesses.fork

      reviewsDF  <- reviewsDFFiber.join
      checkinsDF <- checkinsDFFiber.join
      businessDF <- businessDFFiber.join

      _        <- trendingBusiness(reviewsDF, checkinsDF, businessDF)
      finished <- currentTime(TimeUnit.MILLISECONDS)
      total     = (finished - started) / 1000
      _        <- log.info(s"job -> ${getClass.getCanonicalName} finished in ${total}s")
    } yield ()

  private def trendingBusiness(
    reviewDF: sql.DataFrame,
    checkinsDF: sql.DataFrame,
    businessDF: sql.DataFrame
  ): ZIO[LogZIO with Clock, Throwable, Unit] =
    for {
      reviewFilteredDFFiber <- sparkWrapper.withSession { sparkSession =>
                                 import sparkSession.implicits._

                                 sparkWrapper.suspend {
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
                                   import sparkSession.implicits._

                                   sparkWrapper.suspend {
                                     checkinsDF
                                       .filter(!_.anyNull)
                                       .map(Checkin.fromRow)
                                       .filter(_.checkins > 30)
                                       .withColumnRenamed("businessId", "business_id_c")
                                   }
                                 }.fork

      _ <- reviewFilteredDFFiber.join

      trendingReviewsPath <- fileRepository.listBucket(storage.bucket, "trending_business_review")

      fullPath = S3Path(s"s3a://${storage.bucket}/$trendingReviewsPath")

      _ <- log.info(s"$fullPath")

      trendingReviewsDF <- sparkWrapper.read(fullPath, "csv", Map("header" -> "true"))
      aggregatedReviews <- sparkWrapper.suspend {
                             trendingReviewsDF
                               .groupBy("businessId")
                               .count()
                               .filter("count > 5")
                               .withColumnRenamed("businessId", "business_id_r")
                               .withColumnRenamed("count", "positive_reviews")
                           }

      checkinsFilteredDF <- checkinsFilteredDFFiber.join

      _ <- sparkWrapper.suspend {
             aggregatedReviews
               .join(
                 checkinsFilteredDF,
                 aggregatedReviews.col("business_id_r") === checkinsFilteredDF.col("business_id_c"),
                 "left_outer"
               )
               .join(
                 businessDF.select("business_id", "name", "city"),
                 aggregatedReviews.col("business_id_r") === businessDF("business_id"),
                 "left_outer"
               )
               .select("business_id", "name", "city", "positive_reviews", "checkins")
               .filter(!_.anyNull)
               .sort(desc("positive_reviews"), desc("checkins"))
               .coalesce(1)
               .write
               .option("header", "true")
               .csv(sparkWrapper.destination("trending_businesses"))

           }

      _ <- fileRepository.deleteFiles(storage.bucket, "trending_business_review")
    } yield ()
}

object TrendingBusinessJob {
  lazy val live = (for {
    sparkWrapper   <- ZIO.service[SparkWrapper]
    dataSource     <- ZIO.service[DataSource]
    fileRepository <- ZIO.service[FileRepository]
    appConfig      <- ZIO.service[AppConfig]
  } yield new TrendingBusinessJob(sparkWrapper, dataSource, fileRepository, appConfig.storage)).toLayer
}
