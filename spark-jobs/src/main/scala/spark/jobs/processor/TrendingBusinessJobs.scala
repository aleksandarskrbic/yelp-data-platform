package spark.jobs.processor

import logstage.LogZIO.log
import org.apache.spark.sql
import org.apache.spark.sql.functions.desc
import spark.jobs.adapter.SparkWrapper
import spark.jobs.model.{Checkin, Review}
import spark.jobs.storage.DataSource
import zio.ZIO
import zio.clock.currentTime

import java.util.concurrent.TimeUnit

final class TrendingBusinessJobs(sparkWrapper: SparkWrapper, dataSource: DataSource) {
  def start =
    for {
      reviewsDFFiber    <- dataSource.reviews.fork
      checkinsDFFiber   <- dataSource.checkins.fork
      businessesDFFiber <- dataSource.businesses.fork

      reviewsDF    <- reviewsDFFiber.join
      checkinsDF   <- checkinsDFFiber.join
      businessesDF <- businessesDFFiber.join

      result <- trendingBusiness(reviewsDF, businessesDF, checkinsDF)
    } yield result

  private def trendingBusiness(
    reviewDF: sql.DataFrame,
    businessDF: sql.DataFrame,
    checkinsDF: sql.DataFrame
  ) =
    for {
      started <- currentTime(TimeUnit.MILLISECONDS)
      reviewFilteredDFFiber <- sparkWrapper.withSession { sparkSession =>
                                 sparkWrapper.suspend {
                                   import sparkSession.implicits._

                                   reviewDF
                                     .filter("stars > 3")
                                     .select("review_id", "business_id", "stars", "date")
                                     .filter(!_.anyNull)
                                     .map(Review.fromRow)
                                     .filter(_.monthsAgo < 13)
                                     .withColumnRenamed("businessId", "business_id")
                                     .groupBy("business_id")
                                     .count()
                                     .filter("count > 100")
                                     .withColumnRenamed("count", "positive_reviews")
                                     .withColumnRenamed("business_id", "business_id_r")
                                 }
                               }.fork

      checkinsFilteredDFFiber <- sparkWrapper.withSession { sparkSession =>
                                   sparkWrapper.suspend {
                                     import sparkSession.implicits._

                                     checkinsDF
                                       .filter(!_.anyNull)
                                       .map(Checkin.fromRow)
                                       .filter(_.checkins > 12)
                                       .withColumnRenamed("businessId", "business_id_c")
                                   }
                                 }.fork

      reviewFilteredDF   <- reviewFilteredDFFiber.join
      checkinsFilteredDF <- checkinsFilteredDFFiber.join

      _ <- sparkWrapper.suspend {
             reviewFilteredDF
               .join(
                 checkinsFilteredDF,
                 reviewFilteredDF.col("business_id_r") === checkinsFilteredDF.col("business_id_c"),
                 "left_outer"
               )
               .join(
                 businessDF.select("business_id", "name", "city"),
                 reviewFilteredDF.col("business_id_r") === businessDF("business_id"),
                 "left_outer"
               )
               .select("name", "city", "positive_reviews", "checkins")
               //.sort(desc("checkins"), desc("positive_reviews"))
               .coalesce(1)
               .write
               .option("header", "true")
               .csv(sparkWrapper.destination("trending_business"))
           }
      finished <- currentTime(TimeUnit.MILLISECONDS)
      total     = (finished - started) / 1000
      _        <- log.info(s"$getClass finished in ${total}s")
    } yield ()
}

object TrendingBusinessJobs {
  lazy val live = (for {
    sparkWrapper <- ZIO.service[SparkWrapper]
    dataSource   <- ZIO.service[DataSource]
  } yield new TrendingBusinessJobs(sparkWrapper, dataSource)).toLayer
}
