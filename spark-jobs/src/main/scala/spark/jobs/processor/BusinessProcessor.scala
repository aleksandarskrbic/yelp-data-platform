package spark.jobs.processor

import logstage.LogZIO
import org.apache.spark.sql
import org.apache.spark.sql.Row
import spark.jobs.service.DataLoader
import zio._
import org.apache.spark.sql.functions._
import spark.jobs.adapter.SparkWrapper
import spark.jobs.model.BusinessCheckin
import zio.clock.Clock
import zio.stream.ZStream

final class BusinessProcessor(sparkWrapper: SparkWrapper, dataLoader: DataLoader) {
  def start =
    for {
      businessesDFFiber <- dataLoader.businesses.fork
      checkinsDFFiber   <- dataLoader.checkins.fork
      reviewsDFFiber    <- dataLoader.reviews.fork

      businessesDF <- businessesDFFiber.join

      cityFiber       <- businessByCity(businessesDF).fork
      reviewsFiber    <- businessByReview(businessesDF).fork
      businessesFiber <- businessByIsOpen(businessesDF).fork

      checkinsDF <- checkinsDFFiber.join

      businessCheckinsFiber <- businessCheckins(businessesDF, checkinsDF).fork
      _                     <- businessCheckinsFiber.join

      reviewsDF <- reviewsDFFiber.join

      result <- ZIO.raceAll(
                  reviewsFiber.await,
                  Chunk(
                    cityFiber.await,
                    businessesFiber.await,
                    businessCheckinsFiber.await
                  )
                )
    } yield ()

  private def businessByReview(df: sql.DataFrame) =
    sparkWrapper.suspend {
      df.select("name", "city", "stars", "review_count")
        .sort(desc("stars"), desc("review_count"))
        .coalesce(1)
        .write
        .option("header", "true")
        .csv("s3a://yelp-processed/business_by_reviews")
    }

  private def businessByCity(df: sql.DataFrame) =
    sparkWrapper.suspend {
      df.select("name", "city", "stars", "review_count")
        .groupBy("city")
        .count()
        .sort(desc("count"))
        .coalesce(1)
        .write
        .option("header", "true")
        .csv("s3a://yelp-processed/business_by_city")
    }

  private def businessByIsOpen(df: sql.DataFrame) =
    sparkWrapper.suspend {
      df.select("name", "city", "is_open")
        .groupBy("is_open")
        .count()
        .coalesce(1)
        .write
        .option("header", "true")
        .csv("s3a://yelp-processed/business_by_is_open")
    }

  private def businessCheckins(businessDF: sql.DataFrame, checkinsDF: sql.DataFrame) =
    for {
      rdd <- sparkWrapper.suspend {
               businessDF
                 .join(checkinsDF, businessDF.col("business_id") === checkinsDF.col("business_id"), "left_outer")
                 .select("name", "city", "is_open", "review_count", "stars", "date")
                 .rdd
                 .filter(!_.anyNull)
                 .map(BusinessCheckin.fromRow)
             }
      _ <- sparkWrapper.withSession { sparkSession =>
             sparkWrapper.suspend {
               sparkSession
                 .createDataFrame(rdd)
                 .toDF("name", "city", "is_open", "review_count", "stars", "checkin_count")
                 .sort(desc("checkin_count"))
                 .coalesce(1)
                 .write
                 .option("header", "true")
                 .csv("s3a://yelp-processed/business_checkins")
             }
           }
    } yield ()
}

object BusinessProcessor {
  lazy val live = (for {
    sparkWrapper <- ZIO.service[SparkWrapper]
    dataLoader   <- ZIO.service[DataLoader]
  } yield new BusinessProcessor(sparkWrapper, dataLoader)).toLayer
}
