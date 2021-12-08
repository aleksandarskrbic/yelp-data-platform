package spark.jobs.processor

import logstage.LogZIO
import org.apache.spark.sql
import spark.jobs.service.DataLoader
import zio._
import org.apache.spark.sql.functions._
import zio.clock.Clock

final class BusinessProcessor(dataLoader: DataLoader) {
  def start: ZIO[LogZIO with Clock, Throwable, Exit[Throwable, Unit]] =
    for {
      df              <- dataLoader.businesses
      reviewsFiber    <- businessByReview(df).fork
      cityFiber       <- businessByCity(df).fork
      businessesFiber <- businessByIsOpen(df).fork
      result          <- ZIO.raceAll(reviewsFiber.await, List(cityFiber.await, businessesFiber.await))
    } yield result

  private def businessByReview(df: sql.DataFrame) =
    ZIO.effectSuspend {
      ZIO.effect {
        df.select("name", "city", "stars", "review_count")
          .sort(desc("stars"), desc("review_count"))
          .coalesce(1)
          .write
          .option("header", "true")
          .csv("s3a://yelp-processed/business_by_reviews")
      }
    }

  private def businessByCity(df: sql.DataFrame) =
    ZIO.effectSuspend {
      ZIO.effect {
        df.select("name", "city", "stars", "review_count")
          .groupBy("city")
          .count()
          .sort(desc("count"))
          .coalesce(1)
          .write
          .option("header", "true")
          .csv("s3a://yelp-processed/business_by_city")
      }
    }

  private def businessByIsOpen(df: sql.DataFrame) =
    ZIO.effectSuspend {
      ZIO.effect {
        df.select("name", "city", "is_open")
          .groupBy("is_open")
          .count()
          .coalesce(1)
          .write
          .option("header", "true")
          .csv("s3a://yelp-processed/business_by_is_open")
      }
    }
}

object BusinessProcessor {
  lazy val live = (for {
    //sparkWrapper <- ZIO.service[SparkWrapper]
    dataLoader <- ZIO.service[DataLoader]
  } yield new BusinessProcessor(dataLoader)).toLayer
}
