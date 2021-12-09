package spark.jobs.processor

import zio._
import org.apache.spark.sql
import org.apache.spark.sql.functions.desc
import spark.jobs.adapter.SparkWrapper
import spark.jobs.model.BusinessCheckin

class BusinessJob(sparkWrapper: SparkWrapper) {
  def businessByReview(df: sql.DataFrame): Task[Unit] =
    sparkWrapper.suspend {
      df.select("name", "city", "stars", "review_count")
        .sort(desc("stars"), desc("review_count"))
        .coalesce(1)
        .write
        .option("header", "true")
        .csv("s3a://yelp-processed/business_by_reviews")
    }

  def businessByCity(df: sql.DataFrame): Task[Unit] =
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

  def businessByIsOpen(df: sql.DataFrame): Task[Unit] =
    sparkWrapper.suspend {
      df.select("name", "city", "is_open")
        .groupBy("is_open")
        .count()
        .coalesce(1)
        .write
        .option("header", "true")
        .csv(sparkWrapper.destination("business_by_is_open"))
    }

  def businessCheckins(businessDF: sql.DataFrame, checkinsDF: sql.DataFrame): Task[Unit] =
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
                 .csv(sparkWrapper.destination("business_checkins"))
             }
           }
    } yield ()
}

object BusinessJob {
  lazy val live = (for {
    sparkWrapper <- ZIO.service[SparkWrapper]
  } yield new BusinessJob(sparkWrapper)).toLayer
}
