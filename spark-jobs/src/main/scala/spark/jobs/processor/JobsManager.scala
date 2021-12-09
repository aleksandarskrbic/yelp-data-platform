package spark.jobs.processor

import logstage.LogZIO
import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.sql
import org.apache.spark.sql.Row
import zio._
import org.apache.spark.sql.functions._
import spark.jobs.adapter.SparkWrapper
import spark.jobs.model.BusinessCheckin
import spark.jobs.storage.DataSource
import zio.clock.Clock
import zio.stream.ZStream

final class JobsManager(sparkWrapper: SparkWrapper, dataLoader: DataSource) {
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

  private def wordCounts(reviewDF: sql.DataFrame, topReviews: Boolean) {
    val stops = StopWordsRemover.loadDefaultStopWords("english")

    val filterExp = if (topReviews) "stars > 3" else "stars <= 3"
    val path      = if (topReviews) "data/topReviews" else "data/worstReviews"

    for {
      rdd <- sparkWrapper.suspend {
               reviewDF
                 .select("text", "stars")
                 .filter(filterExp)
                 .rdd
                 .map(row => row(0).asInstanceOf[String].replaceAll("\\W+", " ").toLowerCase())
                 .flatMap(_.split(" "))
                 .filter(!stops.contains(_))
                 .map((_, 1))
                 .reduceByKey(_ + _)
                 .sortBy(_._2, ascending = false)
                 .take(100)
             }
      _ <- sparkWrapper.withSession { sparkSession =>
             sparkWrapper.suspend {
               sparkSession
                 .createDataFrame(rdd)
                 .toDF("word", "count")
                 .coalesce(1)
                 .write
                 .option("header", "true")
                 .csv(path)
             }
           }
    } yield ()
  }
}

object JobsManager {
  lazy val live = (for {
    sparkWrapper <- ZIO.service[SparkWrapper]
    dataLoader   <- ZIO.service[DataSource]
  } yield new JobsManager(sparkWrapper, dataLoader)).toLayer
}
