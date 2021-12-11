package spark.jobs.processor

import zio._
import zio.clock._
import logstage.LogZIO
import logstage.LogZIO.log
import spark.jobs.adapter.SparkWrapper
import spark.jobs.model.WordCount
import spark.jobs.storage.DataSource
import org.apache.spark.sql
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.StopWordsRemover
import java.util.concurrent.TimeUnit

final class ReviewJobs(sparkWrapper: SparkWrapper, dataSource: DataSource) {
  def start: ZIO[LogZIO with Clock, Throwable, Unit] =
    for {
      started   <- currentTime(TimeUnit.MILLISECONDS)
      reviewsDF <- dataSource.reviews
      trf       <- topReviews(reviewsDF).fork
      wrf       <- worstReviews(reviewsDF).fork
      _         <- trf.join
      _         <- wrf.join
      finished  <- currentTime(TimeUnit.MILLISECONDS)
      total      = (finished - started) / 1000
      _         <- log.info(s"$getClass finished in ${total}s")
    } yield ()

  private def topReviews(reviewDF: sql.DataFrame): Task[Unit] =
    wordCounts(reviewDF, topReviews = true)

  private def worstReviews(reviewDF: sql.DataFrame): Task[Unit] =
    wordCounts(reviewDF, topReviews = false)

  private def wordCounts(reviewDF: sql.DataFrame, topReviews: Boolean): Task[Unit] = {
    val stops = StopWordsRemover.loadDefaultStopWords("english")

    val filterExp =
      if (topReviews) "stars > 3"
      else "stars <= 3"

    val path =
      if (topReviews) sparkWrapper.destination("top_reviews")
      else sparkWrapper.destination("worst_reviews")

    for {
      _ <- sparkWrapper.withSession { sparkSession =>
             sparkWrapper.suspend {
               import sparkSession.implicits._

               reviewDF
                 .select("text", "stars")
                 .filter(filterExp)
                 .map(row => row(0).asInstanceOf[String].replaceAll("\\W+", " ").toLowerCase)
                 .flatMap(_.split(" "))
                 .filter(!stops.contains(_))
                 .map(word => WordCount(word))
                 .groupBy("word")
                 .agg(sum($"count"))
                 .sort(col("sum(count)").desc)
                 .coalesce(1)
                 .write
                 .option("header", "true")
                 .csv(path)
             }
           }
    } yield ()
  }
}

object ReviewJobs {
  lazy val live = (for {
    sparkWrapper <- ZIO.service[SparkWrapper]
    dataSource   <- ZIO.service[DataSource]
  } yield new ReviewJobs(sparkWrapper, dataSource)).toLayer
}
