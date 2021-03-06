package spark.jobs.processor

import zio._
import zio.clock._
import logstage.LogZIO
import logstage.LogZIO.log
import spark.jobs.model.WordCount
import spark.jobs.storage.DataSource
import org.apache.spark.sql
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.StopWordsRemover
import spark.jobs.adapter.spark.SparkWrapper

import java.util.concurrent.TimeUnit

final class ReviewJob(sparkWrapper: SparkWrapper, dataSource: DataSource) {
  def start: ZIO[LogZIO with Clock, Throwable, Unit] =
    for {
      started   <- currentTime(TimeUnit.MILLISECONDS)
      reviewsDF <- dataSource.reviews()

      topReviewsFiber   <- topReviews(reviewsDF).fork
      worstReviewsFiber <- worstReviews(reviewsDF).fork

      _ <- topReviewsFiber.join
      _ <- worstReviewsFiber.join

      finished <- currentTime(TimeUnit.MILLISECONDS)

      total = (finished - started) / 1000
      _    <- log.info(s"$getClass finished in ${total}s")
    } yield ()

  private def topReviews(reviewDF: sql.DataFrame): Task[Unit] =
    wordCounts(reviewDF, topReviews = true)

  private def worstReviews(reviewDF: sql.DataFrame): Task[Unit] =
    wordCounts(reviewDF, topReviews = false)

  private def wordCounts(reviewDF: sql.DataFrame, topReviews: Boolean): Task[Unit] = {
    val stops = StopWordsRemover.loadDefaultStopWords("english")

    val filterExp = if (topReviews) "stars > 3" else "stars <= 3"

    val path =
      if (topReviews) sparkWrapper.destination("top_reviews")
      else sparkWrapper.destination("worst_reviews")

    for {
      _ <- sparkWrapper.withSession { sparkSession =>
             import sparkSession.implicits._

             sparkWrapper.suspend {
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

object ReviewJob {
  lazy val live = (for {
    sparkWrapper <- ZIO.service[SparkWrapper]
    dataSource   <- ZIO.service[DataSource]
  } yield new ReviewJob(sparkWrapper, dataSource)).toLayer
}
