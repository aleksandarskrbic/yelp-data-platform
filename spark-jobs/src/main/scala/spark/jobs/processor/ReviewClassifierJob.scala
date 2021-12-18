package spark.jobs.processor

import zio._
import zio.clock._
import logstage.LogZIO
import logstage.LogZIO.log
import spark.jobs.adapter.spark.ml._
import spark.jobs.storage.DataSource
import java.util.concurrent.TimeUnit
import org.apache.spark.ml.Pipeline
import spark.jobs.adapter.spark.SparkWrapper
import spark.jobs.adapter.spark.ml.SparkMLComponent

final class ReviewClassifierJob(sparkWrapper: SparkWrapper, dataSource: DataSource) {
  def start: ZIO[LogZIO with Clock, Throwable, Unit] =
    for {
      started   <- currentTime(TimeUnit.MILLISECONDS)
      reviewsDF <- dataSource.reviews()

      parts <- sparkWrapper.suspend(reviewsDF.randomSplit(Array(0.5, 0.5), 42))
      model <- sparkWrapper.suspend(pipeline.fit(parts.head))
      _ <- sparkWrapper.suspend {
             model.write
               .overwrite()
               .save(sparkWrapper.destination("review_classifier_model"))
           }

      finished <- currentTime(TimeUnit.MILLISECONDS)
      total     = (finished - started) / 1000
      _        <- log.info(s"$getClass finished in ${total}s")
    } yield ()

  lazy val pipeline: Pipeline =
    SparkMLComponent.tokenizer(
      inputCol = "text",
      outputCol = "tokenized"
    ) ~> SparkMLComponent.stopWordsRemover(
      inputCol = "tokenized"
    ) ~> SparkMLComponent.binarizer(
      inputCol = "stars",
      outputCol = "positive"
    ) ~> SparkMLComponent.hashingTF(
      inputCol = "tokenized",
      outputCol = "tf_features"
    ) ~> SparkMLComponent.idf(
      inputCol = "tf_features",
      outputCol = "features"
    ) >-> SparkMLComponent.logisticRegression(
      featuresCol = "features",
      labelCol = "positive",
      maxIterations = 80
    )
}

object ReviewClassifierJob {
  lazy val live = (for {
    sparkWrapper <- ZIO.service[SparkWrapper]
    dataSource   <- ZIO.service[DataSource]
  } yield new ReviewClassifierJob(sparkWrapper, dataSource)).toLayer
}
