package spark.jobs.processor

import zio._
import zio.clock._
import logstage.LogZIO
import logstage.LogZIO.log
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.sql
import spark.jobs.adapter.spark.SparkWrapper
import spark.jobs.adapter.spark.ml.SparkMLComponent
import spark.jobs.adapter.spark.ml._
import spark.jobs.storage.DataSource

import java.util.concurrent.TimeUnit

class ReviewClassifierJob(sparkWrapper: SparkWrapper, dataSource: DataSource) {
  def start: ZIO[LogZIO with Clock, Throwable, Unit] =
    for {
      started   <- currentTime(TimeUnit.MILLISECONDS)
      reviewsDF <- dataSource.reviews
      splitted  <- sparkWrapper.suspend(reviewsDF.randomSplit(Array(0.5, 0.5), 42))

      trainDF = splitted.head
      testDF  = splitted.last

      model <- sparkWrapper.suspend(pipeline.fit(trainDF))
      _ <- sparkWrapper.suspend {
             model
               .transform(testDF)
               .show(100)
           }
      finished <- currentTime(TimeUnit.MILLISECONDS)

      total = (finished - started) / 1000
      _    <- log.info(s"$getClass finished in ${total}s")
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
    ) >=> SparkMLComponent.logisticRegression(
      featuresCol = "features",
      labelCol = "positive",
      maxIterations = 80
    )

  private def logMetrics(model: PipelineModel, testDF: sql.DataFrame): Unit = {
    val out = model
      .transform(testDF)
      .select("prediction", "positive")

    val a = out.rdd
      .map(x => (x(0).asInstanceOf[Double], x(1).asInstanceOf[Double]))

    import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
    val binaryClassificationMetrics = new BinaryClassificationMetrics(a)

    binaryClassificationMetrics.precisionByThreshold.foreach { case (t, p) =>
      println(s"Threshold: $t, Precision: $p")
    }

    binaryClassificationMetrics.recallByThreshold.foreach { case (t, r) =>
      println(s"Threshold: $t, Recall: $r")
    }

  }
}

object ReviewClassifierJob {
  lazy val live = (for {
    sparkWrapper <- ZIO.service[SparkWrapper]
    dataSource   <- ZIO.service[DataSource]
  } yield new ReviewClassifierJob(sparkWrapper, dataSource)).toLayer
}
