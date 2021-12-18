package spark.jobs.processor

import zio._
import zio.clock._
import logstage.LogZIO
import logstage.LogZIO.log
import org.apache.spark.ml.Pipeline
import spark.jobs.storage.DataSource
import spark.jobs.adapter.spark.SparkWrapper
import spark.jobs.adapter.spark.ml.SparkMLComponent
import java.util.concurrent.TimeUnit

final class ReviewRecommenderJob(sparkWrapper: SparkWrapper, dataSource: DataSource) {
  def start: ZIO[LogZIO with Clock, Throwable, Unit] =
    for {
      started   <- currentTime(TimeUnit.MILLISECONDS)
      reviewsDF <- dataSource.reviews(limit = Some(100000))
      model     <- sparkWrapper.suspend(pipeline.fit(reviewsDF))
      _ <- sparkWrapper.suspend {
             model.write
               .overwrite()
               .save(sparkWrapper.destination("model/review_recommender_model"))
           }
      finished <- currentTime(TimeUnit.MILLISECONDS)

      total = (finished - started) / 1000
      _    <- log.info(s"$getClass finished in ${total}s")
    } yield ()

  lazy val pipeline: Pipeline =
    SparkMLComponent.stringIndexer(
      inputCol = "user_id",
      outputCol = "user_id_indexed"
    ) ~> SparkMLComponent.stringIndexer(
      inputCol = "business_id",
      outputCol = "business_id_indexed"
    ) >-> SparkMLComponent.alsRecommender(
      maxIteration = 8,
      userCol = "user_id_indexed",
      itemCol = "business_id_indexed",
      ratingCol = "stars"
    )
}

object ReviewRecommenderJob {
  lazy val live = (for {
    sparkWrapper <- ZIO.service[SparkWrapper]
    dataSource   <- ZIO.service[DataSource]
  } yield new ReviewRecommenderJob(sparkWrapper, dataSource)).toLayer
}
