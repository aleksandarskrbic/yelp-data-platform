package spark.jobs.processor

import zio._
import zio.clock._
import logstage.LogZIO
import logstage.LogZIO.log
import spark.jobs.storage.DataSource
import spark.jobs.adapter.spark.SparkWrapper
import java.util.concurrent.TimeUnit

final class ReviewRecommenderJob(sparkWrapper: SparkWrapper, dataSource: DataSource) {
  def start: ZIO[LogZIO with Clock, Throwable, Unit] =
    for {
      started   <- currentTime(TimeUnit.MILLISECONDS)
      reviewsDF <- dataSource.reviews
      splitted  <- sparkWrapper.suspend(reviewsDF.randomSplit(Array(0.5, 0.5), 42))

      trainDF = splitted.head
      testDF  = splitted.last

      finished <- currentTime(TimeUnit.MILLISECONDS)

      total = (finished - started) / 1000
      _    <- log.info(s"$getClass finished in ${total}s")
    } yield ()

}

object ReviewRecommenderJob {
  lazy val live = (for {
    sparkWrapper <- ZIO.service[SparkWrapper]
    dataSource   <- ZIO.service[DataSource]
  } yield new ReviewRecommenderJob(sparkWrapper, dataSource)).toLayer
}
