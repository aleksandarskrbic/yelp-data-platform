package spark.jobs.processor

import logstage.LogZIO.log
import zio.ZIO
import spark.jobs.model.UserDetails
import spark.jobs.adapter.SparkWrapper
import spark.jobs.storage.DataSource
import zio.clock.currentTime

import java.util.concurrent.TimeUnit

final class UserJobs(sparkWrapper: SparkWrapper, dataSource: DataSource) {
  def start =
    for {
      started <- currentTime(TimeUnit.MILLISECONDS)
      userDF  <- dataSource.users
      _ <- sparkWrapper.withSession { sparkSession =>
             sparkWrapper.suspend {
               import sparkSession.implicits._

               userDF
                 .select("user_id", "review_count", "average_stars", "yelping_since", "friends")
                 .filter(!_.anyNull)
                 .map(UserDetails.fromRow)
                 .toDF("user_id", "review_count", "average_stars", "yelping_for", "friends")
                 .coalesce(1)
                 .write
                 .option("header", "true")
                 .csv(sparkWrapper.destination("user_details"))
             }
           }
      finished <- currentTime(TimeUnit.MILLISECONDS)
      total     = (finished - started) / 1000
      _        <- log.info(s"$getClass finished in ${total}s")
    } yield ()
}

object UserJobs {
  lazy val live = (for {
    sparkWrapper <- ZIO.service[SparkWrapper]
    dataSource   <- ZIO.service[DataSource]
  } yield new UserJobs(sparkWrapper, dataSource)).toLayer
}
