package spark.jobs.processor

import zio._
import zio.clock._
import logstage.LogZIO
import logstage.LogZIO.log
import spark.jobs.model.UserDetails
import spark.jobs.storage.DataSource
import spark.jobs.adapter.SparkWrapper
import java.util.concurrent.TimeUnit

final class UserJob(sparkWrapper: SparkWrapper, dataSource: DataSource) {
  def start: ZIO[LogZIO with Clock, Throwable, Unit] =
    for {
      started <- currentTime(TimeUnit.MILLISECONDS)
      userDF  <- dataSource.users
      _ <- sparkWrapper.withSession { sparkSession =>
             import sparkSession.implicits._

             sparkWrapper.suspend {
               userDF
                 .select("user_id", "review_count", "average_stars", "yelping_since", "friends")
                 .filter(!_.anyNull)
                 .map(UserDetails.fromRow)
                 .coalesce(1)
                 .write
                 .option("header", "true")
                 .csv(sparkWrapper.destination("user_details"))
             }
           }
      finished <- currentTime(TimeUnit.MILLISECONDS)
      total     = (finished - started) / 1000
      _        <- log.info(s"${getClass.getCanonicalName} finished in ${total}s")
    } yield ()
}

object UserJob {
  lazy val live = (for {
    sparkWrapper <- ZIO.service[SparkWrapper]
    dataSource   <- ZIO.service[DataSource]
  } yield new UserJob(sparkWrapper, dataSource)).toLayer
}
