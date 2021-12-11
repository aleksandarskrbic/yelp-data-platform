package spark.jobs.processor

import logstage.LogZIO
import logstage.LogZIO.log
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

final class JobsManager(
  businessJobs: BusinessJobs,
  businessCheckinJobs: BusinessCheckinJobs,
  reviewJobs: ReviewJobs,
  userJobs: UserJobs,
  trendingBusinessJobs: TrendingBusinessJobs
) {
  def start =
    for {
      userJobsFiber         <- userJobs.start.fork
      businessJobsFiber     <- businessJobs.start.fork
      reviewJobs            <- reviewJobs.start.fork
      businessCheckinsFiber <- businessCheckinJobs.start.fork
      //_ <- trendingBusinessJobs.start

      _ <- userJobsFiber.join
      _ <- businessJobsFiber.join
      _ <- reviewJobs.join
      _ <- businessCheckinsFiber.join
    } yield ()

  /*  def start1 =
    for {
      businessesDFFiber <- dataSource.businesses.fork
      checkinsDFFiber   <- dataSource.checkins.fork
      reviewsDFFiber    <- dataSource.reviews.fork
      userDFFiber       <- dataSource.users.fork

      businessesDF <- businessesDFFiber.join

      cityFiber       <- businessJobs.businessByCity(businessesDF).fork
      reviewsFiber    <- businessJobs.businessByReview(businessesDF).fork
      businessesFiber <- businessJobs.businessByIsOpen(businessesDF).fork

      _ <- ZIO.foreach_(Chunk(cityFiber, reviewsFiber, businessesFiber))(_.join)
      _ <- log.info("First batch finished")

      usersDF <- userDFFiber.join

      userDetailsFiber <- userJobs.userDetails(usersDF).fork

      checkinsDF <- checkinsDFFiber.join

      businessCheckinsFiber <- businessJobs.businessCheckins(businessesDF, checkinsDF).fork
      checkinStatsFiber     <- businessJobs.checkinStats(businessesDF, checkinsDF).fork

      _ <- ZIO.foreach_(Chunk(userDetailsFiber, businessCheckinsFiber, checkinStatsFiber))(_.join)
      _ <- log.info("Second batch finished")

      reviewsDF <- reviewsDFFiber.join

      trendingBusinessesFiber <- businessJobs.trendingBusiness(businessesDF, checkinsDF, reviewsDF).fork
      topReviewsFiber         <- reviewJobs.topReviews(reviewsDF).fork
      worstReviewsFiber       <- reviewJobs.worstReviews(reviewsDF).fork

      _ <- ZIO.foreach_(Chunk(trendingBusinessesFiber, topReviewsFiber, worstReviewsFiber))(_.join)
      _ <- log.info("Third batch finished")
    } yield ()*/
}

object JobsManager {
  lazy val live = (for {
    businessJobs         <- ZIO.service[BusinessJobs]
    businessCheckinJobs  <- ZIO.service[BusinessCheckinJobs]
    reviewJobs           <- ZIO.service[ReviewJobs]
    userJobs             <- ZIO.service[UserJobs]
    trendingBusinessJobs <- ZIO.service[TrendingBusinessJobs]
  } yield new JobsManager(
    businessJobs,
    businessCheckinJobs,
    reviewJobs,
    userJobs,
    trendingBusinessJobs
  )).toLayer
}
