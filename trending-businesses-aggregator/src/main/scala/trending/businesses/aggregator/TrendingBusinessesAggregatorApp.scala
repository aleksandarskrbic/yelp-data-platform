package trending.businesses.aggregator

import trending.businesses.aggregator.common.{AppConfig, Logging}
import trending.businesses.aggregator.storage.{S3Client, S3ClientWrapper}
import zio._
import zio.magic._

object TrendingBusinessesAggregatorApp extends zio.App {
  override def run(args: List[String]): URIO[ZEnv, ExitCode] =
    (for {
      s3Client            <- ZIO.service[S3Client]
      appConfig           <- ZIO.service[AppConfig]
      trendingReviewsFile <- s3Client.list(appConfig.source.trendingReviews)
      _ <- trendingReviewsFile.headOption match {
             case Some(value) => ZIO.effect(println(value))
             case None        => ZIO.fail(new RuntimeException("asd"))
           }
      /*      _ <- s3Client
             .streamFile(
               "yelp-processed/trending_business_review",
               "part-00000-0dc52d81-7d21-4339-93de-cf16e44c8d49-c000.csv"
             )
             .mapM(line => ZIO.effect(println(line)))
             .runDrain*/
    } yield ()).inject(AppConfig.live, Logging.live, S3ClientWrapper.live, S3Client.live, ZEnv.live).exitCode
}
