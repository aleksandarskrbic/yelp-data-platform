package trending.businesses.aggregator.processor

import trending.businesses.aggregator.common.AppConfig
import trending.businesses.aggregator.storage.S3Client
import zio.ZIO

final class TrendingBusinessesProcessor(
  s3Client: S3Client,
  sourceConfig: AppConfig.Source,
  sinkConfig: AppConfig.Sink
) {
  def start =
    for {
      trendingReviewsFile <- s3Client.list(sourceConfig.trendingReviews)
      _ <- trendingReviewsFile.headOption match {
             case Some(value) => ???
             case None        => ZIO.fail(new RuntimeException("asd"))
           }
    } yield ()
}
