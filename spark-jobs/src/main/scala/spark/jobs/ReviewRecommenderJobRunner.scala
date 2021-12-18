package spark.jobs

import zio._
import spark.jobs.storage.DataSource
import spark.jobs.adapter.spark.SparkWrapper
import spark.jobs.common.{AppConfig, Logging}
import spark.jobs.processor.ReviewRecommenderJob

object ReviewRecommenderJobRunner extends zio.App {
  override def run(args: List[String]): URIO[ZEnv, ExitCode] =
    (for {
      reviewRecommenderJob <- ZIO.service[ReviewRecommenderJob]
      _                    <- reviewRecommenderJob.start
    } yield ())
      .inject(
        AppConfig.live,
        Logging.live,
        SparkWrapper.live,
        DataSource.live,
        ReviewRecommenderJob.live,
        ZEnv.live
      )
      .exitCode
}
