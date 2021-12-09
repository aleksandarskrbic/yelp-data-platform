package spark.jobs.processor

import spark.jobs.adapter.SparkWrapper
import zio.ZIO

class BusinessJob(sparkWrapper: SparkWrapper) extends BaseJob(sparkWrapper) {
  def start = ???
}

object BusinessJob {
  lazy val live = (for {
    sparkWrapper <- ZIO.service[SparkWrapper]
  } yield new BusinessJob(sparkWrapper)).toLayer
}
