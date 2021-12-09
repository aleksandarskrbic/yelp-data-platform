package spark.jobs.processor

import spark.jobs.adapter.SparkWrapper
import zio.Task

abstract class BaseJob(sparkWrapper: SparkWrapper) {
  def withSparkWrapper[A](fn: SparkWrapper => Task[A]) =
    fn(sparkWrapper)
}
