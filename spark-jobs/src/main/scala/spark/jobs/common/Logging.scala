package spark.jobs.common

import zio.ZLayer
import logstage.{Log, LogZIO}
import izumi.logstage.api.IzLogger
import izumi.logstage.api.logger.LogRouter

object Logging {
  private val router = LogRouter(
    levels = Map(
      "nio.*"        -> Log.Level.Error,
      "o.a.h.*"      -> Log.Level.Error,
      "org.apache.*" -> Log.Level.Error
    )
  )

  lazy val live = ZLayer.succeed(LogZIO.withFiberId(IzLogger(router)))
}
