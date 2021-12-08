package spark.jobs.common

import zio.ZLayer
import logstage.LogZIO
import izumi.logstage.api.IzLogger
import izumi.logstage.api.routing.StaticLogRouter

object Logging {
  private val logger = IzLogger()

  StaticLogRouter.instance.setup(logger.router)

  lazy val live = ZLayer.succeed(LogZIO.withFiberId(logger))
}
