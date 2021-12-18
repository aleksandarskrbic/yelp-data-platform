package query.service.common

import zio.ZLayer
import logstage.LogZIO
import izumi.logstage.api.IzLogger

object Logging {
  lazy val live = ZLayer.succeed(LogZIO.withFiberId(IzLogger()))
}
