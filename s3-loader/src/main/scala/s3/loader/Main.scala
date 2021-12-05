package s3.loader

import izumi.logstage.api.IzLogger
import izumi.logstage.api.routing.StaticLogRouter
import logstage.LogZIO
import zio._

import java.io.File

object Logging {
  private val logger = IzLogger()

  StaticLogRouter.instance.setup(logger.router)

  val live = ZLayer.succeed(LogZIO.withFiberId(logger))
}

object Main extends zio.App {
  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] = ???
}
