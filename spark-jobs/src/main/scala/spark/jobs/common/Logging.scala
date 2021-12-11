package spark.jobs.common

import zio.ZLayer
import logstage.{Log, LogZIO}
import izumi.logstage.api.IzLogger
import izumi.logstage.api.routing.StaticLogRouter

object Logging {
  private val logger = IzLogger(levels =
    Map(
      "o.a.h.fs.s3a.S3AFileSystem"   -> Log.Level.Error,
      "o.s.jetty.io.ManagedSelector" -> Log.Level.Error,
      "org.apache"                   -> Log.Level.Error,
      "jetty"                        -> Log.Level.Error,
      "nio"                          -> Log.Level.Error
    )
  )

  StaticLogRouter.instance.setup(logger.router)

  lazy val live = ZLayer.succeed(LogZIO.withFiberId(logger))
}
