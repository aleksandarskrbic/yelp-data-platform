package query.service.repository

import zio._
import zio.stream._
import query.service.loader.FileLoader

final class RepositoryInitializer(fileLoader: FileLoader) {}

object RepositoryInitializer {
  lazy val live = (for {
    fileLoader <- ZIO.service[FileLoader]

    businessByCityCountFiber    <- fileLoader.businessByCityStream.runCollect.fork
    businessByIsOpenFiber       <- fileLoader.businessByIsOpen.runCollect.fork
    trendingBusinessesFiber     <- fileLoader.trendingBusinesses.runCollect.fork
    checkinStatsFiber           <- fileLoader.checkinStats.runCollect.fork
    userDetailsFiber            <- fileLoader.userDetails.runCollect.fork
    businessCheckinDetailsFiber <- fileLoader.businessCheckinDetails.runCollect.fork
  } yield ())

}
