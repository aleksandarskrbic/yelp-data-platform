package s3.loader.service

import zio._
import zio.stream._
import java.io.File
import logstage.LogZIO.log
import s3.loader.common.AppConfig

final class LoaderService(appConfig: AppConfig, uploadService: UploadService) {
  def start =
    for {
      rootDirectory <- ZIO.effect(createRootDirectory)
      filesToUpload <-
        ZIO
          .effect(Chunk.fromArray(rootDirectory.listFiles()))
          .onError(error => log.error(s"Unable to list files in directory. $error") *> ZIO.succeed(Chunk.empty[File]))
      _ <- ZStream.fromChunk(filesToUpload).mapMParUnordered(filesToUpload.size)(uploadService.upload).runDrain
    } yield ()

  private def createRootDirectory: File = new File(appConfig.upload.directory)
}

object LoaderService {
  lazy val live = (for {
    appConfig     <- ZIO.service[AppConfig]
    uploadService <- ZIO.service[UploadService]
  } yield new LoaderService(appConfig, uploadService)).toLayer
}
