package s3.loader.service

import zio._
import java.io.File
import logstage.LogZIO.log
import s3.loader.common.AppConfig

final class LoaderService(appConfig: AppConfig, uploadService: UploadService) {
  def start =
    for {
      rootDirectory <- ZIO.effect(createRootDirectory)
      filesToUpload <- ZIO
                         .effect(Chunk.fromArray(rootDirectory.listFiles()))
                         .onError(e => log.error(s"Unable to list files in directory. $e").as(Chunk.empty[File]))
      result <- ZIO.foreachParN(filesToUpload.size)(filesToUpload)(uploadService.upload)
    } yield result

  private def createRootDirectory: File = new File(appConfig.upload.directory)
}

object LoaderService {
  lazy val live = (for {
    appConfig     <- ZIO.service[AppConfig]
    uploadService <- ZIO.service[UploadService]
  } yield new LoaderService(appConfig, uploadService)).toLayer
}
