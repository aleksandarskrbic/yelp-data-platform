package s3.loader

import zio._
import zio.magic._
import logstage.LogZIO.log
import s3.loader.common.{AppConfig, Logging}
import s3.loader.service.{LoaderService, UploadService}
import `object`.storage.shared.s3.{S3Client, S3ClientWrapper}

// Motivacija i cilj, omoguciti kreirajuci sistem....
// slicni sistemi
// arhitektura sistema, moduli i interakcija
// opis koriscenih tehnologija spark, minio, presto, zio zasto koristimo nesto, zasto nesto ne koristimo nesto (aws pa imaju managed)
// minio se lako integrise itd...
// sledece poglavlje, implementacija reseanja itd, svaki servis pojedinacno...
// presto settings, itd, REST/graphQL
// Konkretan primer rada ...
// Zakljucak, osvrt na sta sam rekao, feature impovements/work...
object S3LoaderApp extends zio.App {
  override def run(args: List[String]): URIO[ZEnv, ExitCode] =
    (for {
      _             <- log.info("Starting object.storage.shared.s3-loader")
      loaderService <- ZIO.service[LoaderService]
      _             <- loaderService.start
      _             <- log.info(s"Successfully uploaded all files.")
    } yield ())
      .inject(
        ZEnv.live,
        AppConfig.live,
        AppConfig.subLayers,
        Logging.live,
        S3ClientWrapper.live,
        S3Client.live,
        UploadService.live,
        LoaderService.live
      )
      .exitCode
}
