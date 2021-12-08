package s3.loader.model

import java.io.File
import s3.loader.model.FileSize._

sealed trait FileSize
object FileSize {
  val `500MB`: Long = 500L * 1024 * 1024
  val `5GB`: Long   = 5000L * 1024 * 1024

  case object BigFile    extends FileSize
  case object MediumFile extends FileSize
  case object SmallFile  extends FileSize
}

final case class FileWrapper(file: File) extends AnyVal {
  def name: String =
    file.getName

  def size: FileSize = {
    val contentSize = file.length()

    if (contentSize <= FileSize.`500MB`) SmallFile
    else if (contentSize > FileSize.`500MB` && contentSize <= FileSize.`5GB`) MediumFile
    else BigFile
  }
}
