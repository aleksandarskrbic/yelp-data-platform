package s3.loader.model

import zio.Chunk
import java.io.File

final case class PartIndex(value: Long)  extends AnyVal
final case class PartSize(value: Long)   extends AnyVal
final case class FileOffset(value: Long) extends AnyVal

final case class PartDetails(value: Chunk[(PartIndex, PartSize, FileOffset)]) extends AnyVal

object PartDetails {
  def fromFile(file: File, partSize: Long = 5 * 1024 * 1024): PartDetails = {

    def idxToTuple(idx: Long, partSizeValue: Long = partSize): (PartIndex, PartSize, FileOffset) =
      (PartIndex(idx), PartSize(partSizeValue), FileOffset((idx - 1) * partSize))

    val contentLength = file.length

    val partsCount   = contentLength / partSize
    val lastPartSize = contentLength % partSize

    val parts = PartDetails(Chunk.fromIterable(1L to partsCount).map(idxToTuple(_)))

    if (lastPartSize == 0) parts
    else PartDetails(parts.value :+ idxToTuple(partsCount + 1, lastPartSize))
  }
}
