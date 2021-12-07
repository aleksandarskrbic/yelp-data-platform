package s3.loader.model

import com.amazonaws.services.s3.model.UploadPartRequest

final case class UploadPart(uploadRequest: UploadPartRequest) extends AnyVal {
  def part: Int        = uploadRequest.getPartNumber
  def filename: String = uploadRequest.getKey
}
