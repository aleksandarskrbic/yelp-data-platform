package `object`.storage.shared.s3.model

import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest

final case class UploadMetadata(filename: String, bucket: String, initRequest: InitiateMultipartUploadRequest)
