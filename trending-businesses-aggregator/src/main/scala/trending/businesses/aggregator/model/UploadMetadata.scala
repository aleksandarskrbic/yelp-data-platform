package trending.businesses.aggregator.model

import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest

final case class UploadMetadata(filename: String, bucket: String, initRequest: InitiateMultipartUploadRequest)
