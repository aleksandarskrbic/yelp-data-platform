package query.service.model

final case class BusinessCheckinDetails(
  name: String,
  city: String,
  isOpen: Boolean,
  reviewCount: Long,
  stars: Double,
  checkinCount: Long
)
