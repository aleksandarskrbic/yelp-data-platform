package query.service.repository

import zio._

abstract class InMemoryRepository[T](inner: Chunk[T]) {
  def getAll: Chunk[T] = inner
}
