package com.gaufoo.sst

import scala.concurrent.Future

trait Types {
  type Key = String
  type Value = String
  type Offset = Int
}

trait KVEngine extends Types {
  def set(key: Key, value: Value): Future[Value]

  def get(key: Key): Future[Option[Value]]

  def delete(key: Key): Future[Option[Value]]

  def allKeysAsc(): Future[List[Key]]

  def allKeysDes(): Future[List[Key]]

  def shutdown(): Future[Boolean]

  def isShutdown: Boolean
}

object KVEngine {
  def default(dbName: String): KVEngine = SSTEngine.build(dbName)
}