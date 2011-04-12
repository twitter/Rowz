package com.twitter.rowz
package jobs

import com.twitter.gizzard.nameserver.NameServer
import com.twitter.gizzard.shards.ShardId
import com.twitter.gizzard.scheduler._
import com.twitter.rowz.RowzShard.Cursor


class RowzCopyFactory(nameServer: NameServer[RowzShard], scheduler: JobScheduler, defaultCount: Int = 500)
extends CopyJobFactory[RowzShard] {
  import RowzShard._

  def apply(source: ShardId, dest: ShardId) = {
    new RowzCopyJob(source, dest, CursorStart, defaultCount, nameServer, scheduler)
  }
}

class RowzCopyParser(nameServer: NameServer[RowzShard], scheduler: JobScheduler)
extends CopyJobParser[RowzShard] {
  def deserialize(attributes: Map[String, Any], source: ShardId, dest: ShardId, count: Int) = {
    val cursor = attributes("cursor").asInstanceOf[Int]
    val count = attributes("count").asInstanceOf[Int]

    new RowzCopyJob(source, dest, cursor, count, nameServer, scheduler)
  }
}

class RowzCopyJob(
  sourceId: ShardId,
  destinationId: ShardId,
  cursor: Cursor,
  count: Int,
  nameServer: NameServer[RowzShard],
  scheduler: JobScheduler)
extends CopyJob[RowzShard](sourceId, destinationId, count, nameServer, scheduler) {
  def copyPage(source: RowzShard, dest: RowzShard, count: Int) = {
    val (rows, nextCursor) = source.selectAll(cursor, count)

    dest.set(rows)

    nextCursor map { next =>
      new RowzCopyJob(sourceId, destinationId, next, count, nameServer, scheduler)
    }
  }

  def serialize = Map("cursor" -> cursor)
}
