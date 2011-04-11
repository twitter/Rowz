package com.twitter.rowz.jobs

import com.twitter.gizzard.nameserver.NameServer
import com.twitter.gizzard.shards.ShardId
import com.twitter.gizzard.scheduler._


class RowzCopyFactory(nameServer: NameServer[RowzShard], scheduler: JobScheduler, defaultCount: Int = 500)
extends CopyJobFactory[RowzShard] {
  def apply(source: ShardId, dest: ShardId) = {
    new RowzCopyJob(source, dest, 0, defaultCount, nameServer, scheduler)
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
  cursor: Int,
  count: Int,
  nameServer: NameServer[RowzShard],
  scheduler: JobScheduler)
extends CopyJob[RowzShard] {
  def copyPage(source: RowzShard, dest: RowzShard, count: Int) = {
    val rows = source.selectAll(cursor, count)

    if (rows.isEmpty) {
      None
    } else {
      dest.write(rows)
      Some(new RowzCopyJob(sourceId, destinationId, rows.last.id, count, nameServer, scheduler))
    }
  }
}
