package com.twitter.rowz

import com.twitter.gizzard.shards.{ReadWriteShard, ReadWriteShardAdapter}
import com.twitter.util.Time
import RowzShard.Cursor


class RowzShardAdapter(shard: ReadWriteShard[RowzShard])
extends ReadWriteShardAdapter(shard) with RowzShard {

  def set(rows: Seq[Row])                   = shard.writeOperation(_.set(rows))
  def destroy(id: Long, at: Time)           = shard.writeOperation(_.destroy(id, at))

  def read(id: Long)                        = shard.readOperation(_.read(id))
  def selectAll(cursor: Cursor, count: Int) = shard.readOperation(_.selectAll(cursor, count))
}
