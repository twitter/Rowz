package com.twitter.rowz

import com.twitter.gizzard.shards.{ReadWriteShard, ReadWriteShardAdapter}
import com.twitter.util.Time
import RowzShard.Cursor


class RowzShardAdapter(shard: ReadWriteShard[Shard])
extends ReadWriteShardAdapter(shard) with RowzShard {

  def create(id: Long, name: String, at: Time) = shard.writeOperation(_.create(id, name, at))
  def destroy(row: Row, at: Time)              = shard.writeOperation(_.destroy(row, at))
  def write(rows: Seq[Row])                    = shard.writeOperation(_.write(rows))

  def read(id: Long)                           = shard.readOperation(_.read(id))
  def selectAll(cursor: Cursor, count: Int)    = shard.readOperation(_.selectAll(cursor, count))
}
