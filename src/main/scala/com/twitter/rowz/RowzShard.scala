package com.twitter.rowz

import com.twitter.gizzard.shards
import com.twitter.util.Time


object RowzShard {
  type Cursor = Long
  val CursorStart = 0
}

trait RowzShard extends shards.Shard {
  import RowzShard._

  def create(id: Long, name: String, at: Time)
  def destroy(row: Row, at: Time)
  def read(id: Long): Option[Row]
  def selectAll(cursor: Cursor, count: Int): (Seq[Row], Option[Cursor])
  def write(rows: Seq[Row])
}
