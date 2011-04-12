package com.twitter.rowz
package thrift.conversions

import com.twitter.util.Time
import com.twitter.conversions.time._
import com.twitter.rowz

object Row {
  class RichShardingRow(row: rowz.Row) {
    def toThrift = new thrift.Row(
      row.id,
      row.name,
      row.createdAt.inMilliseconds,
      row.updatedAt.inMilliseconds,
      row.state.id
    )
  }
  implicit def shardingRowToRichShardingRow(row: rowz.Row) = new RichShardingRow(row)

  class RichThriftRow(row: thrift.Row) {
    def fromThrift = new rowz.Row(
      row.id,
      row.name,
      Time.fromMilliseconds(row.created_at),
      Time.fromMilliseconds(row.updated_at),
      State(row.state)
    )
  }
  implicit def thriftRowToRichThriftRow(row: thrift.Row) = new RichThriftRow(row)
}
