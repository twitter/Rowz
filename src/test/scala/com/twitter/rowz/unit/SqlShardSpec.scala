package com.twitter.rowz
package unit

import org.specs.mock.{ClassMocker, JMocker}
import com.twitter.util.Time
import com.twitter.conversions.time._
import com.twitter.gizzard.shards.{ShardInfo, ShardId, Busy}
import RowzShard.CursorStart


object SqlShardSpec extends ConfiguredSpecification with JMocker with ClassMocker {
  "SqlShard" should {

    val now = Time.now

    val shardFactory = new SqlShardFactory(queryEvaluatorFactory, config.databaseConnection)
    val shardId = ShardId("localhost", "rowz_001")
    val shardInfo = ShardInfo(shardId, "SqlShard",
      "INT UNSIGNED", "INT UNSIGNED", Busy.Normal)

    val sqlShard = shardFactory.instantiate(shardInfo, 1, List[RowzShard]())
    val row = new Row(1, "a row", now, now, RowState.Normal)
    val row2 = new Row(2, "another row", now, now, RowState.Normal)


    doBefore {
      queryEvaluator.execute("DROP DATABASE IF EXISTS " + config.databaseConnection.database)
      shardFactory.materialize(shardInfo)
    }

    "create & read" in {
      sqlShard.set(Seq(row))
      sqlShard.read(row.id) mustEqual Some(row)
    }

    "create, destroy then read" in {
      sqlShard.set(Seq(row))
      sqlShard.destroy(row.id, row.createdAt + 1.second)
      sqlShard.read(row.id) mustEqual None
    }

    "idempotent" in {
      "read a nonexistent row" in {
        sqlShard.read(row.id) mustEqual None
      }

      "destroy, create, then read" in {
        sqlShard.destroy(row.id, row.createdAt + 1.second)
        sqlShard.set(Seq(row))
        sqlShard.read(row.id) mustEqual None
      }
    }

    "selectAll" in {
      doBefore {
        sqlShard.set(Seq(row, row2))
      }

      "start cursor" in {
        val (rows, nextCursor) = sqlShard.selectAll(CursorStart, 1)
        rows.toList mustEqual List(row)
        nextCursor mustEqual Some(row.id)
      }

      "multiple items" in {
        val (rows, nextCursor) = sqlShard.selectAll(CursorStart, 2)
        rows.toList mustEqual List(row, row2)
        nextCursor mustEqual None
      }

      "non-start cursor" in {
        val (rows, nextCursor) = sqlShard.selectAll(row2.id, 1)
        rows.toList mustEqual List(row2)
        nextCursor mustEqual None
      }
    }
  }
}
