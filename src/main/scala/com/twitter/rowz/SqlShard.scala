package com.twitter.rowz

import java.sql.SQLException
import java.sql.{SQLIntegrityConstraintViolationException, ResultSet}
import com.twitter.querulous.evaluator.{QueryEvaluatorFactory, QueryEvaluator}
import com.twitter.querulous.query.SqlQueryTimeoutException
import com.twitter.gizzard.shards.{ShardException, ShardInfo}
import com.twitter.gizzard.proxy.SqlExceptionWrappingProxy


class SqlShardFactory(qeFactory: QueryEvaluatorFactory, conn: Connection)
extends shards.ShardFactory[Shard] {

  val TABLE_DDL = """
CREATE TABLE IF NOT EXISTS %s (
  id                    BIGINT UNSIGNED          NOT NULL,
  name                  VARCHAR(255)             NOT NULL,
  created_at            BIGINT UNSIGNED          NOT NULL,
  updated_at            BIGINT UNSIGNED          NOT NULL,
  state                 TINYINT                  NOT NULL,

  PRIMARY KEY (id)
) TYPE=INNODB"""

  def instantiate(shardInfo: ShardInfo, weight: Int, children: Seq[RowzShard]) = {
    val queryEvaluator = qeFactory(conn.withHost(shardInfo.hostname))
    new SqlShard(queryEvaluator, shardInfo, weight, children)
  }

  def materialize(shardInfo: shards.ShardInfo) = {
    try {
      val evaluator = qeFactory(connection.withHost(shardInfo.hostname).withoutDatabase)
      evaluator.execute("CREATE DATABASE IF NOT EXISTS " + conn.database)
      evaluator.execute(ddl.format(conn.database +"."+ info.tablePrefix))
    } catch {
      case e: SQLException => throw new shards.ShardException(e.toString)
      case e: SqlQueryTimeoutException => throw new shards.ShardTimeoutException
    }
  }
}


class SqlShard(
  queryEvaluator: QueryEvaluator,
  val shardInfo: shards.ShardInfo,
  val weight: Int,
  val children: Seq[RowzShard]) extends RowzShard {

  private val table = shardInfo.tablePrefix

  def create(id: Long, name: String, at: Time) = write(new Row(id, name, at, at, State.Normal))
  def destroy(row: Row, at: Time)              = write(new Row(row.id, row.name, row.createdAt, at, State.Destroyed))

  def read(id: Long) = {
    queryEvaluator.selectOne("SELECT * FROM " + table + " WHERE id = ? AND state = ?", id, State.Normal.id)(makeRow(_))
  }

  def selectAll(cursor: Cursor, count: Int) = {
    val rows = queryEvaluator.select("SELECT * FROM " + table + " WHERE id > ? ORDER BY id ASC LIMIT " + count + 1, cursor)(makeRow(_))
    val chomped = rows.take(count)
    val nextCursor = if (chomped.size < rows.size) Some(chomped.last.id) else None
    (chomped, nextCursor)
  }

  def write(rows: Seq[Row]) = rows.foreach(write(_))

  def write(row: Row) = {
    val Row(id, name, createdAt, updatedAt, state) = row
    insertOrUpdate {
      queryEvaluator.execute("INSERT INTO " + table + " (id, name, created_at, updated_at, state) VALUES (?, ?, ?, ?, ?)",
        id, name, createdAt.inSeconds, updatedAt.inSeconds, state.id)
    } {
      queryEvaluator.execute("UPDATE " + table + " SET id = ?, name = ?, created_at = ?, updated_at = ?, state = ? WHERE updated_at < ?",
        id, name, createdAt.inSeconds, updatedAt.inSeconds, state.id, updatedAt.inSeconds)
    }
  }

  private def insertOrUpdate(f: => Unit)(g: => Unit) {
    try {
      f
    } catch {
      case e: SQLIntegrityConstraintViolationException => g
    }
  }

  private def makeRow(row: ResultSet) = {
    new Row(row.getLong("id"), row.getString("name"), Time(row.getLong("created_at").seconds), Time(row.getLong("updated_at").seconds), State(row.getInt("state")))
  }
}
