package com.twitter.rowz

import java.sql.SQLException
import java.sql.{SQLIntegrityConstraintViolationException, ResultSet}
import com.twitter.querulous.evaluator.{QueryEvaluatorFactory, QueryEvaluator}
import com.twitter.querulous.query.SqlQueryTimeoutException
import com.twitter.querulous.config.Connection
import com.twitter.gizzard.shards.{ShardException, ShardTimeoutException, ShardInfo, ShardFactory}
import com.twitter.gizzard.proxy.ShardExceptionWrappingQueryEvaluator
import com.twitter.util.Time


class SqlShardFactory(qeFactory: QueryEvaluatorFactory, connection: Connection)
extends ShardFactory[RowzShard] {

  val ddl = """
CREATE TABLE IF NOT EXISTS %s (
  id                    BIGINT UNSIGNED          NOT NULL,
  name                  VARCHAR(255)             NOT NULL,
  created_at            BIGINT UNSIGNED          NOT NULL,
  updated_at            BIGINT UNSIGNED          NOT NULL,
  state                 TINYINT                  NOT NULL,

  PRIMARY KEY (id)
) ENGINE=INNODB DEFAULT CHARSET=utf8
"""

  def instantiate(shardInfo: ShardInfo, weight: Int, children: Seq[RowzShard]) = {
    val evaluator = new ShardExceptionWrappingQueryEvaluator(
      shardInfo.id,
      qeFactory(connection.withHost(shardInfo.hostname))
    )
    new SqlShard(evaluator, shardInfo, weight, children)
  }

  def materialize(shardInfo: ShardInfo) = {
    try {
      val evaluator = qeFactory(connection.withHost(shardInfo.hostname).withoutDatabase)
      evaluator.execute("CREATE DATABASE IF NOT EXISTS " + connection.database)
      evaluator.execute(ddl.format(connection.database +"."+ shardInfo.tablePrefix))
    } catch {
      case e: SQLException             => throw new ShardException(e.toString)
      case e: SqlQueryTimeoutException => throw new ShardTimeoutException(e.timeout, shardInfo.id)
    }
  }
}


class SqlShard(
  queryEvaluator: QueryEvaluator,
  val shardInfo: ShardInfo,
  val weight: Int,
  val children: Seq[RowzShard])
extends RowzShard {
  import RowzShard._

  private val table = shardInfo.tablePrefix

  val readSql      = "SELECT * FROM " + table + " WHERE id = ? AND state = ?"
  val selectAllSql = "SELECT * FROM " + table + " WHERE id >= ? ORDER BY id ASC LIMIT ?"
  val insertSql    = "INSERT INTO " + table + " (id, name, created_at, updated_at, state) VALUES (?, ?, ?, ?, ?)"
  val updateSql    = "UPDATE " + table + " SET id = ?, name = ?, created_at = ?, updated_at = ?, state = ? WHERE updated_at < ?"

  def set(rows: Seq[Row]) = {
    rows.foreach(write)
  }

  def destroy(id: Long, at: Time) = {
    write(Row(id, "", Time.epoch, at, RowState.Destroyed))
  }

  def read(id: Long) = {
    queryEvaluator.selectOne(readSql, id, RowState.Normal.id)(makeRow)
  }

  def selectAll(cursor: Cursor, count: Int) = {
    val rows       = queryEvaluator.select(selectAllSql, cursor, count + 1)(makeRow)
    val chomped    = rows.take(count)
    val nextCursor = if (chomped.size < rows.size) Some(chomped.last.id) else None

    (chomped, nextCursor)
  }

  def write(row: Row) = {
    val Row(id, name, createdAt, updatedAt, state) = row

    insertOrUpdate {
      queryEvaluator.execute(
        insertSql,
        id,
        name,
        createdAt.inMilliseconds,
        updatedAt.inMilliseconds,
        state.id
      )
    } {
      queryEvaluator.execute(
        updateSql,
        id,
        name,
        createdAt.inMilliseconds,
        updatedAt.inMilliseconds,
        state.id,
        updatedAt.inMilliseconds
      )
    }
  }

  private def insertOrUpdate(f: => Unit)(g: => Unit) {
    try {
      f
    } catch {
      case e: ShardException => e.getCause match {
        case cause: SQLIntegrityConstraintViolationException => g
        case _ => throw e
      }
    }
  }

  private def makeRow(row: ResultSet) = {
    new Row(
      row.getLong("id"),
      row.getString("name"),
      Time.fromMilliseconds(row.getLong("created_at")),
      Time.fromMilliseconds(row.getLong("updated_at")),
      RowState(row.getInt("state"))
    )
  }
}
