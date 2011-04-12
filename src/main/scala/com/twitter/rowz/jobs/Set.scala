package com.twitter.rowz
package jobs

import com.twitter.gizzard.scheduler.{JsonJobParser, JsonJob}
import com.twitter.util.Time


class SetJobParser(findForwarding: Long => RowzShard) extends JsonJobParser {
  def apply(attributes: Map[String, Any]): JsonJob = {
    val row = Row(
      attributes("id").asInstanceOf[Long],
      attributes("name").asInstanceOf[String],
      Time.fromMilliseconds(attributes("created_at").asInstanceOf[Long]),
      Time.fromMilliseconds(attributes("updated_at").asInstanceOf[Long]),
      RowState(attributes("state").asInstanceOf[Int])
    )

    new SetJob(row, findForwarding)
  }
}

class SetJob(row: Row, findForwarding: Long => RowzShard) extends JsonJob {
  def toMap = {
    Map(
      "id" -> row.id,
      "name" -> row.name,
      "updated_at" -> row.updatedAt.inMilliseconds,
      "created_at" -> row.createdAt.inMilliseconds,
      "state" -> row.state.id
    )
  }

  def apply() {
    findForwarding(row.id).set(Seq(row))
  }
}
