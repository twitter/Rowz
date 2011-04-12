package com.twitter.rowz
package jobs

import com.twitter.gizzard.scheduler.{JsonJobParser, JsonJob}
import com.twitter.util.Time


class DestroyJobParser(findForwarding: Long => RowzShard) extends JsonJobParser {
  def apply(attributes: Map[String, Any]): JsonJob = {
    new DestroyJob(
      new Row(
        attributes("id").asInstanceOf[Long],
        attributes("name").asInstanceOf[String],
        Time.fromMilliseconds(attributes("createdAt").asInstanceOf[Long]),
        Time.fromMilliseconds(attributes("updatedAt").asInstanceOf[Long]),
        State(attributes("state").asInstanceOf[Int])
      ),
      Time.fromMilliseconds(attributes("at").asInstanceOf[Long]),
      findForwarding
    )
  }
}

class DestroyJob(row: Row, at: Time, findForwarding: Long => RowzShard) extends JsonJob {
  def toMap = {
    Map(
      "id" -> row.id,
      "name" -> row.name,
      "createdAt" -> row.createdAt.inMilliseconds,
      "updatedAt" -> row.updatedAt.inMilliseconds,
      "state" -> row.state.id,
      "at" -> at.inMilliseconds
    )
  }

  def apply() {
    findForwarding(row.id).destroy(row, at)
  }
}
