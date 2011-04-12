package com.twitter.rowz
package jobs

import com.twitter.gizzard.scheduler.{JsonJobParser, JsonJob}
import com.twitter.util.Time


class CreateJobParser(findForwarding: Long => RowzShard) extends JsonJobParser {
  def appy(attributes: Map[String, Any]): JsonJob = {
    new CreateJob(
      attributes("id").asInstanceOf[Long],
      attributes("name").asInstanceOf[String],
      Time.fromMilliseconds(attributes("at").asInstanceOf[Long]),
      findForwarding
    )
  }
}

class CreateJob(id: Long, name: String, at: Time, findForwarding: Long => RowzShard) extends JsonJob {
  def toMap = {
    Map("id" -> id, "name" -> name, "at" -> at.inMilliseconds)
  }

  def apply() {
    findForwarding(id).create(id, name, at)
  }
}
