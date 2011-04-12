package com.twitter.rowz
package jobs

import com.twitter.gizzard.scheduler.{JsonJobParser, JsonJob}
import com.twitter.util.Time


class DestroyJobParser(findForwarding: Long => RowzShard) extends JsonJobParser {
  def apply(attributes: Map[String, Any]): JsonJob = {
    new DestroyJob(
      attributes("id").asInstanceOf[Long],
      Time.fromMilliseconds(attributes("at").asInstanceOf[Long]),
      findForwarding
    )
  }
}

class DestroyJob(id: Long, at: Time, findForwarding: Long => RowzShard) extends JsonJob {
  def toMap = {
    Map("id" -> id, "at" -> at.inMilliseconds)
  }

  def apply() {
    findForwarding(id).destroy(id, at)
  }
}
