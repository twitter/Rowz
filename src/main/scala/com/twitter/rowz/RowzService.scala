package com.twitter.rowz

import com.twitter.gizzard.scheduler.{PrioritizingJobScheduler}
import jobs.{CreateJob, DestroyJob}
import com.twitter.util.Time
import com.twitter.conversions.time._
import thrift.conversions.Row._


class RowzService(
  findForwarding: Long => RowzShard,
  scheduler: PrioritizingJobScheduler,
  makeId: () => Long)
extends thrift.Rowz.Iface {

  def create(name: String, at: Long) = {
    val id = makeId()
    scheduler.put(Priority.High.id, new CreateJob(id, name, Time.fromMilliseconds(at), findForwarding))
    id
  }

  def destroy(row: thrift.Row, at: Long) {
    scheduler.put(Priority.Low.id, new DestroyJob(row.fromThrift, Time.fromMilliseconds(at), findForwarding))
  }

  def read(id: Long) = {
    findForwarding(id).read(id).get.toThrift
  }
}
