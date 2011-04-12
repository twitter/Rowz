package com.twitter.rowz

import com.twitter.gizzard.scheduler.{PrioritizingJobScheduler}
import jobs.{SetJob, DestroyJob}
import com.twitter.util.Time
import com.twitter.conversions.time._
import thrift.conversions.Row._


class RowzService(
  findForwarding: Long => RowzShard,
  scheduler: PrioritizingJobScheduler,
  makeId: () => Long)
extends thrift.Rowz.Iface {

  def create(name: String) = {
    val at  = Time.now
    val row = Row(makeId(), name, at, at, RowState.Normal)

    scheduler.put(Priority.High.id, new SetJob(row, findForwarding))
    row.id
  }

  def update(row: thrift.Row) {
    // set the row's updated_at, to control our own destiny.
    row.setUpdated_at(Time.now.inMilliseconds)

    scheduler.put(Priority.High.id, new SetJob(row.fromThrift, findForwarding))
  }

  def destroy(id: Long) {
    val at = Time.now
    scheduler.put(Priority.Low.id, new DestroyJob(id, at, findForwarding))
  }

  def read(id: Long) = {
    findForwarding(id).read(id).get.toThrift
  }
}
