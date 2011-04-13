package com.twitter.rowz

import com.twitter.gizzard.scheduler.{PrioritizingJobScheduler}
import jobs.SetJob
import com.twitter.util.Time
import com.twitter.conversions.time._
import thrift.conversions.Row._


class RowzService(
  findForwarding: Long => RowzShard,
  scheduler: PrioritizingJobScheduler,
  nextId: () => Long)
extends thrift.Rowz.Iface {

  def read(id: Long) = {
    findForwarding(id).read(id).get.toThrift
  }

  def create(name: String) = {
    val at  = Time.now
    val row = Row(nextId(), name, at, at, RowState.Normal)

    enqueueSet(row)

    row.id
  }

  def update(row: thrift.Row) {
    // set the row's updated_at, to control our own destiny.
    row.setUpdated_at(Time.now.inMilliseconds)

    enqueueSet(row.fromThrift)
  }

  def destroy(id: Long) {
    val at = Time.now
    val row = Row(id, "", at, at, RowState.Destroyed)

    enqueueSet(row)
  }

  private def enqueueSet(row: Row) {
    scheduler.put(Priority.High.id, new SetJob(row, findForwarding))
  }
}
