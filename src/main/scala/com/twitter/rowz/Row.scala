package com.twitter.rowz

import com.twitter.util.Time

object RowState extends Enumeration {
  val Normal, Destroyed = Value
}

case class Row(id: Long, name: String, createdAt: Time, updatedAt: Time, state: RowState.Value)
