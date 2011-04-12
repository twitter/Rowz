package com.twitter.rowz.config

import com.twitter.gizzard
import com.twitter.gizzard.config._
import com.twitter.querulous.config.{Connection, QueryEvaluator}
import com.twitter.util.Duration
import com.twitter.conversions.time._


class AdminService {
  var httpPort = 9990
  var textPort = 9991
}

trait RowzThriftServer extends TServer {
  var name = "rowz"
  var port = 7919
}

trait Rowz extends GizzardServer {
  def server: RowzThriftServer

  def databaseConnection: Connection
  def rowzQueryEvaluator: QueryEvaluator

  def nodeId: Int

  var admin: AdminService = new AdminService
}
