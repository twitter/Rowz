package com.twitter.rowz

import com.twitter.conversions.time._

import org.apache.thrift.protocol.{TProtocol, TBinaryProtocol}
import org.apache.thrift.transport.{TSocket, TFramedTransport}

class RowzClient(hostname: String) {
  def port = 7919
  def socketTimeout = 5.seconds

  val socket = new TSocket(hostname, port, socketTimeout.inMilliseconds.toInt)
  val transport = new TFramedTransport(socket)
  val protocol = new TBinaryProtocol(transport)
  val client = new thrift.Rowz.Client.Factory().getClient(protocol)

  transport.open

  def create(name: String) = client.create(name)
  def update(row: thrift.Row) = client.update(row)
  def destroy(id: Long) = client.destroy(id)
  def read(id: Long) = client.read(id)
}
