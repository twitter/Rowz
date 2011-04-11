package com.twitter.rowz

import com.twitter.ostrich.{Service, ServiceTracker, RuntimeEnvironment}
import com.twitter.util.Eval
import net.lag.configgy.{Config => CConfig}
import java.io.File

import com.twitter.rowz.config.{Rowz => RowzConfig}


object Main extends Service {
  var service: Rowz = _
  var config: RowzConfig = _

  def main(args: Array[String]) {
    config  = Eval[RowzConfig](args.map(new File(_)): _*)
    service = new Rowz(config)

    start()
    println("Running.")
  }

  def start() {
    val adminConfig = new CConfig
    adminConfig.setInt("admin_http_port", config.admin.httpPort)
    adminConfig.setInt("admin_text_port", config.admin.textPort)

    ServiceTracker.register(this)
    ServiceTracker.startAdmin(adminConfig, new RuntimeEnvironment(this.getClass))

    service.start()
  }

  def shutdown() {
    println("Exiting.")

    if (service ne null) service.shutdown()
    service = null
    ServiceTracker.stopAdmin()
  }

  def quiesce() {
    println("Exiting.")

    if (service ne null) service.shutdown(true)
    service = null
    ServiceTracker.stopAdmin()
  }
}
