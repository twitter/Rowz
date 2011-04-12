package com.twitter.rowz

import java.io.File
import org.specs.Specification
import com.twitter.util.Eval


abstract class ConfiguredSpecification extends Specification {
  lazy val config = {
    val conf = Eval[com.twitter.rowz.config.Rowz](new File("config/test.scala"))

    try {
      conf.logging()
    } catch {
      case e => e.printStackTrace()
    }

    conf
  }

  lazy val connection = config.databaseConnection
  lazy val queryEvaluatorFactory = config.rowzQueryEvaluator()
  lazy val queryEvaluator = queryEvaluatorFactory(connection.withoutDatabase)

  noDetailedDiffs()
}

abstract class IntegrationSpecification extends ConfiguredSpecification {
  val (manager, nameServer, rowzService, jobScheduler) = {
    val r = new Rowz(config)
    (r.managerServer, r.nameServer, r.rowzService, r.jobScheduler)
  }

  def reload(f: => Unit) {
    config.nameServer.replicas collect {
      case replica: com.twitter.gizzard.config.Mysql => {
        val db = replica.connection.database

        queryEvaluator.execute("DROP DATABASE IF EXISTS "+ db)
        queryEvaluator.execute("CREATE DATABASE "+ db)
      }
    }

    nameServer.rebuildSchema
    queryEvaluator.execute("DROP DATABASE IF EXISTS "+ config.databaseConnection.database)

    f

    nameServer.reload()
    jobScheduler.start()
  }
}

