package com.twitter.rowz

import com.twitter.gizzard.scheduler._
import com.twitter.gizzard.nameserver
import com.twitter.gizzard.GizzardServer

import com.twitter.rowz.jobs._

object Priority extends Enumeration {
  val High, Medium, Low = Value
}

class Rowz(config: com.twitter.rowz.config.Rowz)
extends GizzardServer[RowzShard](config) {

  // define a factory for Rowz's ReadWriteShardAdapter
  val readWriteShardAdapter = new RowzShardAdapter(_)

  val jobPriorities = List(Priority.High.id, Priority.Medium.id, Priority.Low.id)

  // create the id generator

  def idGenerator = new IdGenerator(config.nodeId)

  // curry findCurrentForwarding to pass to the service and job factories.

  def findForwarding(id: Long) = nameServer.findCurrentForwarding(0, id)


  // wire up copy job, the rowz sql shard and single job type
  val copyPriority = Priority.Medium.id
  val copyFactory  = new RowzCopyFactory(nameServer, jobScheduler(Priority.Medium.id))

  shardRepo += ("SqlShard" -> new SqlShardFactory(config.rowzQueryEvaluator(), config.databaseConnection))
  jobCodec  += ("Set".r    -> new SetJobParser(findForwarding))


  // set up the service listener

  val rowzService = new RowzService(findForwarding, jobScheduler, idGenerator)

  lazy val rowzThriftServer = {
    val processor = new thrift.Rowz.Processor(rowzService)
    config.server(processor)
  }

  def start() {
    startGizzard()
    new Thread(new Runnable { def run() { rowzThriftServer.serve() } }, "RowzServerThread").start()
  }

  def shutdown(quiesce: Boolean = false) {
    rowzThriftServer.stop()
    shutdownGizzard(quiesce)
  }
}

// object Rowz {
//   case class State(
//     rowzService: RowzService,
//     prioritizingScheduler: PrioritizingJobScheduler,
//     nameServer: nameserver.NameServer[Shard],
//     copyFactory: gizzard.jobs.CopyFactory[Shard]) {
//       def start() = {
//         nameServer.reload()
//         prioritizingScheduler.start()
//       }

//       def shutdown() = prioritizingScheduler.shutdown()
//     }

//   def apply(config: ConfigMap, w3c: W3CStats): State = apply(
//     config, w3c,
//     new MemoizingDatabaseFactory(new ApachePoolingDatabaseFactory(
//       config("rowz.db.connection_pool.size_min").toInt,
//       config("rowz.db.connection_pool.size_max").toInt,
//       config("rowz.db.connection_pool.test_idle_msec").toLong.millis,
//       config("rowz.db.connection_pool.max_wait").toLong.millis,
//       config("rowz.db.connection_pool.test_on_borrow").toBoolean,
//       config("rowz.db.connection_pool.min_evictable_idle_msec").toLong.millis))
//   )

//   def apply(config: ConfigMap, w3c: W3CStats, databaseFactory: DatabaseFactory): State = {
//     val queryEvaluatorFactory    = new StandardQueryEvaluatorFactory(databaseFactory, new SqlQueryFactory)

//     val throttledLogger         = new ThrottledLogger[String](Logger(), config("throttled_log.period_msec").toInt, config("throttled_log.rate").toInt)
//     val future                  = new Future("ReplicatingFuture", config.configMap("rowz.replication.future"))

//     val shardRepository         = new nameserver.BasicShardRepository[Shard](new ReadWriteShardAdapter(_), throttledLogger, future)
//     shardRepository             += ("com.twitter.rowz.SqlShard" -> new SqlShardFactory(queryEvaluatorFactory, config))

//     val nameServerShards = config.getList("rowz.nameserver.hostnames").map { hostname =>
//       new nameserver.SqlShard(
//         queryEvaluatorFactory(
//           hostname,
//           config("rowz.nameserver.name"),
//           config("rowz.nameserver.username"),
//           config("rowz.nameserver.password")))
//     }

//     val replicatingNameServerShard = new nameserver.ReadWriteShardAdapter(new ReplicatingShard(
//       new ShardInfo("com.twitter.gizzard.shards.ReplicatingShard", "", ""),
//       1, nameServerShards, new nameserver.LoadBalancer(nameServerShards), throttledLogger, future))
//     val nameServer                 = new nameserver.NameServer(replicatingNameServerShard, shardRepository, Hash)
//     val forwardingManager          = new ForwardingManager(nameServer)

//     val polymorphicJobParser    = new PolymorphicJobParser
//     val schedulerMap = new mutable.HashMap[Int, JobScheduler]
//     List((Priority.High, "high"), (Priority.Medium, "medium"), (Priority.Low, "low")).foreach { case (priority, configName) =>
//       val queueConfig = config.configMap("rowz.queue")
//       val scheduler = JobScheduler(configName, queueConfig, polymorphicJobParser, w3c)
//       schedulerMap(priority.id) = scheduler
//     }
//     val prioritizingScheduler = new PrioritizingJobScheduler(schedulerMap)

//     val copyJobParser           = new BoundJobParser(jobs.CopyParser, (nameServer, prioritizingScheduler(Priority.Medium.id)))
//     val migrateJobParser        = new BoundJobParser(new gizzard.jobs.MigrateParser(jobs.CopyParser), (nameServer, prioritizingScheduler(Priority.Medium.id)))
//     val createJobParser         = new BoundJobParser(jobs.CreateParser, forwardingManager)
//     val destroyJobParser        = new BoundJobParser(jobs.DestroyParser, forwardingManager)
//     polymorphicJobParser        += ("Copy".r, copyJobParser)
//     polymorphicJobParser        += ("Migrate".r, migrateJobParser)
//     polymorphicJobParser        += ("Create".r, createJobParser)
//     polymorphicJobParser        += ("Destroy".r, destroyJobParser)

//     val rowzService             = new RowzService(forwardingManager, prioritizingScheduler, new IdGenerator(config("host.id").toInt))

//     State(rowzService, prioritizingScheduler, nameServer, jobs.CopyFactory)
//   }
// }
