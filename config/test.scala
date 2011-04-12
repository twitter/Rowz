import com.twitter.rowz.config._
import com.twitter.gizzard.config._
import com.twitter.querulous.config._
import com.twitter.querulous.query.QueryClass
import com.twitter.conversions.time._
import com.twitter.conversions.storage._
import com.twitter.rowz.Priority


trait Credentials extends Connection {
  import scala.collection.JavaConversions._
  val env = System.getenv().toMap
  val username = env.getOrElse("DB_USERNAME", "root")
  val password = env.getOrElse("DB_PASSWORD", null)
}

class TestQueryEvaluator(label: String) extends QueryEvaluator {
  query.debug = DebugLog

  database.pool = new ApachePoolingDatabase {
    sizeMin = 5
    sizeMax = 5
    maxWait = 1.second
    minEvictableIdle = 60.seconds
    testIdle = 1.second
    testOnBorrow = false
  }

  database.timeout = new TimingOutDatabase {
    poolSize  = 10
    queueSize = 10000
    open = 1.second
  }
}

new Rowz {
  val nodeId = 0 // must be unique per app server

  val server = new RowzThriftServer with THsHaServer {
    timeout = 100.millis
    idleTimeout = 10.seconds
    threadPool.minThreads = 10
    threadPool.maxThreads = 10
  }

  val nameServer = new NameServer {
    //mappingFunction = Fnv1a64 // uncomment to hash ids, rather than using identity

    val replicas = Seq(new Mysql {
      queryEvaluator = new TestQueryEvaluator("nameserver")

      val connection = new Connection with Credentials {
        val hostnames = Seq("localhost")
        val database  = "rowz_nameserver_test"
      }
    })
  }

  val databaseConnection = new Connection with Credentials {
    val hostnames = Seq("localhost")
    val database  = "rowz_test"
  }

  val rowzQueryEvaluator = new TestQueryEvaluator("rowz") {
    query.timeouts = Map(
      QueryClass.Select  -> QueryTimeout(1.seconds),
      QueryClass.Execute -> QueryTimeout(1.seconds)
    )
  }

  class TestScheduler(val name: String) extends Scheduler {
    val schedulerType = new KestrelScheduler {
      path = "/tmp"
      keepJournal = false
    }

    threads = 1
    errorLimit = 25
    errorRetryDelay = 900.seconds
    errorStrobeInterval = 30.seconds
    perFlushItemLimit = 1000
    jitterRate = 0.0f
    badJobQueue = new JsonJobLogger { name = "bad_jobs" }
  }

  val jobQueues = Map(
    Priority.High.id   -> new TestScheduler("high"),
    Priority.Medium.id -> new TestScheduler("medium"),
    Priority.Low.id    -> new TestScheduler("low")
  )

  logging = new LogConfigString("level = \"debug\"\nfilename = \"test.log\"")
}

