package com.twitter.rowz
package integration

import com.twitter.util.Time
import com.twitter.gizzard.shards.ShardInfo
import com.twitter.gizzard.nameserver.Forwarding


object RowzSpec extends IntegrationSpecification {
  "Rowz" should {
    val shardInfo = new ShardInfo("SqlShard", "shard_001", "localhost")

    doBefore {
      reload {
        nameServer.createShard(shardInfo)
        nameServer.setForwarding(new Forwarding(0, Long.MinValue, shardInfo.id))
      }
    }

    "row create & read" in {
      val id = rowzService.create("a row")

      rowzService.read(id) must eventually(not(throwA[Exception]))
      rowzService.read(id).name mustEqual "a row"

      Thread.sleep(20)
      rowzService.destroy(id)
      rowzService.read(id) must eventually(throwA[Exception])
    }
  }
}
