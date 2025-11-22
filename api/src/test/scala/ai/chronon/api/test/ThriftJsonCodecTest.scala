/*
 *    Copyright (C) 2023 The Chronon Authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package ai.chronon.api.test

import ai.chronon.api._
import ai.chronon.planner.SourceWithFilterNode
import org.junit.Assert._
import org.scalatest.flatspec.AnyFlatSpec
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class ThriftJsonCodecTest extends AnyFlatSpec {
  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  it should "hexDigest should be stable across serialization/deserialization" in {
    // Create a SourceWithFilterNode with the structure from the example
    val query = new Query()
    val selects = new java.util.HashMap[String, String]()
    selects.put("listing_id", "listing_id")
    selects.put("ts", "event_time_ms")
    selects.put("row_id", "event_id")
    selects.put("user_id", "user_id")
    query.setSelects(selects)
    query.setTimeColumn("event_time_ms")

    val events = new EventSource()
    events.setQuery(query)
    events.setTable("data.gcp_exports_user_activities__0")

    val source = new Source()
    source.setEvents(events)

    val sourceWithFilter = new SourceWithFilterNode()
    sourceWithFilter.setSource(source)

    // Compute hexDigest of the original object
    val hash1 = ThriftJsonCodec.hexDigest(sourceWithFilter)
    logger.info(s"Hash 1 (original): $hash1")

    // Serialize to JSON string and deserialize back
    val jsonStr = ThriftJsonCodec.toJsonStr(sourceWithFilter)
    logger.info(s"JSON: $jsonStr")

    val deserialized = ThriftJsonCodec.fromJsonStr(jsonStr, check = false, classOf[SourceWithFilterNode])

    // Compute hexDigest of the deserialized object
    val hash2 = ThriftJsonCodec.hexDigest(deserialized)
    logger.info(s"Hash 2 (after deserialize): $hash2")

    // The hashes should be equal - if they're not, it proves the hypothesis that
    // JSON key ordering is non-deterministic
    assertEquals(s"hexDigest should be stable across serialization/deserialization. Hash1: $hash1, Hash2: $hash2",
                 hash1, hash2)
  }

  it should "hexDigest should be stable when map keys are inserted in different order" in {
    // Create first object with keys in one order using LinkedHashMap to preserve insertion order
    val query1 = new Query()
    val selects1 = new java.util.LinkedHashMap[String, String]()
    selects1.put("listing_id", "listing_id")
    selects1.put("row_id", "event_id")
    selects1.put("ts", "event_time_ms")
    selects1.put("user_id", "user_id")
    query1.setSelects(selects1)
    query1.setTimeColumn("event_time_ms")

    val events1 = new EventSource()
    events1.setQuery(query1)
    events1.setTable("data.gcp_exports_user_activities__0")

    val source1 = new Source()
    source1.setEvents(events1)

    val sourceWithFilter1 = new SourceWithFilterNode()
    sourceWithFilter1.setSource(source1)

    // Create second object with keys in different order using LinkedHashMap
    val query2 = new Query()
    val selects2 = new java.util.LinkedHashMap[String, String]()
    // Insert in reverse order
    selects2.put("user_id", "user_id")
    selects2.put("ts", "event_time_ms")
    selects2.put("row_id", "event_id")
    selects2.put("listing_id", "listing_id")
    query2.setSelects(selects2)
    query2.setTimeColumn("event_time_ms")

    val events2 = new EventSource()
    events2.setQuery(query2)
    events2.setTable("data.gcp_exports_user_activities__0")

    val source2 = new Source()
    source2.setEvents(events2)

    val sourceWithFilter2 = new SourceWithFilterNode()
    sourceWithFilter2.setSource(source2)

    // Compute hexDigest for both
    val hash1 = ThriftJsonCodec.hexDigest(sourceWithFilter1)
    val hash2 = ThriftJsonCodec.hexDigest(sourceWithFilter2)

    val json1 = ThriftJsonCodec.toJsonStr(sourceWithFilter1)
    val json2 = ThriftJsonCodec.toJsonStr(sourceWithFilter2)

    println(s"JSON 1: $json1")
    println(s"JSON 2: $json2")
    println(s"Hash 1 (order 1): $hash1")
    println(s"Hash 2 (order 2): $hash2")

    // These should be equal if canonicalization works, different if not
    assertEquals(s"hexDigest should be the same regardless of map insertion order. Hash1: $hash1, Hash2: $hash2",
                 hash1, hash2)
  }
}
