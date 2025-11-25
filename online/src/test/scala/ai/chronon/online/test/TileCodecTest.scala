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

package ai.chronon.online.test

import ai.chronon.api.ScalaJavaConversions.JListOps
import ai.chronon.api.{StructField, _}
import ai.chronon.online.TileCodec
import ai.chronon.online.serde.ArrayRow
import org.junit.Assert.assertEquals
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

class TileCodecTest extends AnyFlatSpec {
  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)
  private val histogram = Map[String, Int]("A" -> 3, "B" -> 2).asJava

  private val aggregationsAndExpected: Array[(Aggregation, Seq[Any])] = Array(
    Builders.Aggregation(Operation.AVERAGE, "views", Seq(new Window(1, TimeUnit.DAYS))) -> Seq(16.0),
    Builders.Aggregation(Operation.AVERAGE, "rating", Seq(new Window(1, TimeUnit.DAYS))) -> Seq(4.0),
    Builders.Aggregation(Operation.SUM,
                         "rating",
                         Seq(new Window(1, TimeUnit.DAYS), new Window(7, TimeUnit.DAYS))) -> Seq(12.0f, 12.0f),
    Builders.Aggregation(Operation.UNIQUE_COUNT,
                         "title",
                         Seq(new Window(1, TimeUnit.DAYS), new Window(7, TimeUnit.DAYS))) -> Seq(3L, 3L),
    Builders.Aggregation(Operation.LAST,
                         "title",
                         Seq(new Window(1, TimeUnit.DAYS), new Window(7, TimeUnit.DAYS))) -> Seq("C", "C"),
    Builders.Aggregation(Operation.LAST_K,
                         "title",
                         Seq(new Window(1, TimeUnit.DAYS), new Window(7, TimeUnit.DAYS)),
                         argMap = Map("k" -> "2")) -> Seq(List("C", "B").asJava, List("C", "B").asJava),
    Builders.Aggregation(Operation.TOP_K,
                         "title",
                         Seq(new Window(1, TimeUnit.DAYS), new Window(7, TimeUnit.DAYS)),
                         argMap = Map("k" -> "1")) -> Seq(List("C").asJava, List("C").asJava),
    Builders.Aggregation(Operation.MIN,
                         "title",
                         Seq(new Window(1, TimeUnit.DAYS), new Window(7, TimeUnit.DAYS))) -> Seq("A", "A"),
    Builders.Aggregation(Operation.APPROX_UNIQUE_COUNT,
                         "title",
                         Seq(new Window(1, TimeUnit.DAYS), new Window(7, TimeUnit.DAYS))) -> Seq(3L, 3L),
    Builders.Aggregation(Operation.HISTOGRAM,
                         "hist_input",
                         Seq(new Window(1, TimeUnit.DAYS), new Window(7, TimeUnit.DAYS)),
                         argMap = Map("k" -> "2")) -> Seq(histogram, histogram),
    Builders.Aggregation(Operation.LAST_K,
                         "activity",
                         Seq(new Window(1, TimeUnit.DAYS), new Window(7, TimeUnit.DAYS)),
                         argMap = Map("k" -> "2")) -> Seq(List(Array("C", 3.0f), Array("B", 5.0f)).toJava,
                                                          List(Array("C", 3.0f), Array("B", 5.0f)).toJava)
  )

  private val bucketedAggregations: Array[Aggregation] = Array(
    Builders.Aggregation(
      operation = Operation.AVERAGE,
      inputColumn = "views",
      buckets = Seq("title"),
      windows = Seq(new Window(1, TimeUnit.DAYS), new Window(7, TimeUnit.DAYS))
    )
  )
  private val expectedBucketResult = Map("A" -> 4.0, "B" -> 40.0, "C" -> 4.0).asJava
  private val expectedBucketedResults = Seq(expectedBucketResult, expectedBucketResult)

  private val schema = List(
    "created" -> LongType,
    "views" -> IntType,
    "rating" -> FloatType,
    "title" -> StringType,
    "hist_input" -> ListType(StringType),
    "activity" -> StructType("activity_struct",
                             Array(
                               StructField("story_name", StringType),
                               StructField("story_rating", FloatType)
                             ))
  )

  def createRow(ts: Long, views: Int, rating: Float, title: String, histInput: Seq[String]): Row = {
    val values: Array[(String, Any)] = Array(
      "created" -> ts,
      "views" -> views,
      "rating" -> rating,
      "title" -> title,
      "hist_input" -> histInput,
      "activity" -> Map("story_name" -> title, "story_rating" -> rating)
    )
    new ArrayRow(values.map(_._2), ts)
  }

  def deepEquals(a: Any, b: Any): Boolean = (a, b) match {
    case (arr1: Array[_], arr2: Array[_]) => arr1.toSeq == arr2.toSeq
    case (seq1: Seq[_], seq2: Seq[_]) =>
      seq1.size == seq2.size && seq1.zip(seq2).forall { case (x, y) => deepEquals(x, y) }
    case (list1: java.util.List[_], list2: java.util.List[_]) =>
      deepEquals(list1.asScala.toSeq, list2.asScala.toSeq)
    case _ => a == b
  }

  it should "tile codec ir ser round trip" in {
    val groupByMetadata = Builders.MetaData(name = "my_group_by")
    val (aggregations, expectedVals) = aggregationsAndExpected.unzip
    val expectedFlattenedVals = expectedVals.flatten
    val groupBy = Builders.GroupBy(metaData = groupByMetadata, aggregations = aggregations)
    val tileCodec = new TileCodec(groupBy, schema)
    val rowIR = tileCodec.rowAggregator.init

    val originalIsComplete = true
    val rows = Seq(
      createRow(1519862399984L, 4, 4.0f, "A", Seq("D", "A", "B", "A")),
      createRow(1519862399984L, 40, 5.0f, "B", Seq()),
      createRow(1519862399988L, 4, 3.0f, "C", Seq("A", "B", "C"))
    )
    rows.foreach(row => tileCodec.rowAggregator.update(rowIR, row))
    val bytes = tileCodec.makeTileIr(rowIR, originalIsComplete)
    assert(bytes.length > 0)

    val (deserPayload, isComplete) = tileCodec.decodeTileIr(bytes)
    assert(isComplete == originalIsComplete)

    // lets finalize the payload intermediate results and verify things
    val finalResults = tileCodec.windowedRowAggregator.finalize(deserPayload)
    assertEquals(expectedFlattenedVals.length, finalResults.length)

    // we use a windowed row aggregator for the final results as we want the final flattened results
    val windowedRowAggregator = TileCodec.buildWindowedRowAggregator(groupBy, schema)
    expectedFlattenedVals.zip(finalResults).zip(windowedRowAggregator.outputSchema.map(_._1)).foreach {
      case ((expected, actual), name) =>
        logger.info(s"Checking: $name")
        deepEquals(expected, actual) shouldBe true
    }
  }

  it should "tile codec ir ser round trip_with buckets" in {
    val groupByMetadata = Builders.MetaData(name = "my_group_by")
    val groupBy = Builders.GroupBy(metaData = groupByMetadata, aggregations = bucketedAggregations)
    val tileCodec = new TileCodec(groupBy, schema)
    val rowIR = tileCodec.rowAggregator.init

    val originalIsComplete = true
    val rows = Seq(
      createRow(1519862399984L, 4, 4.0f, "A", Seq("D", "A", "B", "A")),
      createRow(1519862399984L, 40, 5.0f, "B", Seq()),
      createRow(1519862399988L, 4, 3.0f, "C", Seq("A", "B", "C"))
    )
    rows.foreach(row => tileCodec.rowAggregator.update(rowIR, row))
    val bytes = tileCodec.makeTileIr(rowIR, originalIsComplete)
    assert(bytes.length > 0)

    val (deserPayload, isComplete) = tileCodec.decodeTileIr(bytes)
    assert(isComplete == originalIsComplete)

    // lets finalize the payload intermediate results and verify things
    val finalResults = tileCodec.windowedRowAggregator.finalize(deserPayload)
    assertEquals(expectedBucketedResults.size, finalResults.length)

    // we use a windowed row aggregator for the final results as we want the final flattened results
    val windowedRowAggregator = TileCodec.buildWindowedRowAggregator(groupBy, schema)
    expectedBucketedResults.zip(finalResults).zip(windowedRowAggregator.outputSchema.map(_._1)).foreach {
      case ((expected, actual), name) =>
        logger.info(s"Checking: $name")
        assertEquals(expected, actual)
    }
  }
}
