package ai.chronon.api.test

import ai.chronon.api.CollectionExtensions._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.{ArrayList => JArrayList}

class CollectionExtensionsTest extends AnyFlatSpec with Matchers {

  "JListExtension" should "handle foreach with null list" in {
    val nullList: java.util.List[String] = null
    var count = 0
    nullList.foreach(_ => count += 1)
    count shouldBe 0
  }

  it should "handle foreach with empty list" in {
    val emptyList = new JArrayList[String]()
    var count = 0
    emptyList.foreach(_ => count += 1)
    count shouldBe 0
  }

  it should "handle foreach with non-empty list" in {
    val list = new JArrayList[String]()
    list.add("1")
    list.add("2")
    list.add("3")

    var sum = 0
    list.foreach(s => sum += s.toInt)
    sum shouldBe 6
  }

  it should "handle map with null list" in {
    val nullList: java.util.List[String] = null
    nullList.map(_.toInt).toList shouldBe empty
  }

  it should "handle map with empty list" in {
    val emptyList = new JArrayList[String]()
    emptyList.map(_.toInt).toList shouldBe empty
  }

  it should "handle map with non-empty list" in {
    val list = new JArrayList[String]()
    list.add("1")
    list.add("2")
    list.add("3")

    list.map(_.toInt).toList shouldBe List(1, 2, 3)
  }

  it should "handle flatMap with null list" in {
    val nullList: java.util.List[String] = null
    nullList.flatMap(s => Iterator(s, s)).toList shouldBe empty
  }

  it should "handle flatMap with empty list" in {
    val emptyList = new JArrayList[String]()
    emptyList.flatMap(s => Iterator(s, s)).toList shouldBe empty
  }

  it should "handle flatMap with non-empty list" in {
    val list = new JArrayList[String]()
    list.add("1")
    list.add("2")

    list.flatMap(s => Iterator(s, s)).toList shouldBe List("1", "1", "2", "2")
  }

  it should "handle flatMap with empty iterators" in {
    val list = new JArrayList[String]()
    list.add("1")
    list.add("2")

    list.flatMap(_ => Iterator.empty).toList shouldBe empty
  }

  it should "handle flatMap with mixed empty and non-empty iterators" in {
    val list = new JArrayList[String]()
    list.add("1")
    list.add("2")
    list.add("3")

    val result = list
      .flatMap(s =>
        if (s.toInt % 2 == 0) Iterator(s, s)
        else Iterator.empty)
      .toList

    result shouldBe List("2", "2")
  }

  // TODO: To make implicit distinct function working for iterator
  "IteratorExtensions" should "handle distinct with null iterator" ignore {
    val nullIterator: Iterator[String] = null
    nullIterator.distinct shouldBe empty
  }

  it should "handle distinct with empty iterator" in {
    val emptyIterator: Iterator[String] = Iterator.empty
    emptyIterator.distinct shouldBe empty
  }

  it should "handle distinct with non-empty iterator containing duplicates" in {
    val iterator = Iterator("1", "2", "1", "3", "2", "3")
    iterator.distinct.toSeq.sorted shouldBe Seq("1", "2", "3")
  }

  it should "handle distinct with non-empty iterator containing no duplicates" in {
    val iterator = Iterator("1", "2", "3")
    iterator.distinct.toSeq.sorted shouldBe Seq("1", "2", "3")
  }

  it should "handle distinct with complex objects" ignore {
    case class TestClass(id: Int, name: String)

    val obj1 = TestClass(1, "one")
    val obj2 = TestClass(2, "two")
    val iterator = Iterator(obj1, obj2, obj1)
    val distinctObjs = iterator.distinct
    distinctObjs should have length 2
    distinctObjs shouldBe Seq(obj1, obj2)
  }

  "JMapExtension" should "handle safeGet with null map" in {
    val nullMap: java.util.Map[String, String] = null
    nullMap.safeGet("key") shouldBe None
    nullMap.safeGet("key", "default") shouldBe Some("default")
  }

  it should "handle safeGet with empty map" in {
    val emptyMap = new java.util.HashMap[String, String]()
    emptyMap.safeGet("nonexistent") shouldBe None
    emptyMap.safeGet("nonexistent", "default") shouldBe Some("default")
  }

  it should "handle safeGet with existing key" in {
    val map = new java.util.HashMap[String, String]()
    map.put("key", "value")
    map.safeGet("key") shouldBe Some("value")
    map.safeGet("key", "default") shouldBe Some("value")
  }

  it should "handle safeGet with null value in map" in {
    val map = new java.util.HashMap[String, String]()
    map.put("nullKey", null)
    map.safeGet("nullKey") shouldBe None
    map.safeGet("nullKey", "default") shouldBe Some("default")
  }

  it should "handle safeGet with different value types" in {
    val map = new java.util.HashMap[String, Integer]()
    map.put("num", 42)
    map.safeGet("num") shouldBe Some(42)
    map.safeGet("num", 0) shouldBe Some(42)
    map.safeGet("nonexistent", 0) shouldBe Some(0)
  }
}
