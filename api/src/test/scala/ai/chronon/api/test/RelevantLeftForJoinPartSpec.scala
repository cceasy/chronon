package ai.chronon.api.test

import ai.chronon.api
import ai.chronon.api.Builders._
import ai.chronon.api.planner.RelevantLeftForJoinPart
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers


class RelevantLeftForJoinPartSpec extends AnyFlatSpec with Matchers {

  // Helper method to create a basic GroupBy setup
  private def createGroupBy(
      name: String = "team1.cohorts",
      selects: Map[String, String] = Map("user_id" -> "user_id"),
      keyColumns: Seq[String] = Seq("user_id"),
      wheres: Seq[String] = null
  ): api.GroupBy = {
    val metadata = MetaData(
      name = name,
      team = "team1"
    )

    val query = Query(
      selects = selects,
      wheres = wheres
    )

    val source = Source.events(
      query = query,
      table = "team1.events"
    )

    GroupBy(
      metaData = metadata,
      sources = Seq(source),
      keyColumns = keyColumns
    )
  }

  // Helper method to create a basic join setup
  private def createBasicJoin(
      groupBy: api.GroupBy,
      leftTableName: String = "team1.events",
      leftSelects: Map[String, String] = Map("user_id" -> "user_id"),
      leftWheres: Seq[String] = null,
      joinName: String = "test_join",
      prefix: String = null,
      leftStart: String = "2024-01-01"
  ): (api.Join, api.JoinPart) = {
    val query = Query(
      selects = leftSelects,
      wheres = leftWheres,
      startPartition = leftStart
    )

    val source = Source.events(
      query = query,
      table = leftTableName
    )

    val metadata = MetaData(
      name = joinName,
      team = "team1",
      namespace = "output_ns"
    )

    val joinPart = JoinPart(
      groupBy = groupBy,
      keyMapping = Map("user_id" -> "user_id"),
      prefix = prefix
    )

    val join = Join(
      metaData = metadata,
      left = source,
      joinParts = Seq(joinPart)
    )

    (join, joinPart)
  }

  "partTableName" should "remain stable when adding unrelated left select columns" in {
    val groupBy = createGroupBy()

    val (baseJoin, joinPart) = createBasicJoin(
      groupBy = groupBy,
      leftSelects = Map("user_id" -> "user_id", "ts" -> "timestamp")
    )

    val (joinWithExtraSelects, _) = createBasicJoin(
      groupBy = groupBy,
      leftSelects = Map(
        "user_id" -> "user_id",
        "ts" -> "timestamp",
        "extra_field" -> "some_value" // Additional unrelated select
      )
    )

    val baseTableName = RelevantLeftForJoinPart.fullPartTableName(baseJoin, joinPart)
    val extraSelectsTableName = RelevantLeftForJoinPart.fullPartTableName(joinWithExtraSelects, joinPart)

    baseTableName shouldEqual extraSelectsTableName
  }

  it should "remain stable when changing start date on the left side" in {
    val groupBy = createGroupBy()

    val (baseJoin, joinPart) = createBasicJoin(
      groupBy = groupBy,
      leftStart = "2024-01-01"
    )

    val (joinWithDifferentDate, _) = createBasicJoin(
      groupBy = groupBy,
      leftStart = "2024-02-01" // Different start date
    )

    val baseTableName = RelevantLeftForJoinPart.fullPartTableName(baseJoin, joinPart)
    val differentDateTableName = RelevantLeftForJoinPart.fullPartTableName(joinWithDifferentDate, joinPart)

    baseTableName shouldEqual differentDateTableName
  }

  it should "change when the right side (GroupBy) has different key columns" in {
    val baseGroupBy = createGroupBy(
      selects = Map("user_id" -> "user_id", "client_id" -> "client_id", "activity" -> "COUNT(*)"),
      keyColumns = Seq("user_id")
    )

    val modifiedGroupBy = createGroupBy(
      selects = Map("user_id" -> "user_id", "client_id" -> "client_id", "activity" -> "COUNT(*)"),
      keyColumns = Seq("user_id", "client_id") // Additional key column
    )

    val (baseJoin, baseJoinPart) = createBasicJoin(groupBy = baseGroupBy)
    val (modifiedJoin, modifiedJoinPart) = createBasicJoin(groupBy = modifiedGroupBy)

    val baseTableName = RelevantLeftForJoinPart.fullPartTableName(baseJoin, baseJoinPart)
    val modifiedTableName = RelevantLeftForJoinPart.fullPartTableName(modifiedJoin, modifiedJoinPart)

    baseTableName should not equal modifiedTableName
  }

  it should "change when the right side (GroupBy) has different selects" in {
    val baseGroupBy = createGroupBy(
      selects = Map("user_id" -> "user_id", "activity" -> "COUNT(*)"),
      keyColumns = Seq("user_id")
    )

    val modifiedGroupBy = createGroupBy(
      selects = Map("user_id" -> "user_id", "activity" -> "SUM(value)"), // Different aggregation
      keyColumns = Seq("user_id")
    )

    val (baseJoin, baseJoinPart) = createBasicJoin(groupBy = baseGroupBy)
    val (modifiedJoin, modifiedJoinPart) = createBasicJoin(groupBy = modifiedGroupBy)

    val baseTableName = RelevantLeftForJoinPart.fullPartTableName(baseJoin, baseJoinPart)
    val modifiedTableName = RelevantLeftForJoinPart.fullPartTableName(modifiedJoin, modifiedJoinPart)

    baseTableName should not equal modifiedTableName
  }

  it should "change when the right side (GroupBy) has different where clauses" in {
    val baseGroupBy = createGroupBy(
      wheres = Seq("value > 0"),
      keyColumns = Seq("user_id")
    )

    val modifiedGroupBy = createGroupBy(
      wheres = Seq("value > 10"), // Different filter condition
      keyColumns = Seq("user_id")
    )

    val (baseJoin, baseJoinPart) = createBasicJoin(groupBy = baseGroupBy)
    val (modifiedJoin, modifiedJoinPart) = createBasicJoin(groupBy = modifiedGroupBy)

    val baseTableName = RelevantLeftForJoinPart.fullPartTableName(baseJoin, baseJoinPart)
    val modifiedTableName = RelevantLeftForJoinPart.fullPartTableName(modifiedJoin, modifiedJoinPart)

    baseTableName should not equal modifiedTableName
  }

  it should "not change with new join name but same left source table" in {
    val groupBy = createGroupBy()

    val (join1, joinPart) = createBasicJoin(
      groupBy = groupBy,
      joinName = "test_join_1"
    )

    val (join2, _) = createBasicJoin(
      groupBy = groupBy,
      joinName = "test_join_2" // Different join name
    )

    val tableName1 = RelevantLeftForJoinPart.fullPartTableName(join1, joinPart)
    val tableName2 = RelevantLeftForJoinPart.fullPartTableName(join2, joinPart)

    tableName1 shouldEqual tableName2
  }

  it should "handle prefix in join part correctly" in {
    val groupBy = createGroupBy()

    val (joinWithPrefix, joinPartWithPrefix) = createBasicJoin(
      groupBy = groupBy,
      prefix = "test_prefix"
    )

    val (joinWithoutPrefix, joinPartWithoutPrefix) = createBasicJoin(
      groupBy = groupBy
    )

    val tableNameWithPrefix = RelevantLeftForJoinPart.fullPartTableName(joinWithPrefix, joinPartWithPrefix)
    val tableNameWithoutPrefix = RelevantLeftForJoinPart.fullPartTableName(joinWithoutPrefix, joinPartWithoutPrefix)

    tableNameWithPrefix should not equal tableNameWithoutPrefix
    tableNameWithPrefix should include("test_prefix__")
  }
}
