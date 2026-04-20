package ai.chronon.spark.extensions

import java.util.Locale
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.{DeleteAction, InsertAction, LogicalPlan, MergeIntoTable}
import org.apache.spark.sql.types.{DataType, StructType}

/** Spark session extension that adds support for WHEN NOT MATCHED BY SOURCE in MERGE INTO
  * on Spark 3.5 (natively supported in Spark 4.0+).
  *
  * The execution engine in Spark 3.5 already handles notMatchedBySourceActions on MergeIntoTable —
  * only the parser lacks support for the syntax. This extension wraps the parser, intercepts
  * MERGE statements containing "NOT MATCHED BY SOURCE", strips that clause, parses the rest
  * with the delegate parser, then programmatically adds the notMatchedBySourceActions to the plan.
  */
class ChrononMergeExtension extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectParser((session, delegate) => new MergeBySourceParser(session, delegate))
  }
}

private[extensions] class MergeBySourceParser(session: SparkSession, delegate: ParserInterface)
    extends ParserInterface {

  import MergeBySourceParser._

  override def parsePlan(sqlText: String): LogicalPlan = {
    val upper = sqlText.toUpperCase(Locale.ROOT).replaceAll("\\s+", " ")
    if (!upper.contains("NOT MATCHED BY SOURCE")) {
      return delegate.parsePlan(sqlText)
    }

    // Extract the WHEN NOT MATCHED BY SOURCE clause and parse the rest normally
    val (strippedSql, deleteCondition) = extractNotMatchedBySource(sqlText)
    val basePlan = delegate.parsePlan(strippedSql)

    basePlan match {
      case merge: MergeIntoTable =>
        val conditionExpr = deleteCondition.map { cond =>
          delegate.parseExpression(cond)
        }
        val deleteAction = DeleteAction(conditionExpr)
        merge.copy(notMatchedBySourceActions = Seq(deleteAction))
      case other => other
    }
  }

  // Delegate all other parser methods
  override def parseExpression(sqlText: String): Expression = delegate.parseExpression(sqlText)
  override def parseTableIdentifier(sqlText: String): TableIdentifier = delegate.parseTableIdentifier(sqlText)
  override def parseFunctionIdentifier(sqlText: String): FunctionIdentifier =
    delegate.parseFunctionIdentifier(sqlText)
  override def parseMultipartIdentifier(sqlText: String): Seq[String] = delegate.parseMultipartIdentifier(sqlText)
  override def parseTableSchema(sqlText: String): StructType = delegate.parseTableSchema(sqlText)
  override def parseDataType(sqlText: String): DataType = delegate.parseDataType(sqlText)
  override def parseQuery(sqlText: String): LogicalPlan = delegate.parseQuery(sqlText)
}

private[extensions] object MergeBySourceParser {

  /** Extracts the WHEN NOT MATCHED BY SOURCE ... THEN DELETE clause from a MERGE statement.
    * Returns (sql without that clause, optional delete condition).
    */
  def extractNotMatchedBySource(sql: String): (String, Option[String]) = {
    // Match: WHEN NOT MATCHED BY SOURCE [AND <condition>] THEN DELETE
    val pattern =
      """(?is)\s*WHEN\s+NOT\s+MATCHED\s+BY\s+SOURCE\s+(?:AND\s+(.+?)\s+)?THEN\s+DELETE""".r

    pattern.findFirstMatchIn(sql) match {
      case Some(m) =>
        val stripped = sql.substring(0, m.start) + sql.substring(m.end)
        val condition = Option(m.group(1)).map(_.trim)
        (stripped, condition)
      case None =>
        (sql, None)
    }
  }
}
