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

package ai.chronon.aggregator.windowing

import ai.chronon.api.Extensions.WindowOps
import ai.chronon.api.Extensions.WindowUtils
import ai.chronon.api.GroupBy
import ai.chronon.api.ScalaJavaConversions._
import ai.chronon.api.TimeUnit
import ai.chronon.api.Window
import org.slf4j.{Logger, LoggerFactory}

trait Resolution extends Serializable {
  // For a given window what is the resolution of the tail
  // The tail hops with the window size as represented by the return value
  def calculateTailHop(window: Window): Long

  // What are the hops that we will use to tile the full window
  // 1. Need to be sorted in descending order, and
  // 2. Every element needs to be a multiple of the next one
  // 3. calculateTailHop needs to return values only from this.
  val hopSizes: Array[Long]

  // Name for configuration lookup, derived from class name
  def name: String = this.getClass.getSimpleName.stripSuffix("$")
}

object FiveMinuteResolution extends Resolution {
  def calculateTailHop(window: Window): Long =
    window.millis match {
      case x if x >= new Window(12, TimeUnit.DAYS).millis  => WindowUtils.Day.millis
      case x if x >= new Window(12, TimeUnit.HOURS).millis => WindowUtils.Hour.millis
      case _                                               => WindowUtils.FiveMinutes
    }

  val hopSizes: Array[Long] =
    Array(WindowUtils.Day.millis, WindowUtils.Hour.millis, WindowUtils.FiveMinutes)
}

/** OneMinuteResolution provides 1-minute precision for windows < 2 hours.
  *
  * Trade-offs compared to FiveMinuteResolution:
  * - 5x better precision for windows < 2h (±1 min vs ±5 min)
  * - 5x more storage/tiles for windows < 2h
  * - 5x more streaming writes if GroupBy has windows < 2h
  * - Same behavior as FiveMinuteResolution for windows >= 2h
  *
  * Set via the `resolution` field in GroupBy config (e.g., resolution="OneMinuteResolution" in Python API).
  * Resolved at runtime by ResolutionUtils.getResolutionByName(groupBy.resolution).
  */
object OneMinuteResolution extends Resolution {
  def calculateTailHop(window: Window): Long =
    window.millis match {
      case x if x >= new Window(12, TimeUnit.DAYS).millis  => WindowUtils.Day.millis
      case x if x >= new Window(12, TimeUnit.HOURS).millis => WindowUtils.Hour.millis
      case x if x >= new Window(2, TimeUnit.HOURS).millis  => WindowUtils.FiveMinutes
      case _                                               => WindowUtils.Minute
    }

  val hopSizes: Array[Long] =
    Array(WindowUtils.Day.millis, WindowUtils.Hour.millis, WindowUtils.FiveMinutes, WindowUtils.Minute)
}

object DailyResolution extends Resolution {

  def calculateTailHop(window: Window): Long =
    window.timeUnit match {
      case TimeUnit.DAYS => WindowUtils.Day.millis
      case _ =>
        throw new IllegalArgumentException(
          s"Invalid request for window $window for daily aggregation. " +
            "Window can only be multiples of 1d or the operation needs to be un-windowed."
        )
    }

  val hopSizes: Array[Long] = Array(WindowUtils.Day.millis)
}

object ResolutionUtils {

  @transient private implicit lazy val logger: Logger = LoggerFactory.getLogger(getClass)
  val DefaultResolution: Resolution = FiveMinuteResolution

  private val registry: Map[String, Resolution] = Seq(
    FiveMinuteResolution,
    OneMinuteResolution,
    DailyResolution
  ).map(r => r.name -> r).toMap

  /** Get a resolution by name. Useful for configuration-driven resolution selection.
    * @param name The resolution name (e.g., "FiveMinuteResolution", "OneMinuteResolution").
    *             If null or not found, returns DefaultResolution.
    * @return The corresponding Resolution, or DefaultResolution if not found
    */
  def getResolutionByName(name: String): Resolution = {
    val r = registry.getOrElse(name, DefaultResolution)
    logger.info(s"Using resolution: ${r.name} for name: $name")
    r
  }

  /** Find the smallest tail window resolution in a GroupBy. Returns 1D if the GroupBy does not define any windows (all-time aggregates).
    * Resolution is extracted from the GroupBy's resolution field.
    *
    * @param groupBy The GroupBy configuration
    * @return The smallest tail hop size in milliseconds
    */
  def getSmallestTailHopMillis(groupBy: GroupBy): Long = {
    val resolution = getResolutionByName(groupBy.resolution)

    val tailHops =
      for (
        aggs <- Option(groupBy.aggregations).toSeq;
        agg <- aggs.iterator().toScala;
        windows <- Option(agg.windows).toSeq;
        window <- windows.iterator().toScala
      ) yield {
        resolution.calculateTailHop(window)
      }

    if (tailHops.isEmpty) WindowUtils.Day.millis
    else tailHops.min

  }
}
