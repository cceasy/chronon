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

package ai.chronon.aggregator.test

import ai.chronon.aggregator.windowing._
import ai.chronon.api.Extensions.WindowOps
import ai.chronon.api.Extensions.WindowUtils
import ai.chronon.api.{TimeUnit, Window}
import org.junit.Assert._
import org.scalatest.flatspec.AnyFlatSpec

class ResolutionTest extends AnyFlatSpec {

  "FiveMinuteResolution" should "calculate correct tail hops" in {
    // Windows < 12h use 5min hop
    assertEquals(WindowUtils.FiveMinutes, FiveMinuteResolution.calculateTailHop(new Window(1, TimeUnit.HOURS)))
    assertEquals(WindowUtils.FiveMinutes, FiveMinuteResolution.calculateTailHop(new Window(2, TimeUnit.HOURS)))
    assertEquals(WindowUtils.FiveMinutes, FiveMinuteResolution.calculateTailHop(new Window(11, TimeUnit.HOURS)))

    // Windows >= 12h and < 12d use 1hr hop
    assertEquals(WindowUtils.Hour.millis, FiveMinuteResolution.calculateTailHop(new Window(12, TimeUnit.HOURS)))
    assertEquals(WindowUtils.Hour.millis, FiveMinuteResolution.calculateTailHop(new Window(1, TimeUnit.DAYS)))
    assertEquals(WindowUtils.Hour.millis, FiveMinuteResolution.calculateTailHop(new Window(11, TimeUnit.DAYS)))

    // Windows >= 12d use 1d hop
    assertEquals(WindowUtils.Day.millis, FiveMinuteResolution.calculateTailHop(new Window(12, TimeUnit.DAYS)))
    assertEquals(WindowUtils.Day.millis, FiveMinuteResolution.calculateTailHop(new Window(30, TimeUnit.DAYS)))
  }

  "OneMinuteResolution" should "calculate correct tail hops" in {
    // Windows < 2h use 1min hop
    assertEquals(WindowUtils.Minute, OneMinuteResolution.calculateTailHop(new Window(30, TimeUnit.MINUTES)))
    assertEquals(WindowUtils.Minute, OneMinuteResolution.calculateTailHop(new Window(1, TimeUnit.HOURS)))

    // Windows >= 2h and < 12h use 5min hop
    assertEquals(WindowUtils.FiveMinutes, OneMinuteResolution.calculateTailHop(new Window(2, TimeUnit.HOURS)))
    assertEquals(WindowUtils.FiveMinutes, OneMinuteResolution.calculateTailHop(new Window(3, TimeUnit.HOURS)))
    assertEquals(WindowUtils.FiveMinutes, OneMinuteResolution.calculateTailHop(new Window(6, TimeUnit.HOURS)))
    assertEquals(WindowUtils.FiveMinutes, OneMinuteResolution.calculateTailHop(new Window(11, TimeUnit.HOURS)))

    // Windows >= 12h and < 12d use 1hr hop (same as FiveMinuteResolution)
    assertEquals(WindowUtils.Hour.millis, OneMinuteResolution.calculateTailHop(new Window(12, TimeUnit.HOURS)))
    assertEquals(WindowUtils.Hour.millis, OneMinuteResolution.calculateTailHop(new Window(1, TimeUnit.DAYS)))
    assertEquals(WindowUtils.Hour.millis, OneMinuteResolution.calculateTailHop(new Window(11, TimeUnit.DAYS)))

    // Windows >= 12d use 1d hop (same as FiveMinuteResolution)
    assertEquals(WindowUtils.Day.millis, OneMinuteResolution.calculateTailHop(new Window(12, TimeUnit.DAYS)))
    assertEquals(WindowUtils.Day.millis, OneMinuteResolution.calculateTailHop(new Window(30, TimeUnit.DAYS)))
  }

  "OneMinuteResolution" should "have correct hopSizes array" in {
    val hopSizes = OneMinuteResolution.hopSizes
    assertEquals(4, hopSizes.length)
    assertEquals(WindowUtils.Day.millis, hopSizes(0))
    assertEquals(WindowUtils.Hour.millis, hopSizes(1))
    assertEquals(WindowUtils.FiveMinutes, hopSizes(2))
    assertEquals(WindowUtils.Minute, hopSizes(3))

    // Verify divisibility constraint
    assertTrue("Day should be divisible by Hour", hopSizes(0) % hopSizes(1) == 0)
    assertTrue("Hour should be divisible by FiveMinutes", hopSizes(1) % hopSizes(2) == 0)
    assertTrue("FiveMinutes should be divisible by Minute", hopSizes(2) % hopSizes(3) == 0)
  }

  "ResolutionUtils.getResolutionByName" should "return correct resolution for known names" in {
    assertEquals(FiveMinuteResolution, ResolutionUtils.getResolutionByName("FiveMinuteResolution"))
    assertEquals(OneMinuteResolution, ResolutionUtils.getResolutionByName("OneMinuteResolution"))
    assertEquals(DailyResolution, ResolutionUtils.getResolutionByName("DailyResolution"))
  }

  it should "return DefaultResolution for unknown names" in {
    assertEquals(ResolutionUtils.DefaultResolution, ResolutionUtils.getResolutionByName("UnknownResolution"))
    assertEquals(ResolutionUtils.DefaultResolution, ResolutionUtils.getResolutionByName(""))
  }

  it should "return DefaultResolution for null name" in {
    assertEquals(ResolutionUtils.DefaultResolution, ResolutionUtils.getResolutionByName(null))
  }

  it should "default to FiveMinuteResolution" in {
    // DefaultResolution should be FiveMinuteResolution
    assertEquals(FiveMinuteResolution, ResolutionUtils.DefaultResolution)
    // null/missing config should behave identically to FiveMinuteResolution
    val defaultRes = ResolutionUtils.getResolutionByName(null)
    assertEquals(FiveMinuteResolution.hopSizes.toSeq, defaultRes.hopSizes.toSeq)
    assertEquals(
      FiveMinuteResolution.calculateTailHop(new Window(1, TimeUnit.HOURS)),
      defaultRes.calculateTailHop(new Window(1, TimeUnit.HOURS))
    )
  }

  it should "return a resolution with correct behavior for OneMinuteResolution" in {
    val res = ResolutionUtils.getResolutionByName("OneMinuteResolution")
    // 1h window should use 1min hop (not 5min)
    assertEquals(WindowUtils.Minute, res.calculateTailHop(new Window(1, TimeUnit.HOURS)))
    // 6h window should use 5min hop
    assertEquals(WindowUtils.FiveMinutes, res.calculateTailHop(new Window(6, TimeUnit.HOURS)))
    // hopSizes should have 4 levels
    assertEquals(4, res.hopSizes.length)
  }

  "OneMinuteResolution" should "align with FiveMinuteResolution for windows >= 2h" in {
    // For all windows >= 2h, OneMinuteResolution should give same result as FiveMinuteResolution
    val testWindows = Seq(
      new Window(2, TimeUnit.HOURS),
      new Window(3, TimeUnit.HOURS),
      new Window(6, TimeUnit.HOURS),
      new Window(12, TimeUnit.HOURS),
      new Window(1, TimeUnit.DAYS),
      new Window(7, TimeUnit.DAYS),
      new Window(12, TimeUnit.DAYS),
      new Window(30, TimeUnit.DAYS)
    )

    testWindows.foreach { window =>
      assertEquals(
        s"Mismatch for window ${window.getLength}${window.getTimeUnit}",
        FiveMinuteResolution.calculateTailHop(window),
        OneMinuteResolution.calculateTailHop(window)
      )
    }
  }

  "Resolution.name" should "return correct names" in {
    assertEquals("FiveMinuteResolution", FiveMinuteResolution.name)
    assertEquals("OneMinuteResolution", OneMinuteResolution.name)
    assertEquals("DailyResolution", DailyResolution.name)
  }
}
