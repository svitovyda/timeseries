package com.svitovyda.timeseries

import com.svitovyda.timeseries.TimeSeries.{CurrentWindow, _}
import org.scalatest.{FlatSpec, Matchers}

class TimeSeriesSpec extends FlatSpec with Matchers {

  it should "Window not be valid for invalid data and be updated correctly" in {
    val window = Window()
    window.isValid shouldBe false
    val window1 = window.update(1, 5.6)
    window1 shouldBe Window(1, 5.6, 1, 5.6, 5.6, 5.6)
    val window2 = window1.update(3, 4.4)
    window2 shouldBe Window(3, 4.4, 2, 4.4, 5.6, 10)
    val window3 = window2.update(5, 10)
    window3 shouldBe Window(5, 10, 3, 4.4, 10, 20)
    window3.isValid shouldBe true
  }

  it should "correctly update state" in {
    val state = (1001 to 1020).foldLeft(CurrentWindow()) { case (z, time) =>
      z.update(time, 2, 10)
    }
    state shouldBe CurrentWindow(Window(1020, 2, 10, 2, 2, 20), 1021, None)

    val stateOne = (1001 to 1011).foldLeft(CurrentWindow()) { case (z, time) =>
      z.update(time, 2, 10)
    }
    stateOne shouldBe CurrentWindow(
      window = Window(1011, 2, 1, 2, 2, 2),
      endTime = 1021,
      previousResult = Some(Window(1010, 2, 10, 2, 2, 20).toFormattedString)
    )
  }

  it should "correctly iterate" in {
    val data = List(
      "1001 3",
      "1010 5",

      "1085 8",
      "sfks skhfd",

      "1106 .1",
      "1112 3.3.3",
      "1114  2.",

      "1124\t 50"
    )
    val state = TimeSeries.iterate(data.iterator, 10)
    state shouldBe CurrentWindow(
      window = Window(
        endTime = 1124,
        endMeasurement = 50,
        count = 1,
        minPrice = 50,
        maxPrice = 50,
        sum = 50
      ),
      endTime = 1126,
      previousResult = Some(Window(
        endTime = 1114,
        endMeasurement = 2,
        count = 2,
        minPrice = 0.1,
        maxPrice = 2,
        sum = 2.1
      ).toFormattedString)
    )
  }

}
