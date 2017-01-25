package com.svitovyda.timeseries

import com.svitovyda.timeseries.TimeSeries._
import org.scalatest.{FlatSpec, Matchers}

import scala.util.{Failure, Success}

class TimeSeriesSpec extends FlatSpec with Matchers {

  it should "correctly validate Item" in {
    Item.validate("123 1.5") shouldBe Success(Item(123, 1.5))
    Item.validate("  345 \t \t 0.3 ") shouldBe Success(Item(345, 0.3))
    Item.validate("0 6.") shouldBe Success(Item(0, 6))

    Item.validate("1 2 3 1 . 5") shouldBe a[Failure[_]]
    Item.validate("a b") shouldBe a[Failure[_]]
    Item.validate("1.1 3") shouldBe a[Failure[_]]
    Item.validate("a 1") shouldBe a[Failure[_]]
    Item.validate("2 b") shouldBe a[Failure[_]]
    Item.validate("1 1b") shouldBe a[Failure[_]]
    Item.validate("3 3.3.3") shouldBe a[Failure[_]]
    Item.validate("4") shouldBe a[Failure[_]]
    Item.validate("") shouldBe a[Failure[_]]
    Item.validate("7 4,5.7") shouldBe a[Failure[_]]
  }

  it should "correctly update Window (including big time gaps)" in {
    Window() shouldBe Window(Nil)
    val window1 = Window().update(Item(2, 3))
    window1 shouldBe Window(List(Item(2, 3)))
    window1.min shouldBe 3
    window1.max shouldBe 3
    window1.count shouldBe 1
    window1.sum shouldBe 3
    window1.endMeasurement shouldBe 3
    window1.endTime shouldBe 2

    val window2 = window1.update(Item(3, 5)).update(Item(4, 1))
    window2 shouldBe Window(List(Item(4, 1), Item(3, 5), Item(2, 3)))
    window2.min shouldBe 1
    window2.max shouldBe 5
    window2.count shouldBe 3
    window2.sum shouldBe 9
    window2.endMeasurement shouldBe 1
    window2.endTime shouldBe 4

    val window3 = window2.update(Item(5000, 1))
    window3 shouldBe Window(List(Item(5000, 1)))
    window3.min shouldBe 1
    window3.max shouldBe 1
    window3.count shouldBe 1
    window3.sum shouldBe 1
    window3.endMeasurement shouldBe 1
    window3.endTime shouldBe 5000
  }

  def getInputData = Iterator(
    "begin",
    "1355270609\t1.80215",
    "1355270621\t1.80185",
    "1355270646\t1.80195",
    "1355270702\t1.80225",
    "aaa",
    "1355270702\t1.80215",
    "1355270829\t1.80235",
    "1355270854\t1.80205",
    "1355270868\t1.80225",
    "bbb",
    "1355271000\t1.80245",
    "1355271023\t1.80285",
    "1355271024\t1.80275",
    "1355271026\t1.80285",
    "1355271027\t1.80265",
    "2 3 43 5 6 ",
    "1355271056\t1.80275",
    "1355271428\t1.80265",
    "1355271466\t1.80275",
    "1355271471\t1.80295",
    "1355271507\t1.80265",
    "3, h",
    "1355271562\t1.80275",
    "1355271588\t1.80295",
    "end"
  )

  def getResult = Iterator(
    "1355270609 1,80215  1  1,80215 1,80215 1,80215",
    "1355270621 1,80185  2  3,60400 1,80185 1,80215",
    "1355270646 1,80195  3  5,40595 1,80185 1,80215",
    "1355270702 1,80225  2  3,60420 1,80195 1,80225",
    "1355270702 1,80215  3  5,40635 1,80195 1,80225",
    "1355270829 1,80235  1  1,80235 1,80235 1,80235",
    "1355270854 1,80205  2  3,60440 1,80205 1,80235",
    "1355270868 1,80225  3  5,40665 1,80205 1,80235",
    "1355271000 1,80245  1  1,80245 1,80245 1,80245",
    "1355271023 1,80285  2  3,60530 1,80245 1,80285",
    "1355271024 1,80275  3  5,40805 1,80245 1,80285",
    "1355271026 1,80285  4  7,21090 1,80245 1,80285",
    "1355271027 1,80265  5  9,01355 1,80245 1,80285",
    "1355271056 1,80275  6 10,81630 1,80245 1,80285",
    "1355271428 1,80265  1  1,80265 1,80265 1,80265",
    "1355271466 1,80275  2  3,60540 1,80265 1,80275",
    "1355271471 1,80295  3  5,40835 1,80265 1,80295",
    "1355271507 1,80265  3  5,40835 1,80265 1,80295",
    "1355271562 1,80275  2  3,60540 1,80265 1,80275",
    "1355271588 1,80295  2  3,60570 1,80275 1,80295"
  )

  it should "correctly tailrec iterate (ignoring incorrect data)" in {
    val input = getInputData
    val result = getResult
    TimeSeries.iterate(input) { w =>
      w.toFormattedString shouldBe result.next()
    }
  }

  it should "correctly RollingWindowIterator iterate (ignoring incorrect data)" in {
    val input = getInputData
    val result = getResult
    val it = RollingWindowIterator(input)
    while (it.hasNext) {
      it.next().foreach(w => w.toFormattedString shouldBe result.next())
    }
  }
}
