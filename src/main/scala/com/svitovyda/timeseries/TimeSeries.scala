package com.svitovyda.timeseries

import scala.annotation.tailrec
import scala.io.{BufferedSource, Source}
import scala.util.{Failure, Try}

object TimeSeries {

  val WindowSize: Int = 60
  private val LinePattern = "[\\t ]*(\\d+)[\\t ]+([\\d.]+)[\\t ]*".r

  case class Item(time: Long, measurement: Double)
  object Item {
    def validate(line: String): Try[Item] = line match {
      case LinePattern(t, m) =>
        for {
          time <- Try {t.toLong}
          measurement <- Try {m.toDouble}
        } yield Item(time, measurement)
      case _                 =>
        Failure(new RuntimeException(s"Could not parse line: $line"))
    }
  }

  case class Window(
    sum: Double = 0,
    min: Double = Double.MaxValue,
    max: Double = Double.MinValue,
    count: Int = 0,
    items: List[Item] = Nil
  ) {
    def endMeasurement: Double = items.headOption.map(_.measurement).getOrElse(0)
    def endTime: Long = items.headOption.map(_.time).getOrElse(0)

    def +(item: Item): Window = Window(
      sum = sum + item.measurement,
      min = math.min(min, item.measurement),
      max = math.max(max, item.measurement),
      count = count + 1,
      items = item :: items
    )
    def update(item: Item): Window = {
      val timeTill = item.time - WindowSize
      items.foldLeft(Window()) {
        case (w, i) if i.time > timeTill => w + i
        case (w, _)                      => w
      } + item
    }

    def toSimpleString: String = s"$endTime $endMeasurement $count $sum $min $max"
    def toFormattedString: String = f"$endTime $endMeasurement%.5f $count%2d $sum%8.5f $min%.5f $max%.5f"
  }
  object Window {
    private def apply(item: Item): Window = new Window(
      sum = item.measurement,
      min = item.measurement,
      max = item.measurement,
      count = 1,
      List(item)
    )
  }

  private def printCaption() = {
    println("T          V        N  RS      MinV    MaxV")
    1 to 50 foreach { _ => print("-") }
    println()
  }

  private def trace(window: Window): Unit = {
    println(window.toFormattedString)
  }

  def processFile(fileName: String): Unit = {
    val readFile = Try {Source.fromFile(fileName)} map { file: BufferedSource =>
      printCaption()

      iterate(file.getLines())(trace)
      /*  alternatively:
      val it: RollingWindowIterator = RollingWindowIterator(file.getLines())
      for (window <- it) window.foreach(trace)
      */
      file.close()
    }

    readFile.recover { case e: Throwable =>
      println(e)
    }
  }

  case class RollingWindowIterator(iterator: Iterator[String]) extends Iterator[Try[Window]] {
    private var window = Window()
    override def hasNext: Boolean = iterator.hasNext
    override def next(): Try[Window] = Item.validate(iterator.next()).map { item =>
      window = window.update(item)
      window
    }
  }

  @tailrec
  def iterate(iterator: Iterator[String], window: Window = Window())(f: (Window) => Unit): Unit = {
    if (iterator.hasNext) {
      val next = Item.validate(iterator.next()).map { item =>
        val w = window.update(item)
        f(w)
        w
      }.getOrElse(window)

      iterate(iterator, next)(f)
    }
  }

}
