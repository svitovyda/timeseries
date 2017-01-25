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

  case class Window(items: List[Item] = Nil) {
    def sum: Double = items.map(_.measurement).sum
    def min: Double = items.map(_.measurement).min
    def max: Double = items.map(_.measurement).max
    def count: Int = items.length
    def endMeasurement: Double = items.head.measurement
    def endTime: Long = items.head.time

    def update(item: Item): Window = Window(item :: items.takeWhile(_.time > item.time - WindowSize))

    override def toString: String = s"$endTime $endMeasurement $count $sum $min $max"
    def toFormattedString: String = f"$endTime $endMeasurement%.5f $count%2d $sum%8.5f $min%.5f $max%.5f"
  }
  object Window {
    def apply(item: Item): Window = new Window(List(item))
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

    readFile.recover { case e: Throwable => println(e) }
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
