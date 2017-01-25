package com.svitovyda.timeseries

import scala.io.{BufferedSource, Source}
import scala.util.Try

object TimeSeries {

  val WindowSize: Int = 60
  private val Pattern = "(\\d+)[\\t ]+([\\d.]+)".r

  case class Window(
    endTime: Long = 0,
    endMeasurement: Double = 0,
    count: Int = 0,
    minPrice: Double = Double.MaxValue,
    maxPrice: Double = Double.MinValue,
    sum: Double = 0
  ) {
    def update(time: Long, measurement: Double): Window = Window(
      endTime = time,
      endMeasurement = measurement,
      count = count + 1,
      minPrice = math.min(minPrice, measurement),
      maxPrice = math.max(maxPrice, measurement),
      sum = sum + measurement
    )

    def isValid: Boolean = endTime > 0 &&
      endMeasurement > 0 &&
      count > 0 &&
      minPrice > 0 &&
      minPrice < Double.MaxValue &&
      maxPrice > 0 &&
      sum > 0

    override def toString: String = s"$endTime $endMeasurement $count $sum $minPrice $maxPrice"
    def toFormattedString: String = toString //f"$endTime $endMeasurement% 8.5f $count% 3d $sum% 9.5f $minPrice% 8.5f $maxPrice% 8.5f"
  }
  object Window {
    def apply(time: Long, measurement: Double): Window = new Window(
      endTime = time,
      endMeasurement = measurement,
      count = 1,
      minPrice = measurement,
      maxPrice = measurement,
      sum = measurement
    )
  }

  case class CurrentWindow(
    window: Window = Window(),
    endTime: Long = 0,
    previousResult: Option[String] = None
  ) {
    def update(time: Long, measurement: Double, windowSize: Int): CurrentWindow =
      if (time < endTime)
        copy(window = window.update(time, measurement), previousResult = None)
      else {
        val nextEndTime = endTime + windowSize
        CurrentWindow(
          window = Window(time, measurement),
          endTime = if (nextEndTime < time) time + windowSize else nextEndTime,
          previousResult = if (window.isValid) Some(window.toFormattedString) else None
        )
      }

    def update(
      timeString: String,
      measurementString: String,
      windowSize: Int = WindowSize
    ): CurrentWindow = {
      val result = for {
        time <- Try {timeString.toLong}
        measurement <- Try {measurementString.toDouble}
      } yield update(time, measurement, windowSize)

      result.recover { case e => copy(previousResult = None) }.get
    }
  }

  def processFile(fileName: String): Unit = {
    val readFile = Try {Source.fromFile(fileName)} map { file: BufferedSource =>
      iterate(file.getLines())
      file.close()
    }

    readFile.recover { case e: Throwable => println(e) }
  }

  def iterate(iterator: Iterator[String], windowSize: Int = WindowSize): CurrentWindow = {
    var state = CurrentWindow()

    println("T          V       N RS      MinV    MaxV")
    1 to 50 foreach { _ => print("-") }
    println()
    for (line <- iterator) line match {
      case Pattern(time, measurement) =>
        state = state.update(time, measurement, windowSize)
        state.previousResult.foreach { println(_) }
      case _ =>
    }
    if (state.window.isValid) println(state.window.toFormattedString)

    state
  }

}
