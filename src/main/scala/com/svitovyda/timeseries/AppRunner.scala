package com.svitovyda.timeseries

object AppRunner {
  def main(args: Array[String]): Unit = {
    println(args.length, args.mkString)
    if(args.nonEmpty) TimeSeries.processFile(args.head)
  }
}
