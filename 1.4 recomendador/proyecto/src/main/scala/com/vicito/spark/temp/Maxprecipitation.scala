package com.vicito.spark.temp

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

import scala.math.max

/** Find the maximum temperature by weather station for a year */
object Maxprecipitation {

  def parseLine(line:String)= {
    val fields = line.split(",")
    val stationID = fields(0)
    val entryType = fields(2)
    val temperature = fields(3).toFloat * 0.1f * (9.0f / 5.0f) + 32.0f
    (stationID, entryType, temperature)
  }
    /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "Maxprecipitation")

    val lines = sc.textFile("data/1800.csv")
    val parsedLines = lines.map(parseLine)
    val maxprec = parsedLines.filter(x => x._2 == "PRCP")
    val stationPrec = maxprec.map(x => (x._1, x._3.toFloat))
    val maxPrecByStation = stationPrec.reduceByKey( (x,y) => max(x,y))
    val results = maxPrecByStation.collect()

    for (result <- results.sorted) {
       val station = result._1
       val prec = result._2
       val formattedTemp = f"$prec%.2f "
       println(s"$station max prec: $formattedTemp")
    }

  }
}
