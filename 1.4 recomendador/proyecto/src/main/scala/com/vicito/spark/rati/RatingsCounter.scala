package com.vicito.spark.rati

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
object RatingsCounter {
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "RatingsCounter")

    val lines = sc.textFile("data/u.data")
    val ratings = lines.map(x => x.toString().split("\t")(2))
    val results = ratings.countByValue()
    val sortedResults = results.toSeq.sortBy(_._1)

    sortedResults.foreach(println)
  }
}
