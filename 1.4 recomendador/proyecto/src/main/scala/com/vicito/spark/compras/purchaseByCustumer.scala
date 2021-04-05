package com.vicito.spark.compras

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object purchaseByCustumer {
  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local", "WordCountBetterSorted")

    val lines = sc.textFile("data/customer-orders.csv")
    // Cuanto ha gastado cada uno
    val palabras = lines.map(x => x.split(","))

    val tupleg= palabras.map(x=> (x(0).toInt,x(2).toFloat))
    val suma= tupleg.reduceByKey((x,y)=> x+y)
    val orden = suma.map( x => (x._2, x._1) ).sortByKey()
    val sol =orden.map( x => (x._2, x._1) )
    sol.foreach(println)

  }
}
