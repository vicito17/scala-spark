package com.vicito.spark.friends

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object FriendsByAge {

  def parseLine(line: String) = {
      val fields = line.split(",")
      val age = fields(2).toInt
      val numFriends = fields(3).toInt
      (age, numFriends)
  }

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "FriendsByAge")

    val path="/home/vicentecipre/Escritorio/cursos/scala-spark/1.4 recomendador/SparkScala/fakefriends.csv"
    val lines = sc.textFile(path)

    val rdd = lines.map(parseLine)

    val totalsByAge = rdd.mapValues(x => (x, 1)).reduceByKey( (x,y) => (x._1 + y._1, x._2 + y._2))

      val averagesByAge = totalsByAge.mapValues(x => x._1 / x._2)

    val results = averagesByAge.collect()

    results.sorted.foreach(println)
  }

}
