package com.vicito.spark.friends

import org.apache.log4j._
import org.apache.spark._

/** Compute the average number of friends by age in a social network. */
object FriendsByname {

  /** A function that splits a line of input into (age, numFriends) tuples. */


  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "FriendsByAge")

    // Load each line of the source data into an RDD
    val path="SparkScala/fakefriends.csv"
    val lines = sc.textFile(path)

    // Use our parseLines function to convert to (age, numFriends) tuples
    val rdd = lines.map(line=> line.split(","))

    val tuple=rdd.map(x=> (x(1),1))
    val frec= tuple.reduceByKey(_+_)
    val sol = frec.sortBy(_._2, ascending = false )

    sol.foreach(x=> println(x))
  }

}
