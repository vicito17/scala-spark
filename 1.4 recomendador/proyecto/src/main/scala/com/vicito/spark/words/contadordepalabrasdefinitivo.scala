package com.vicito.spark.words

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, rdd}
import org.apache.spark.rdd.RDD

object contadordepalabrasdefinitivo {

  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using the local machine
    val sc = new SparkContext("local", "WordCountBetterSorted")

    // Load each line of my book into an RDD
    val lines = sc.textFile("data/book.txt")
    val stopwords = sc.textFile("data/stopwords.txt")
    val todas=stopwords.reduce((x,y)=> x+" "+ y)

    // Split using a regular expression that extracts words
    val palabras = lines.flatMap(x => x.split("\\W+"))

    val palabraslimpias=palabras.filter(x=> !todas.contains(x))

    // Normalize everything to lowercase
    val minus = palabraslimpias.map(x => x.toLowerCase())

    // Count of the occurrences of each word
    val suma = minus.map(x => (x, 1)).reduceByKey( (x,y) => x + y )

    // Flip (word, count) tuples to (count, word) and then sort by key (the counts)
    val wordCountsSorted = suma.map( x => (x._2, x._1) ).sortByKey()

    // Print the results, flipping the (count, word) results to word: count as we go.
    for (result <- wordCountsSorted) {
      val count = result._1
      val word = result._2
      println(s"$word: $count")
    }


  }
}
