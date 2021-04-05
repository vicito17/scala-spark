package com.vicito.spark.movies

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._
import scala.io.Source
import java.nio.charset.CodingErrorAction
import scala.io.Codec
import org.apache.spark.sql.functions._

object PopularMoviesDataSets {
  
  def loadMovieNames() : Map[Int, String] = {
    
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    var movieNames:Map[Int, String] = Map()
    
     val lines = Source.fromFile("../ml-100k/u.item").getLines()
     for (line <- lines) {
       var fields = line.split('|')
       if (fields.length > 1) {
        movieNames += (fields(0).toInt -> fields(1))
       }
     }
    
     return movieNames
  }
 
  final case class Movie(movieID: Int)
  
  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val spark = SparkSession
      .builder
      .appName("PopularMovies")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .getOrCreate()
    
    val lines = spark.sparkContext.textFile("data/u.data")
                     .map(x => Movie(x.split("\t")(1).toInt))
    
    import spark.implicits._
    val moviesDS = lines.toDS()
    
    val topMovieIDs = moviesDS.groupBy("movieID")
      .count()
      .orderBy(desc("count"))
      .cache()

    
//    topMovieIDs.show()
    
    val top10 = topMovieIDs.take(10)

    val names = loadMovieNames()
    
    println
    for (result <- top10) {
      println (names(result(0).asInstanceOf[Int]) + ": " + result(1))
    }

    // Stop the session
    spark.stop()
  }
  
}

