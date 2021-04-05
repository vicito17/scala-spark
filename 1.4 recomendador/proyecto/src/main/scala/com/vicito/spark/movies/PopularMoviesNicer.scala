package com.vicito.spark.movies

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.io.Source
import java.nio.charset.CodingErrorAction
import scala.io.Codec

object PopularMoviesNicer {
  
  def loadMovieNames() : Map[Int, String] = {
    
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    // Create a Map of Ints to Strings, and populate it from u.item.
    var movieNames:Map[Int, String] = Map()
    
     val lines = Source.fromFile("data/u.item").getLines()
     for (line <- lines) {
       var fields = line.split('|')
       if (fields.length > 1) {
        movieNames += (fields(0).toInt -> fields(1))
       }
     }
    
     return movieNames
  }
 
  def main(args: Array[String]) {
   
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val sc = new SparkContext("local[*]", "PopularMoviesNicer")
    
    var nameDict = sc.broadcast(loadMovieNames)
    
    val lines = sc.textFile("data/u.data")
    
    val movies = lines.map(x => (x.split("\t")(1).toInt, 1))
    
    val movieCounts = movies.reduceByKey( (x, y) => x + y )
    
    val flipped = movieCounts.map( x => (x._2, x._1) )
    
    val sortedMovies = flipped.sortByKey()
    
    val sortedMoviesWithNames = sortedMovies.map( x  => (nameDict.value(x._2), x._1) )
    
    val results = sortedMoviesWithNames.collect()
    
    results.foreach(println)
  }
  
}

