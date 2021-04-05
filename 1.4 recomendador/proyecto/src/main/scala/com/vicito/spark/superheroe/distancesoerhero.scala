package com.vicito.spark.superheroe

import com.vicito.spark.superheroe.DegreesOfSeparation.{createStartingRdd, hitCounter}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator

import scala.collection.mutable.ArrayBuffer

object distancesoerhero {

  // The characters we want to find the separation between.
  val startCharacterID = 5306 //SpiderMan


  /** Converts a line of raw input into a BFSNode */
  def convertToBFS(line: String): (Int,Array[Int]) = {

    // Split up the line into fields
    val fields = line.split("\\s+")

    // Extract this hero ID from the first field
    val heroID = fields(0).toInt

    // Extract subsequent hero ID's into the connections array
    var connections: ArrayBuffer[Int] = ArrayBuffer()
    for ( connection <- 1 to (fields.length - 1)) {
      connections += fields(connection).toInt
    }

    return (heroID, connections.toArray)
  }


  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local[*]", "distancehero")
    var arr_heroe=Array(startCharacterID)

    // Our accumulator, used to signal when we find the target
    // character in our BFS traversal.

    val inputFile = sc.textFile("data/Marvel-graph.txt")
    val iterationRdd =  inputFile.map(convertToBFS)
    val dist1= iterationRdd.filter(x=> x._2.contains(startCharacterID))
    val pj_dist1=dist1.map(x=>x._1)
    //print(pj_dist1.count())
   // rdd_heroe=rdd_heroe.union(pj_dist1)
   // val friends_dist1 =dist1.flatMap(x=>x._2)
   // val dist2= friends_dist1.distinct().filter(x=> !rdd_heroe.con)

  }
}
