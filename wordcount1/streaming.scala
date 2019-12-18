import org.apache.spark.streaming._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.util._

import org.apache.log4j.{Level, Logger}

object SimpleApp {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf()
      .setAppName("wordcount1")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(5))
    val lines = ssc.socketTextStream("thumm01", 54321)
    val wordAccumulator = new CollectionAccumulator[String]()
    ssc.sparkContext.register(wordAccumulator, "words")
    // val wordAccumulator = ssc.sparkContext.collectionAccumulator[String]("words")
    // ssc.checkpoint(".")
    lines.foreachRDD { line =>
    // rdd.foreach { line =>
    // lines
      println(line)
      line.flatMap(l => l.split(" "))
      .foreach(i => {
        wordAccumulator.add(i)
        println(wordAccumulator.value)
        Logger
          .getLogger(this.getClass())
          .error(i + " " + wordAccumulator.value.toArray.filter(_ == i).size)
        // (i, wordAccumulator.value.toArray.filter(_ == i).size)
      })
      // this works, accumulator should be used in driver side!
      println(wordAccumulator.value.toArray.groupBy(w => w).map(w => (w._1, w._2.size)))
      // .reduceByKey(_ + _)

    }
    // result.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
