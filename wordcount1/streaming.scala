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
    val shutdownAccumulator = new LongAccumulator()
    ssc.sparkContext.register(wordAccumulator, "words")
    ssc.sparkContext.register(shutdownAccumulator, "shutdownAccumulator")

    // val wordAccumulator = ssc.sparkContext.collectionAccumulator[String]("words")
    // ssc.checkpoint(".")

    lines.foreachRDD { line =>
      // rdd.foreach { line =>
      // lines
      println(line)
      line
        .flatMap(l => l.split(" "))
        .foreach(i => {
          wordAccumulator.add(i)
          // for exit
          if (i == ":exit") {
            shutdownAccumulator.add(1)
          }
        })
      // this works, accumulator should be used in driver side!
      println(
        wordAccumulator.value.toArray
          .groupBy(w => w)
          .map(w => (w._1, w._2.size))
      )
      if (shutdownAccumulator.value > 0) {
        ssc.stop()
        println("Application stopped!")
      }
      // .reduceByKey(_ + _)

    }
    // result.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
