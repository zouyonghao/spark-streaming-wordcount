import org.apache.spark.streaming._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object SimpleApp {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf()
      .setAppName("wordcount1")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(5))
    val lines = ssc.socketTextStream("thumm01", 54321)

    val wordAccumulator = ssc.sparkContext.collectionAccumulator[String]("")

    val result = lines.flatMap(_.split(" ")).map(i => {
        wordAccumulator.add(i)
        println(wordAccumulator.value)
        (i, wordAccumulator.value.toArray.filter(_ == i).size)
    }
        ).reduceByKey(_+_)
    result.print()
    ssc.start()
    ssc.awaitTermination()
  }
}