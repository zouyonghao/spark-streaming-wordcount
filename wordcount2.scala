import scala.collection.mutable.HashMap
import org.apache.spark.AccumulableParam

class HashMapParam extends AccumulableParam[HashMap[String, Int], (String, Int)] {

  def addAccumulator(map: HashMap[String, Int], i: (String, Int)): HashMap[String, Int] = {
    map.update(i._1, map.getOrElse(i._1, 0) + 1)
    map
  }
  def addInPlace(map1: HashMap[String, Int], map2: HashMap[String, Int]): HashMap[String, Int] = {
    map2.foreach(i => addAccumulator(map1, i))
    map1
  }
  def zero(initialValue: HashMap[String, Int]): HashMap[String, Int] = {
    new HashMap[String, Int]()
  }
}

import org.apache.spark.streaming._
val ssc = new StreamingContext(sc, Seconds(5))
val lines = ssc.socketTextStream("thumm01", 54321)
val result = lines.flatMap(_.split(" ")).map(w => (w, 1)).reduceByKey(_ + _)
result.print()
ssc.start()