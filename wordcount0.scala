import org.apache.spark.streaming._
val ssc = new StreamingContext(sc, Seconds(5))
val lines = ssc.socketTextStream("thumm01", 54321)
val result = lines.flatMap(_.split(" ")).map(w => (w, 1)).reduceByKey(_ + _)
result.print()
ssc.start()