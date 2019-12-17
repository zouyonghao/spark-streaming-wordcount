# 大数据系统基础

## 实验四： Spark Streaming

### 邹永浩 2019211168

#### 任务1 Spark Streaming 词频统计

使用的代码如下：
```scala
import org.apache.spark.streaming._
val ssc = new StreamingContext(sc, Seconds(5))
val lines = ssc.socketTextStream("thumm01", 54321)
val result = lines.flatMap(_.split(" ")).map(w => (w, 1)).reduceByKey(_ + _)
result.print()
ssc.start()
```

分别启动 `nc` 和 `spark-shell`

![](1.png)

此时统计效果为统计5秒内的词频

![](2.png)

#### 任务2 累加词频统计

若要支持累加词频统计，又很多方法，最简单的为使用 `CollectionAccumulator`



#### 任务3 使用 Spark 计算均值与方差