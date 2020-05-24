package com.spark.yi.wc
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkWordCount {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "E:\\repository\\hadoop-2.6.0-cdh5.14.0")
    val sc: SparkContext = {
      val conf: SparkConf = new SparkConf()
        .setMaster("local[*]")
        .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
      SparkContext.getOrCreate(conf)
    }

    val inputRDD: RDD[String] = sc.textFile("E:\\study\\anphy\\yi-bdfs\\spark-eco\\spark-base\\src\\main\\resources\\1.txt", minPartitions = 2)

    val wordcountRDD: RDD[(String, Int)] = inputRDD
      .filter(line => null != line.length && line.trim.length > 0)
      .flatMap(line => line.trim.split("\\s+"))
      .map(word => (word, 1))
      .reduceByKey((a, b) => a + b)

    wordcountRDD
      .coalesce(1)
      .saveAsTextFile("E:\\study\\anphy\\yi-bdfs\\spark-eco\\spark-base\\src\\main\\resources\\out\\" + System.currentTimeMillis)
    Thread.sleep(100000)
    if (!sc.isStopped) sc.stop()
  }
}