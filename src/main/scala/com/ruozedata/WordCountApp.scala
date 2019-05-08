package com.ruozedata

import org.apache.spark.{SparkConf, SparkContext}

object WordCountApp extends App {

  val conf = new SparkConf().setAppName("WordCountApp").setMaster("local[2]")
  val sc = new SparkContext(conf)

  //输入（用args()传入参数）
 // val dataFile = sc.textFile(args(0))
  val dataFile = sc.textFile("hdfs://192.168.153.130:8020/wc_input/wc.txt")

  //业务逻辑
  val outputFile = dataFile.flatMap(_.split(",")).map((_,1)).reduceByKey(_+_)//.collect().foreach(println)


  //输出文件
  outputFile.saveAsTextFile(args(1))

  //关闭流（输入流）
  sc.stop()
}
