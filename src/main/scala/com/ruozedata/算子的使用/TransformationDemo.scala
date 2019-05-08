package com.ruozedata.算子的使用

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object TransformationDemo extends App {

  val sparkConf = new SparkConf().setMaster("local").setAppName("TransformationDemo")
  val sc = new SparkContext(sparkConf)


  /** map */
  def map = {
    val list = List("小黄", "小白", "小蓝")
    val listRDD = sc.parallelize(list)
    listRDD.map(("hello", _)).foreach(println)
  }

  //  map
  //  (hello,小黄)
  //  (hello,小白)
  //  (hello,小蓝)


  /** flatMap */
  def flatMap = {
    val list = List("小黄", "小白", "小蓝")
    val listRDD = sc.parallelize(list)
    listRDD.flatMap(_.split(",")).map(("hello", _)).foreach(println)
  }

  //  flatMap
  //  (hello,小黄)
  //  (hello,小白)
  //  (hello,小蓝)


  /**
    * mapPartitions
    *
    * map:
    * 一条数据一条数据的处理（数据库、文件系统等）
    * mapPartitions：
    * 一次获取的是一个分区的数据(hdfs)
    * 正常情况下，mapPartitions是一个高性能的算子
    * 因为每次处理的是一个分区的数据，减少了去获取数据的次数。
    *
    * 但是，如果我们的分区如果设置得不合理，有可能导致每个分区里面的数据量过大。
    */
  def mapPartitions = {
    val list = List(1, 2, 3, 4, 5, 6)
    val listRDD = sc.parallelize(list)

    listRDD.mapPartitions(iterator => {
      val newList: ListBuffer[String] = ListBuffer()
      while (iterator.hasNext) {
        newList.append(s"hello ${iterator.next()}")
      }
      newList.toIterator
    }).foreach(println(_))
  }

  //  mapPartitions
  //  hello 1
  //  hello 2
  //  hello 3
  //  hello 4
  //  hello 5
  //  hello 6


  /**
    * mapPartitionsWithIndex
    * 每次获取和处理的就是一个分区的数据，并且可获知处理的分区的分区号
    *
    */
  def mapPartitionsWithIndex = {
    val list = List(1, 2, 3, 4, 5, 6, 7, 8)
    val listRDD = sc.parallelize(list)

    listRDD.mapPartitionsWithIndex((index, iterator) => {
      val listBuffer: ListBuffer[String] = new ListBuffer()
      while (iterator.hasNext) {
        //        listBuffer.append(s"${index}\t_${iterator.next()}")
        listBuffer.append(f"${index}\t_${iterator.next().toFloat}%3.2f")
        //        listBuffer.append(raw"${index}\t${iterator.next()}")
      }
      listBuffer.iterator
    }, true)
      .foreach(println(_))
  }

  mapPartitionsWithIndex

  //  ------------------------------------------
  //  scala中插值器  s  f  raw ，它们都定义在 StringContext.scala 中
  //  s: 解析\t \n等特殊符号
  //  f: 格式化输出 （默认格式是 s）
  //  raw: 不解析\t \n等特殊符号
  //  ${}内可写任意字符串

  //   s        f                  raw
  //   0	_1    0	_1.00       0\t1
  //   0	_2    0	_2.00       0\t2
  //   0	_3    0	_3.00       0\t3
  //   0	_4    0	_4.00       0\t4
  //   0	_5    0	_5.00       0\t5
  //   0	_6    0	_6.00       0\t6
  //   0	_7    0	_7.00       0\t7
  //   0	_8    0	_8.00       0\t8
  //  ------------------------------------------

  /**
    * reduce其实就是将RDD中的所有元素进行合并，当运行call方法时，会传入两个参数，
    * 在call方法中将两个参数合并后返回，而这个返回值会和一个新的RDD中的元素再次传入call方法中，
    * 继续合并，直到合并到只剩下一个元素时。
    */
  def reduce = {
    val list = List(1, 2, 3, 4, 5, 6)
    val listRDD = sc.parallelize(list)
    println(listRDD.reduce(_ + _))
  }

  //  reduce
  //  21


  /**
    * reduceByKey仅将RDD中所有key-value键值对 中 key值相同的value进行合并
    */
  def reduceByKey = {
    val list = List(("小白", 1), ("小灰", 2), ("小灰", 3), ("小白", 4), ("小白", 5))
    val listRDD = sc.parallelize(list)
    listRDD.reduceByKey(_ + _).foreach(tuple => println(s"name: ${tuple._1} -> ${tuple._2}"))
  }

  //  reduceByKey
  //  name: 小白 -> 10
  //  name: 小灰 -> 5


  /** 累加 */
  def union = {
    val list1 = List(1, 2, 3, 4)
    val list2 = List(3, 4, 5, 6)
    val rdd1 = sc.parallelize(list1)
    val rdd2 = sc.parallelize(list2)

    rdd1.union(rdd2).foreach(x => print(s"$x ")) //.foreach(println)
  }

  //  union()
  //  name: 小白 -> 10
  //  name: 小灰 -> 5


  /**
    * groupByKey
    * union只是将两个RDD简单的累加在一起，而join则不一样，join类似与hadoop中的combin操作，只是少了排序这一段
    * groupByKey，因为join可以理解为union与groupByKey的结合：
    * groupByKey是将RDD中的元素进行分组，组名是call方法中的返回值
    *
    * 即groupByKey是将PairRDD中拥有相同key值得元素归为一组。
    */
  def groupByKey = {
    val list = List(("Spark", "sparkSQL"), ("Hadoop", "YARN"), ("Spark", "sparkStreming"), ("Hadoop", "HDFS"))
    val listRDD = sc.parallelize(list)
    val groupByKeyRDD = listRDD.groupByKey()
    groupByKeyRDD.foreach(t => {
      val name = t._1
      val iterator = t._2.iterator
      var include = ""
      while (iterator.hasNext) include = include + iterator.next() + " "
      println(s"name: $name include: $include")
    })
  }

  //  groupByKey
  //  name: Spark include: sparkSQL sparkStreming
  //  name: Hadoop include: YARN HDFS


  /** 连接 */
  def join = {
    val list1 = List((1, "小白"), (2, "小灰"), (3, "小蓝"))
    val list2 = List((1, 99), (2, 98), (3, 97))
    val list1RDD = sc.parallelize(list1)
    val list2RDD = sc.parallelize(list2)

    val joinRDD = list2RDD.join(list1RDD)
    joinRDD.foreach(t => println(s"学号: ${t._1} 姓名: ${t._2._1} 成绩: ${t._2._2}"))
  }

  //  join()
  //  list1RDD.join(list2RDD)       list2RDD.join(list1RDD)
  //  学号: 1 姓名: 小白 成绩: 99      学号: 1 姓名: 99 成绩: 小白
  //  学号: 3 姓名: 小蓝 成绩: 97      学号: 3 姓名: 97 成绩: 小蓝
  //  学号: 2 姓名: 小灰 成绩: 98      学号: 2 姓名: 98 成绩: 小灰


  /** 抽样 */
  def sample = {
    val list = 1.to(100)
    val listRDD = sc.parallelize(list)
    listRDD.sample(false, 0.1, 0).foreach(x => print(s"$x ")) //.foreach(println)
  }

  //  sample()
  //  10 39 41 53 54 58 60 80 89 98


  /** 笛卡尔积 */
  def cartesian = {
    val list1 = List("A", "B")
    val list2 = List(1, 2, 3)
    val list1RDD = sc.parallelize(list1)
    val list2RDD = sc.parallelize(list2)

    list2RDD.cartesian(list1RDD).foreach(x => println(s"${x._1} -> ${x._2}"))
  }

  //  cartesian
  //  list1RDD.cartesian(list2RDD)      list2RDD.cartesian(list1RDD)
  //  A -> 1                            1 -> A
  //  A -> 2                            1 -> B
  //  A -> 3                            2 -> A
  //  B -> 1                            2 -> B
  //  B -> 2                            3 -> A
  //  B -> 3                            3 -> B


  /** 过滤 */
  def filter = {
    val list = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    val listRDD = sc.parallelize(list)
    listRDD.filter(_ % 2 == 0).foreach(x => print(s"$x "))
  }

  //  filter
  //  2 4 6 8 10


  /** 去重 */
  def distinct = {
    val list = List(1, 1, 2, 2, 3, 4, 5)
    sc.parallelize(list).distinct().foreach(x => print(s"$x "))
  }

  //  distinct
  //  4 1 3 5 2


  /** 交叉点 */
  def intersect = {
    val list1 = List(1, 2, 3, 4)
    val list2 = List(3, 4, 5, 6)
    val list1RDD = sc.parallelize(list1)
    val list2RDD = sc.parallelize(list2)
    list1RDD.intersection(list2RDD).foreach(x => print(s"$x "))
  }

  //  intersect
  //  list1RDD.intersection(list2RDD)   list2RDD.intersection(list1RDD)
  //  4 3                                4 3


  /**
    * 分区数由  多 -> 少
    *
    * 默认分区数由parallelize中的第二个参数 numSlices控制
    */
  def coalesce = {
    val list = List(1, 2, 3, 4, 5, 6, 7, 8, 9)
    sc.parallelize(list, 5).coalesce(5).foreach(x => print(s"$x "))
  }

  //  coalesce
  //  ------------------------------------------------------------------------------------
  //  numSlicens = 3
  //  coalesce(1)             coalesce(2)           coalesce(3) coalesce(4)....
  //  1 2 3 4 5 6 7 8 9       1 2 3                 1 2 3
  //                          4 5 6 7 8 9           4 5 6
  //                                                7 8 9

  //  numSliens = 5
  //  coalesce(4)          coalesce(5) coalesce(6)...   coalesce(1) coalesce(2) coalesce(3)
  //  1                      1
  //  2 3                    2 3                               1、2、3的结果同上
  //  4 5                    4 5
  //  6 7 8 9                6 7
  //                         8 9
  //  ------------------------------------------------------------------------------------


  /**
    * 进行重分区
    * 解决问题   本区数少  ->  增加分区数
    *
    * 可以绝对控制parallelize中的numSlices数量
    */
  def replication = {
    val list = List(1, 2, 3, 4)
    sc.parallelize(list, 1).repartition(3).foreach(x => print(s"$x "))
  }

  //  replication
  //  ---------------------------------------
  //  numSlicens = 1
  //  repartition(1)        repartition(3)
  //  1 3                   3
  //  2 4                   1 4
  //                        2
  //  ---------------------------------------


  /**
    * repartitionAndSortWithinPartitions
    * 此函数是repartition函数的变种，与repartition函数不通的是，
    * 此函数在给定的partitioner内部进行排序，性能比repartition要高
    *
    * 分区数量由 HashPartitioner 来控制
    */
  def repartitionAndSortWithinPartitions = {
    val list = List(1, 4, 55, 66, 33, 48, 23)
    val listRDD = sc.parallelize(list, 1)
    listRDD.map(x => (x, x))
      .repartitionAndSortWithinPartitions(new HashPartitioner(4))
      .mapPartitionsWithIndex((index, iterator) => {
        val listBuffer: ListBuffer[String] = new ListBuffer
        while (iterator.hasNext) {
          //          listBuffer.append(s"$index${iterator.next()}")
          listBuffer.append(index + "" + iterator.next())
        }
        listBuffer.iterator
      }, false)
      .foreach(x => print(s"$x "))
  }

  //  repartitionAndSortWithinPartitions
  //  ------------------------------------------------------------
  //  numSlices = 1
  //  .repartitionAndSortWithinPartitions(new HashPartitioner(2))
  //  0(4,4) 0(48,48) 0(66,66)
  //  1(1,1) 1(23,23) 1(33,33) 1(55,55)

  //  .repartitionAndSortWithinPartitions(new HashPartitioner(3))
  //  0(33,33) 0(48,48) 0(66,66)
  //  1(1,1) 1(4,4) 1(55,55)
  //  2(23,23)

  //  .repartitionAndSortWithinPartitions(new HashPartitioner(4))
  //  0(4,4) 0(48,48)
  //  1(1,1) 1(33,33)
  //  2(66,66)
  //  3(23,23) 3(55,55)
  //  ------------------------------------------------------------


  /**
    * 对两个RDD中的KV元素，每个RDD中相同key中的元素分别聚合成一个集合。
    * 与reduceByKey不同的是针对两个RDD中相同的key的元素进行合并。
    */
  def cogroup = {
    val list1 = List((1, "www"), (2, "bbs"))
    val list2 = List((1, "spark"), (2, "spark"), (3, "fink"))
    val list3 = List((1, "org"), (2, "org"), (3, "com"))

    val list1RDD = sc.parallelize(list1)
    val list2RDD = sc.parallelize(list2)
    val list3RDD = sc.parallelize(list3)

    list1RDD.cogroup(list2RDD, list3RDD)
      .foreach(tuple => println(s"${tuple._1} ${tuple._2._1} ${tuple._2._2} ${tuple._2._3}"))
  }

  //  cogroup
  //  ------------------------------------------------------------
  //  list1RDD.cogroup(list2RDD,list3RDD)
  //  1 CompactBuffer(www) CompactBuffer(spark) CompactBuffer(org)
  //  3 CompactBuffer() CompactBuffer(fink) CompactBuffer(com)
  //  2 CompactBuffer(bbs) CompactBuffer(spark) CompactBuffer(org)
  //  ------------------------------------------------------------


  /**
    * sortByKey函数作用与Key-Value形式的RDD，并对Key进行排序
    * 默认为true，升序;   flase，降序
    *
    */
  def sortByKey = {
    val list = List((99, "小白"), (44, "小紫"), (55, "小黑"), (22, "小蓝"))
    sc.parallelize(list).sortByKey(false)
      .foreach(tuple => println(s"${tuple._1} -> ${tuple._2}"))
  }

  //  sortByKey
  //  ------------------------------------------------------------
  //  sortByKey(false)降序        sortByKey() 默认为true，升序
  //  99 -> 小白                  22 -> 小蓝
  //  55 -> 小黑                  44 -> 小紫
  //  44 -> 小紫                  55 -> 小黑
  //  22 -> 小蓝                  99 -> 小白
  //  ------------------------------------------------------------


  /**
    * aggregateByKey
    * aggregateByKey函数对PairRDD中相同Key的值进行聚合操作，在聚合过程汇总同样使用了一个中立的初始值。
    * 和aggregateByKey函数类似，aggregateByKey返回值的类型不需要和RDD中value的类型一致。
    * 因为aggregateByKey是对相同Key中的值进行聚合操作，所以aggregateByKey函数最终返回的类型还是Pair RDD,
    * 对应的结果是Key和聚合好的值；而aggregate函数直接是返回非RDD的结果，这点需要注意。
    * 在实现过程中，定义了三个aggregateByKey函数原型，但最终调用的aggregateByKey函数都一致。
    *
    * 可设置中立值
    */
  def aggregateByKey = {
    val list = List("hello,world", "hello,spark")
    sc.parallelize(list).flatMap(_.split(",")).map((_, 1)).aggregateByKey(0)(_ + _, _ + _).foreach(tuple => println(s"${tuple._1} -> ${tuple._2}"))
  }

  //  aggregateByKey
  //  aggregateByKey(0)          aggregateByKey(10)
  //  spark -> 1                  spark -> 11
  //  hello -> 2                  hello -> 12
  //  world -> 1                  world -> 11


  //todo..


  sc.stop()
}
