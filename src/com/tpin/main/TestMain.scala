package com.tpin.main

import java.text.SimpleDateFormat
import java.util.Date

import com.tpin.entity.{Pattern, VertexAttr, EdgeAttr}
import com.tpin.utils._
import org.apache.spark.graphx.Graph
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by david on 6/30/16.
 */
object TestMain {
  def printGraph[VD,ED]( graph:Graph[VD,ED])={
    graph.vertices.foreach { println }
    graph.edges.foreach { case edge => println(edge) }
  }

  def main(args: Array[String]) {
        val path=Seq(1,2,3,4,5,6,7,8,9)
         path.sliding(2).map(seq=>(seq(0),seq(1))).foreach(println)
        val path2=Seq(1)
        path.tail.foreach(println)
        path2.sliding(2).foreach(println)

    //    annotation objectFile david:IL测设模块
    //        val graph = getFromProgram(sc)
//    val sparkConf = new SparkConf().setAppName("find_IL")
//    val sc = new SparkContext(sparkConf)
//    val graph = InputOutputTools.getFromCsv(sc)
//    val AfterAddedIL = AddNewAttr.Add_Loose_IL(sc, graph)
//
//    val Pattern4=PatternMatching.Match_Pattern_4(sc,AfterAddedIL)
//    Pattern4.foreach(println)
//    InputOutputTools.saveAsCsvFile(AfterAddedIL)
//    printGraph(AfterAddedIL)

//    val sparkConf = new SparkConf().setAppName("find_IL")
//    val sc = new SparkContext(sparkConf)
//    val hiveContext = new HiveContext(sc)
//    val format = new SimpleDateFormat("hh:mm:ss.SSS")
//    // 从HDFS获取TPIN
//    val tpin1 = InputOutputTools.getFromHDFS_Object(sc).persist()
//    println("\rread from object file after construct:  \n"+tpin1.vertices.count)
//    println(tpin1.edges.count)
//    // 构建带社团编号的TPINo
//    //  hiveContext.sql("SELECT * FROM tpin.tpin_edge_wwd").rdd.take(10)
//    // 构建关联交易模式（单向环和双向环）
//    val pattern1 = PatternMatching.Match_Pattern_1(sc,tpin1).persist()
//
//    val add_strict_IL= AddNewAttr.Add_Strict_IL(sc,tpin1).persist()
//    val pattern2 = PatternMatching.Match_Pattern_2(sc,add_strict_IL).persist()
//    val pattern3 = PatternMatching.Match_Pattern_3(sc,add_strict_IL).persist()
//
//    val add_loose_IL= AddNewAttr.Add_Loose_IL(sc,tpin1).persist()
//    val pattern4 = PatternMatching.Match_Pattern_4(sc,add_loose_IL).persist()
//    val patterns=pattern1.union(pattern2).union(pattern3).union(pattern4)

//    val sparkConf = new SparkConf().setAppName("find_IL")
//    val sc = new SparkContext(sparkConf)
//    val hiveContext = new HiveContext(sc)
//    val tpin1 = InputOutputTools.getFromHDFS_Object(sc).persist()
//    val paths = PatternMatching.constructPaths_GT0_2(sc, tpin1).persist()
//    println("法定代表人关系边以及权值大于20%的投资关系边构成的\n" +
//      "前件路径个数为："+paths.count())
//    println("长度为1的有："+paths.filter(_.size==1).count())
//    println("长度为2的有："+paths.filter(_.size==2).count())
//    println("长度为3的有："+paths.filter(_.size==3).count())
//    println("长度为3以上的有："+paths.filter(_.size>=4).count())

//    val sparkConf = new SparkConf().setAppName("find_IL")
//        val sc = new SparkContext(sparkConf)
//        val hiveContext = new HiveContext(sc)
//    val pattern1=Pattern(1,Seq((1L,2L)),Seq(9,1))
//    val pattern2=Pattern(1,Seq((1L,2L)),Seq(9,1))
//    val pattern3=Pattern(1,Seq((1L,2L)),Seq())
//    val patterns=sc.parallelize(Seq(pattern1,pattern2,pattern3))
//    InputOutputTools.checkDirExist(sc,"/tpin/wwd/month1/patterns3")
//     patterns.saveAsObjectFile("/tpin/wwd/month1/patterns3")
//    println("finish!")

  }

}
