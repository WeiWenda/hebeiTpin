package com.tpin.main

import java.util.Arrays


import com.tpin.entity.{EdgeAttr, VertexType, VertexAttr}
import org.apache.spark.graphx.{VertexId, Graph, Edge}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * Created by 蔚文达 on 2016/12/7.
 */
object Credit{

  def GetFromCsv(sc:SparkContext):Graph[Double,Double] = {
    val edgesTxt = sc.textFile("res/InputCsv/edges1.csv")
    val vertexTxt = sc.textFile("res/InputCsv/vertices1.csv")
    val vertices = vertexTxt.filter(!_.startsWith("id")).map(_.split(",")).map {
      case v =>
        (v(0).toLong, v(1).toDouble)
    }
    val edges = edgesTxt.filter(!_.startsWith("source")).map(_.split(",")).map {
      case e =>
        Edge(e(0).toLong, e(1).toLong, e(2).toDouble)
    }
    Graph(vertices, edges)
  }

  def Decay(x:Int):Double={
    return x.toDouble
  }


  def ShortestPath(sc:SparkContext, graph: Graph[Double, Double])= {
    val threshold = 10
    //annotation of david:修正信用评分
    //annotation of david:pathLength是当前最长路径的指示
    var pathLength = 1
    val maxPathLength:Int = 2
    //annotation of david:点上每个元组(id,vd)表示一个点id，对当前点的影响vd,当前乘了几项
    var subgraph = graph.mapVertices((id, vd) => Seq((id,vd,1)))

    while (pathLength < maxPathLength) {

      val pathLengthBC =sc.broadcast(pathLength)

      val pervertices = subgraph
        .aggregateMessages[Seq[(Long, Double, Int)]](edge => {
        edge.sendToDst(
          //annotation of david：只传播上一个长度的，此处不再考虑环的问题
          edge.srcAttr.filter(_._3 == pathLengthBC.value).map { case (vid, weight, length) => (vid, weight * edge.attr, length + 1) }
        )
      }, _ ++ _)
        .map { case (dstid, srcList) =>
        var resultMap = mutable.HashMap[Long, Double]()
        srcList.filter(e=>e._2*Decay(e._3)>threshold).foreach { case (srcid, weight, length) =>
          //annotation of david:衰减小于阈值时不再传播
          if (!resultMap.contains(srcid))
            resultMap.put(srcid,weight)
          else if (resultMap(srcid) < weight)
            resultMap.update(srcid,weight)
        }
        (dstid, resultMap.toSeq.map(e => (e._1, e._2, pathLengthBC.value)))
      }


      //annotation of david:不妨直接限制每个点的记录数
      subgraph = subgraph.outerJoinVertices(pervertices){ (vid, old, opt) =>
        var resultMap = mutable.HashMap[Long, (Double,Int)]()
        (old ++ opt.getOrElse(Seq[(Long, Double, Int)]()))
          .foreach { case (srcid, weight, length) =>
          if (!resultMap.contains(srcid))
            resultMap.put(srcid,(weight,length))
          else if (resultMap(srcid)._1 < weight)
            resultMap.update(srcid,(weight,length))
        }
        resultMap.toSeq.map(e => (e._1, e._2._1, e._2._2))
      }
      pathLength = pathLength + 1
    }
    subgraph
  }


  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SCTPIN")
    conf.set("spark.driver.maxResultSize", "10g")
    conf.set("spark.rdd.compress", "true")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.registerKryoClasses(Array(classOf[Seq[Seq[VertexId]]]))
    conf.set("spark.shuffle.consolidateFiles", "true")
    val sc = new SparkContext(conf)
    val graph = GetFromCsv(sc)

    val rankvalue = ShortestPath(sc,graph)
//    main_top_500.printGraph(rankvalue)

    sc.stop()
  }


}
