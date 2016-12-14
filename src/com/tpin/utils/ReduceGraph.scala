package com.tpin.utils

import com.tpin.entity.{Pattern, VertexType, EdgeAttr, VertexAttr}
import com.tpin.main.TestMain
import org.apache.hadoop.hive.ql.plan.TezEdgeProperty.EdgeType
import org.apache.spark.graphx._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{graphx, SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
 * Created by david on 7/1/16.
 */
object ReduceGraph {

  def constructVertexCommunityId(vertexId: VertexId, attr: VertexAttr,communityIdOpt: Option[VertexId]): (VertexAttr, VertexId) = {
    communityIdOpt match {
      case Some(communityId) => (attr, communityId)
      case None => (attr, -1L)
    }
  }
  //annotation of david:VertexId表示changTo,-1表示不发生变化
  def doReduction(newGraph: Graph[(VertexAttr,VertexId),EdgeAttr]): Graph[Iterable[(VertexId, VertexAttr)], Iterable[Edge[EdgeAttr]]] = {
    //将点的属性扩散到边上，利用的是triplets视图
    //newGraph.edges:     weight color
    //newGraph.vertices:  oldid color changeTo
    //所有不属于待聚合区域的点newId属性为-1，否则为聚合后的id。
    //output
    //changeEdge.edges:     weight color direct newsrc newdst
    //changeEdge.vertices:  oldId color newId
    //in out 边的处理不完备
    //结果为Graph[(graphx.VertexId, String, Long), (String, String,Long,Long)]
    val changeEdge= newGraph.mapTriplets(e => {
      if (e.dstAttr._2 != -1 && e.srcAttr._2 == -1)
        (e.attr,e.srcId,e.dstAttr._2,"in")

      else if (e.srcAttr._2 != -1 && e.dstAttr._2 == -1)
        (e.attr, e.srcAttr._2,e.dstId,"out")

      else if (e.dstAttr._2 != -1 && e.srcAttr._2 != -1) {
        if (e.dstAttr._2 == e.srcAttr._2)
          (e.attr,-1.toLong,-1.toLong,"remove")
        else
        //表示不属于同一个待聚合区域,完备了
          (e.attr, e.srcAttr._2,e.dstAttr._2,"between")
      }
      else
        (e.attr,e.srcId,e.dstId,"remain")
    })
//    changeEdge.edges.foreach{ case edge => println(edge) }
    //    changeEdge.edges.filter(_.attr._5=="in").count  //26596
    //    changeEdge.edges.filter(_.attr._5=="out").count  //19440
    //    changeEdge.edges.filter(_.attr._5=="remove").count  //15
    //    changeEdge.edges.filter(_.attr._5=="between").count  //625
    //    changeEdge.edges.filter(_.attr._5=="remain").count  //841397
    //    changeEdge.edges.count  //888073
    val changeVertex = newGraph
    //annotation of david:聚合点的属性
    val newVertex = changeVertex.vertices.map{case (vid,(attr,changeTo)) =>
      if(changeTo == -1)
        (vid,(vid,attr))
      else
        (changeTo,(vid,attr))}
      .groupByKey()
    //只滤除e.attr._2!=(-1)的边，即被标识为remove的边
    //多节点聚合边权重计算方法，首先需要计算weight_sum,repeat_num,weight_bigger
    val newEdge= changeEdge.edges.filter(e=> ! e.attr._4.equals("remove")).map {
        e => e.attr match {
          case (attr, newSrcId, newDstId,direct) =>
            ((newSrcId, newDstId), Edge(e.srcId,e.dstId,attr))
        }
      }.groupByKey().map{case ((src,dst),attr)=>Edge(src,dst,attr)}
//    newEdge.foreach{ case edge => println(edge) }
    Graph(newVertex, newEdge)
  }

  def reductionIL(graph:  Graph[VertexAttr,EdgeAttr]): Graph[Iterable[(VertexId, VertexAttr)], Iterable[Edge[EdgeAttr]]]= {

    val IL_subgraph=graph.subgraph(epred = e => e.attr.is_IL)

    val IL_side_Node = IL_subgraph.degrees

    val toMerge=Graph(IL_side_Node, IL_subgraph.mapEdges(e=>null).edges)

    val ConnectComponent=toMerge.connectedComponents().vertices

    val tempGraph=graph.outerJoinVertices(ConnectComponent)(constructVertexCommunityId)
    TestMain.printGraph(tempGraph)

    doReduction(tempGraph)
  }


//  def reductionIR(graph:  Graph[(String, String, String), (String, String)]):  Graph[String, (String, String)] = {
//    def msgFun(triplet: EdgeTriplet[(String, String, String), (String, String)]): Iterator[(VertexId, String)] = {
//      Iterator((triplet.srcId, "matching!"))
//    }
//
//    val MessageOfMatching = graph.subgraph(epred = e => e.attr._2 == "CL").mapReduceTriplets[String](msgFun, (a, b) => a)
//
//    val Matching = graph.joinVertices(MessageOfMatching)((id,oldString, newString)=>oldString match{
//      case (color,id,mc)=> ("Matching"+color,id,mc)
//    }).mapVertices((vid, attr)=>attr._1)
//
//    val ToMerge = Matching.subgraph(vpred = (vid, attr) => attr== "MatchingL", epred = e => e.attr._2 == "IR")
//
//    //更改待聚合点的属性为自己的VertexId
//    val g1 = ToMerge.mapVertices((vid, attr) => vid)
//
//    def sendMsg(e: EdgeTriplet[VertexId, (String, String)]): Iterator[(VertexId, VertexId)] = {
//      //很大程序上依赖IR边是双向边，才能够只向终点发消息
//      if (e.srcAttr < e.dstAttr)
//        Iterator((e.dstId, e.srcAttr))
//      else
//        Iterator((e.dstId, e.dstAttr))
//    }
//    def gatherMsg(a: VertexId, b: VertexId): VertexId = {
//      if (a < b) a
//      else b
//    }
//    //得到所有待聚合区域的聚合目标点
//    val MessageOfConnectPoint = g1.mapReduceTriplets[VertexId](sendMsg, gatherMsg)
//    //所有不属于待聚合区域的点changeTo属性为-1，否则为聚合后的id。
//    val newGraph = graph.outerJoinVertices(MessageOfConnectPoint)((vid, color, changeTo) => (vid, color._1, changeTo.getOrElse(-1.toLong)))
//    doReduction(newGraph)
//  }

//  def reductionCL(graph:  Graph[(String, String, String), (String, String)]):Graph[String, (String, String)]= {
//    def ScatterMsg1(triplet: EdgeTriplet[(String, String, String), (String, String)]): Iterator[(VertexId, ArrayBuffer[Long])] = {
//      Iterator((triplet.srcId, ArrayBuffer(triplet.dstId.toLong)))
//    }
//    def GatherMsg1(a: ArrayBuffer[Long], b: ArrayBuffer[Long]): ArrayBuffer[Long] = {
//      a++b
//    }
//    val MessageOfMatching = graph.subgraph(epred = e => e.attr._2 == "CL").mapReduceTriplets[ArrayBuffer[Long]](ScatterMsg1,GatherMsg1)
//
//    val Matching=MessageOfMatching.filter(_._2.size>1).flatMap{
//      case(vid:VertexId,arr:ArrayBuffer[Long]) =>
//        val min=arr.min
//        arr.map{case vid=>(vid,min)}
//    }//10713
//    //所有不属于待聚合区域的点changeTo属性为-1，否则为聚合后的id，ConnectPoint聚合后的id是它自己。
//    val newGraph = graph.outerJoinVertices(Matching){case (vid, (color,sbh,name), changeTo)=> (vid, color.toString, changeTo.getOrElse(-1.toLong))}
//    //    val change=newGraph.vertices.filter(v=>v._2._3!=(-1))
//    doReduction(newGraph)
//  }

}
