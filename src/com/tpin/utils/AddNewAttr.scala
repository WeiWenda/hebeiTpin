package com.tpin.utils

import com.tpin.entity.{VertexAttr, EdgeAttr}
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Graph, Edge, VertexRDD}

import scala.collection.mutable.LinkedList

/**
 * Created by david on 6/30/16.
 */
object AddNewAttr {

  def FindFrequence(d: LinkedList[(Long,LinkedList[Long])]): Seq[(Long,Long)] = {
    val Frequencies = d
    val result =
      for (i <- 0 until Frequencies.length) yield
      for (j <- i + 1 until Frequencies.length) yield {
        val (vid1, list1) = Frequencies(i)
        val (vid2, list2) = Frequencies(j)
        if (list1.intersect(list2).size >= 2)
          Option(Iterable((vid1, vid2), (vid2, vid1)))
        else
          Option.empty
      }
    result.flatten.filter(!_.isEmpty).map(_.get).flatten
  }


  //annotation of david:添加严格版的互锁，依赖的投资边权重必须大于0.2
  def Add_Strict_IL(sc: SparkContext, graph: Graph[VertexAttr, EdgeAttr]): Graph[VertexAttr, EdgeAttr] = {

    //annotation of david:仅从单条路径发送消息，造成了算法的不一致
    // 含义：每个人所控制及间接控制企业的列表
    val Messages = graph.subgraph(epred = e => e.attr.isTZ()).aggregateMessages[LinkedList[Long]](
      ctx => ctx.sendToSrc(LinkedList((ctx.dstId, ctx.attr.w_tz)).filter(_._2>0.2).map(_._1)),_ ++ _)

    val KnowCompany=graph.outerJoinVertices(Messages) { case (vid, oldattr, option) =>
      option match {
        case Some(list) => (oldattr, list)
        case None => (oldattr, LinkedList[Long]())
      }
    }
    val Messages_off_direction = KnowCompany.subgraph(epred = e => e.attr.isTZ()).aggregateMessages[LinkedList[(Long,LinkedList[Long])]](
      ctx => ctx.sendToDst(LinkedList((ctx.srcId, ctx.srcAttr._2))),_ ++ _)
    val NewEdges = Messages_off_direction.flatMap{case (dstid,list)=> FindFrequence(list)}
      .distinct.join(graph.vertices)
      .map { case (src, (dst,vertexAttr))=>
          val edgeAttr = EdgeAttr()
          edgeAttr.is_IL = true
          edgeAttr.community_id=vertexAttr.community_id
          Edge(src, dst, edgeAttr)
        }
    Graph(graph.vertices, graph.edges.union(NewEdges))
  }

  def Add_Loose_IL(context: SparkContext, graph: Graph[VertexAttr, EdgeAttr]): Graph[VertexAttr, EdgeAttr] = {

    //annotation of david:the company need to know all of its board
    val Messages_off_direction = graph.subgraph(epred = e => e.attr.isTZ()&&e.attr.w_tz>0.2).aggregateMessages[LinkedList[Long]](
      ctx => ctx.sendToDst(LinkedList(ctx.srcId)),_ ++ _)
    val NewEdges = Messages_off_direction.flatMap{case (dstid,list)=> list.map(e1=>list.map(e2=>(e1,e2))).flatten.filter(e=> e._1 != e._2)}
      .distinct.join(graph.vertices)
      .map { case (src, (dst,vertexAttr)) =>
      val edgeAttr = EdgeAttr()
      edgeAttr.is_IL = true
      edgeAttr.community_id=vertexAttr.community_id
      Edge(src, dst, edgeAttr)
    }
    Graph(graph.vertices, graph.edges.union(NewEdges))
  }
}
