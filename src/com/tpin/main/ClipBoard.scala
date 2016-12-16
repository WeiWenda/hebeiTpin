package com.tpin.main

import com.tpin.entity.{Pattern, EdgeAttr, VertexAttr}
import com.tpin.utils.AddNewAttr.Path
import com.tpin.utils._
import org.apache.spark.{graphx, SparkContext}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import java.util.{Date, Arrays}

import scala.collection.mutable.LinkedList
object ClipBoard{
    //annotation of david:构建IL开头的路径集合
    def getPathStartFromIL(graph: Graph[VertexAttr, EdgeAttr],weight:Double)= {

        //annotation of david:路径的第一条边一定是IL
        val IL_Paths = graph
            .subgraph(epred = e => e.attr.is_IL)
            .mapVertices((id, vd) => Seq(Seq(id)))
            .aggregateMessages[Seq[Seq[VertexId]]](ctx =>
            ctx.sendToDst(ctx.srcAttr.map(_ ++ Seq(ctx.dstId))),_ ++ _
        )
        // 使用接收到消息的顶点及其边构建子图，二分查找判断顶点编号是否在上一步的vertexId中提高效率
        val initialGraph = graph.outerJoinVertices(IL_Paths) { case (vid, oldattr, option) =>
            option match {
                case Some(list) => list
                case None => Seq[Seq[VertexId]]()
            }
        }
        AddNewAttr.getPath(initialGraph,weight,initLength = 2)
    }
    //annotation of david:构建互锁关联交易模式（单向环和双向环），未考虑第三方企业的选择
    def matchPattern_2(graph: Graph[VertexAttr, EdgeAttr],weight:Double): RDD[Pattern] = {
        // 从带社团编号的TPIN中提取交易边
        //annotation of david:属性为社团id
        val tradeEdges = graph.edges.filter(_.attr.isTrade()).map(edge => ((edge.srcId, edge.dstId), edge.attr.community_id)).cache()
        // 构建路径终点和路径组成的二元组
        val pathsWithLastVertex = getPathStartFromIL(graph,weight).flatMap{case (vid,paths)=>paths.map((vid,_))}.cache()
        // 交易边的起点、终点和上一步的二元组join
        //annotation of david:一个交易边的两条前件路径，（包含他本身的）和社团id
        val pathTuples = tradeEdges
            .keyBy(_._1._1).join(pathsWithLastVertex)
            .keyBy(_._2._1._1._2).join(pathsWithLastVertex)
            .map{case(dstid,( (srcid,(((srcid2,dstid2),cid),srcpath)) ,dstpath)) =>
                ((srcpath,dstpath),cid)
            }
        // 匹配模式，规则为“1、以交易边起点为路径终点的路径的起点 等于 以交易边终点为路径终点的路径的起点（去除不成环的情况） 2、两条路径中顶点的交集的元素个数为1（去除交叉的情况）”
        val pattern = pathTuples
            .filter { case ((srcpath, dstpath), cid) =>
                srcpath(0) == dstpath(1) && srcpath(1) == dstpath(0) && srcpath.intersect(dstpath).size == 2
            }
            .map { case ((srcpath, dstpath), cid) =>
                val edges=
                    (
                        (Seq[Seq[Long]]()++ srcpath.sliding(2) ++ dstpath.sliding(2)).filter(_.size==2) ++
                            Seq(Seq(srcpath.last,dstpath.last))
                        ).map(list=> (list(0),list(1)))
                val vertices=(srcpath.tail.tail++dstpath)
                val result=Pattern(2,edges,vertices)
                result.communityId=cid
                result
            }
        pattern
    }
}
