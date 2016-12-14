package com.tpin.utils

import com.tpin.entity.{EdgeAttr, Pattern, VertexAttr}
import com.tpin.main.TaxIncidencne_TaxRate
import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext

// 启动shell
// /opt/spark-1.5.1-bin-hadoop2.6/bin/spark-shell --executor-memory 12G --total-executor-cores 20 --executor-cores 4

/**
  * Created by yzk on 2016/7/8.
  * 本文件为考虑税率差异时关联交易模式的匹配
  * 所采用的指标为税负，与行业预警值相比较，若低于行业预警值的50%则认为存在异常
  */
object InterlockExperiment {

  def analysis(sc: SparkContext, hiveContext: HiveContext, OutPutPath: String, delta: Double) {
    println("delta:" + delta + "  Path:" + OutPutPath)

    //    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
    //    val sc = new SparkContext(conf)
    //    val hiveContext = new HiveContext(sc)
    val fw = CSVProcess.openCsvFileHandle(OutPutPath, false)
    CSVProcess.closeCsvFileHandle(fw)
    for (i <- 2 to 12) {
      val patterns1 = Pattern.getFromHiveTable(hiveContext, i, 1)
      val AbnormalVertices = TaxIncidencne_TaxRate.getAbnormalVerticesFromHiveTable2(hiveContext, i)
      val pattern3_withYCHZ = patterns1.keyBy(_.src_id)
        .leftOuterJoin(AbnormalVertices).map { case (id, (pattern, ychz)) =>
        pattern.src_flag = if (ychz.getOrElse((0D, 0D))._1 < (ychz.getOrElse((0D, 0D))._2 * delta)) 1 else 0
        pattern.src_sf = ychz.getOrElse((0D, 0D))._1
        (pattern.dst_id, pattern)
      }.leftOuterJoin(AbnormalVertices).map { case (id, (pattern, ychz)) =>
        pattern.dst_flag = if (ychz.getOrElse((0D, 0D))._1 < (ychz.getOrElse((0D, 0D))._2 * delta)) 1 else 0
        pattern.dst_sf = ychz.getOrElse((0D, 0D))._1
        pattern
      }.coalesce(20).persist()
      //      val patterns3 = Pattern.getFromHiveTable(hiveContext, i, 3)
      //      Pattern.CheckIntersect(patterns1, patterns3)
      //
      //      //annotation of david:month,index,count_src,count_dst,税负存在异常_src，税负存在异常_dst
      //      val p1 = patterns1.zipWithIndex().map { case (pattern, index) =>
      //        val result = Seq(i, index.toInt,pattern.src_id,pattern.dst_id,pattern.src_flag,pattern.dst_flag, Pattern.count1InCount(pattern.src_flag), Pattern.count1InCount(pattern.dst_flag)
      //          , (pattern.src_flag | 1), (pattern.dst_flag | 1))
      //        result
      //      }.collect().map(_.mkString(","))
      val p3 = pattern3_withYCHZ.zipWithIndex().map { case (pattern, index) =>
        val result = Seq(i, index.toInt, pattern.src_id, pattern.dst_id, pattern.src_flag, pattern.dst_flag, pattern.src_sf, pattern.dst_sf, Pattern.count1InCount(pattern.src_flag), Pattern.count1InCount(pattern.dst_flag)
          , (pattern.src_flag & 1), (pattern.dst_flag & 1))
        result
      }.collect().map(_.mkString(","))
      val fw = CSVProcess.openCsvFileHandle(OutPutPath, true)
      CSVProcess.saveAsCsvFile(fw, "month,id,src_id,dst_id,src_flag,dst_flag,src_sf,dst_sf,src_count,dst_count,taxrate_src,taxrate_dst")

      for (str <- p3) {
        CSVProcess.saveAsCsvFile(fw, str)
      }
      CSVProcess.closeCsvFileHandle(fw)
      println("month " + i + " save csv file finished!")
    }

  }

  //annotation of david:以模式的终点为中心，找出所有周围满足购方税率高于销方税率的企业
  def getConfirmedPattern(tpin_month: Graph[VertexAttr, EdgeAttr], pattern: RDD[Pattern], delta: Double): RDD[(Long, Long)]= {

    //annotation of david:将模式的源点标注为2，终点标注为1
    val message_src = pattern.flatMap(e => Seq((e.dst_id, 1), (e.src_id, 2)))
    val graph = tpin_month.outerJoinVertices(message_src) { case (vid, vattr, opt) =>
      (vattr, opt.getOrElse(0))
    }

    //annotation of david:只保留模式的异常交易边和终点相关企业
    val subgraph = graph.subgraph(epred = triplets => triplets.srcAttr._2 == 1 || (triplets.dstAttr._2 == 1 && triplets.srcAttr._2 == 2))

    val Message1 = subgraph.aggregateMessages[Seq[(Long, Double)]](ctx => {
      if (ctx.dstAttr._2 == 1 && ctx.attr.isTrade()) {
        ctx.sendToDst(Seq((ctx.srcId, ctx.attr.taxrate)))
      }
    }, _ ++ _)
    val Message2 = subgraph.aggregateMessages[Seq[Double]](ctx => {
      if (ctx.srcAttr._2 == 1 && ctx.attr.isTrade()) {
        ctx.sendToSrc(Seq(ctx.attr.taxrate))
      }
    }, _ ++ _)
    val Message1_satisfy_condtion_H2L = Message1.join(Message2).flatMap {
      case (dstid, (srcList, dst_taxrate)) =>
        srcList.filter { case (srcid, src_taxrate) =>
          //annotation of david:存在小于交易边税率的销项税率
          dst_taxrate.filter(dst => dst < src_taxrate).size > 0
        }.map { case (srcid, src_taxrate) => ((srcid, dstid), 1) }
    }
    val H2L = pattern.map(e => ((e.src_id, e.dst_id), e.dst_isHighSF(delta))).join(Message1_satisfy_condtion_H2L).filter {
      case (dstid, (isHighSF, useles)) => isHighSF
    }.keys
    val Message1_satisfy_condtion_L2H = Message1.join(Message2).flatMap {
      case (dstid, (srcList, dst_taxrate)) =>
        srcList.filter { case (srcid, src_taxrate) =>
          //annotation of david:存在大于交易边税率的销项税率
          dst_taxrate.filter(dst => dst > src_taxrate).size > 0
        }.map { case (srcid, src_taxrate) => ((srcid, dstid), 1) }
    }
    val L2H = pattern.map(e => ((e.src_id, e.dst_id), e.src_isHighSF(delta))).join(Message1_satisfy_condtion_L2H).filter {
      case (dstid, (isHighSF, useles)) => isHighSF
    }.keys
    H2L.union(L2H).distinct
  }


  //annotation of david:实验1: 检查两个模式的重叠个数，并输出到命令行
  def Experiment1(sc: SparkContext, hiveContext: HiveContext, args: Array[String]) {

    for (i <- 2 to 12) {
      println("=======================================")
      println("month " + i + " Processing!")

      val patterns1 = Pattern.getFromHiveTable(hiveContext, i, 1)
      val patterns2 = Pattern.getFromHiveTable(hiveContext, i, 2)
      Pattern.CheckIntersect(patterns1, patterns2)

    }

  }
  //annotation of david:实验2 :按照getConfirmedPattern的规则，得到模式中被确定存在问题的模式个数，并输出到命令行和本地文件
  def Experiment2(sc: SparkContext, hiveContext: HiveContext, args: Array[String]): Unit = {
    val savePath = "/tpin/wwd/month"
    val OutputPath = args(1)+"result.csv"
    val fw = CSVProcess.openCsvFileHandle(OutputPath, false)
    CSVProcess.saveAsCsvFile(fw, "month,pattern_confirmed_1,pattern_confirmed_2")
    CSVProcess.closeCsvFileHandle(fw)
    for (i <- 2 to 12) {
      println("=======================================")
      println("month " + i + " Processing!")
      val verticesPath = savePath + i + "/vertices"
      val edgesPath = savePath + i + "/edges"
      val tpin_month = InputOutputTools.getFromHDFS_Object(sc, verticesPath, edgesPath)

      val patterns1 = Pattern.getFromHiveTable(hiveContext, i, 1).distinct()
      val cp1=getConfirmedPattern(tpin_month ,patterns1,args(0).toDouble).count()
      println("pattern1 confirmed num: " + cp1 )

      val patterns2 = Pattern.getFromHiveTable(hiveContext, i, 2).distinct()
      val cp2 = getConfirmedPattern(tpin_month ,patterns2,args(0).toDouble).count()
      println("pattern2 confirmed num: " + cp2 )

      val fw = CSVProcess.openCsvFileHandle(OutputPath, true)

      CSVProcess.saveAsCsvFile(fw, i+","+cp1+","+cp2)

      CSVProcess.closeCsvFileHandle(fw)

    }
  }
}
