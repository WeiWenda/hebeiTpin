package com.tpin.main

import java.util.Arrays

import com.tpin.entity.{Pattern, EdgeAttr, VertexAttr, VertexType}
import com.tpin.utils._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._
import org.apache.spark.{SparkException, SparkConf, SparkContext}

// 启动shell
// /opt/spark-1.5.1-bin-hadoop2.6/bin/spark-shell --executor-memory 12G --total-executor-cores 20 --executor-cores 4

/**
  * Created by yzk on 2016/7/8.
  * 本文件为考虑税率差异时关联交易模式的匹配
  * 所采用的指标为税负，与行业预警值相比较，若低于行业预警值的50%则认为存在异常
  */
object TaxIncidencne_TaxRate {

  /**
    * 构建每个月的子图，并将子图保存至hdfs中，路径为：/tpin/yzk/month1/vertices && /tpin/yzk/month1/edges
    * 子图的节点格式为：（ID,(节点类型，识别号，社团ID)）
    * 子图的边的格式为：（srcID,dstID,(控制权重，投资权重，交易标志位（1.0表示该条边为交易边，0.0表示该条边为非交易边），交易税率，社团编号)）
    **/
  def getGraphInPerMonth(sc: SparkContext, oldTpin: Graph[VertexAttr, EdgeAttr]
                         , MonthTradeData: RDD[(String, String, Double, Double, String)], savePath: String) = {
    //       val data = dataList(0)

    // 为纳税人原始交易数据匹配纳税人重新编号后的ID srcsbh,dstsbh,je,se
    val TradeEdges1 = oldTpin.triplets.map(e => ((e.srcAttr.nsrsbh, e.dstAttr.nsrsbh), (e.srcId, e.dstId)))
      .join(MonthTradeData.map(e => ((e._1, e._2), (e._3, e._4))))
      .map { case ((srcsbh, dstsbh), ((srcid, dstid), (je, se))) => (srcid, dstid, je, se) }
    // 计算交易边对应的税率
    val TradeEdges2 = TradeEdges1.map(x => ((x._1, x._2), ((x._4 / x._3) * 100).round + 0.0))
    // 输出原始交易边总数
    println("edge_count_init:" + TradeEdges2.count())
    // 对具有相同起点、相同终点、相同税率的交易边进行合并
    val TradeEdges3 = TradeEdges2.distinct
    // 输出相同税率合并后的交易边总数
    println("edge_count_distinct:" + TradeEdges3.count())
    //    edge_jy_tmp: RDD[((Long, Long), Double)]
    // 针对税率差对交易边进行过滤：若某条交易边的税率与其终点对应纳税人的销售交易边的税率存在不相等的情况，则保留该条交易边，否则删除
    val edge_jy_filter1 = TradeEdges3.map(x => (x._1._2, (x._2, x._1._1))).join(TradeEdges3.map(x => (x._1._1, x._2)))
      .filter(x => x._2._1._1 != x._2._2).map(x => ((x._2._1._2, x._1), x._2._1._1)).distinct
    val edge_jy_filter2 = TradeEdges3.map(x => (x._1._1, (x._2, x._1._1))).join(TradeEdges3.map(x => (x._1._2, x._2)))
      .filter(x => x._2._1._1 != x._2._2).map(x => ((x._1, x._2._1._2), x._2._1._1)).distinct

    val TradeEdges4 = (edge_jy_filter1 ++ edge_jy_filter2).distinct()
    // 输出税率差异过滤后的交易边总数
    println("edge_count_taxrate:" + TradeEdges4.count())


    // 子图中的非交易边
    val NoTradeEdges = oldTpin.edges.filter(e => !e.attr.isTrade()).map { case e =>
      e.attr.trade_je = 0.0
      e.attr.w_trade = 0.0
      e.attr.se = 0.0
      e
    }
    // 子图中的交易边（此处和原图中的交易边进行Join，以得到交易边对应的社团编号）
    val TradeEdges5 = oldTpin.edges.map(e => ((e.srcId, e.dstId), e.attr))
      .join(TradeEdges4)
      .map { case ((srcid, dstid), (attr, taxrate)) =>
        attr.taxrate = taxrate
        Edge(srcid, dstid, attr)
      }
    // 输出存在税率差异的交易边中处于社团内部的交易边的总数
    println("edge_count_inCommunity:" + TradeEdges5.filter { e => e.attr.community_id > 0 }.count)

    // 子图中的边集合，其属性格式为：(控制权重，投资权重，交易标志位（1.0表示该条边为交易边，0.0表示该条边为非交易边），交易税率，社团编号)
    // 注意：此处和原有TPIN定义中的边不太一致，控制投资关系是合并在一起的，交易关系是独立的，而不是三者合并在一起
    val edge_result = NoTradeEdges ++ TradeEdges5

    // 构建该月子图，节点仍然和TPIN大图中一致
    val month_tpin = Graph(oldTpin.vertices, edge_result)

    // 将子图保存在HDFS中
    InputOutputTools.checkDirExist(sc, savePath + "/vertices")
    InputOutputTools.checkDirExist(sc, savePath + "/edges")

    month_tpin.vertices.saveAsObjectFile(savePath + "/vertices")
    month_tpin.edges.saveAsObjectFile(savePath + "/edges")
  }

  def getAbnormalVerticesFromHiveTable(hiveContext: HiveContext, month: Int) = {
    //import org.apache.spark.sql.hive.HiveContext
    //val hiveContext = new HiveContext(sc)
    //hiveContext.sql("select count(*) from tpin_zhibiao_wwd where month = 3 ").rdd.map(row=> row.getAs[Long](0)).collect
    val verticesInfo = hiveContext.sql("SELECT id,sf,ychz FROM tpin_zhibiao_wwd where month = " + month).rdd

    val vertices = verticesInfo.map(row => ((row.getAs[Long]("id"), (row.getAs[Int]("ychz"), row.getAs[Double]("sf")))))

    vertices
  }

  def getAbnormalVerticesFromHiveTable2(hiveContext: HiveContext, month: Int) = {
    //import org.apache.spark.sql.hive.HiveContext
    //val hiveContext = new HiveContext(sc)
    //hiveContext.sql("select count(*) from tpin_zhibiao_wwd where month = 3 ").rdd.map(row=> row.getAs[Long](0)).collect
    val verticesInfo = hiveContext.sql("SELECT id,sf,hyyjz FROM tpin_zhibiao_wwd where month = " + month).rdd

    val vertices = verticesInfo.map(row => ((row.getAs[Long]("id"), (row.getAs[Double]("sf"), row.getAs[Double]("hyyjz")))))

    vertices
  }

  def FindFiveYC(args: Array[String], hiveContext: HiveContext, sc: SparkContext): Unit = {
    // 设定各月子图的保存路径，用于子图的保存与加载
    val savePath = "/tpin/wwd/month"
    val OutputPath = args(1)

    println("Compute TaxIndicators Finishing!")

    //    val oldTpin = InputOutputTools.getFromHiveTable(hiveContext)

    // 交易数据集合 List[RDD[Array[String]]]
    //    val MonthsTradeData =InputOutputTools.getMonthsTradeDataFromHive(hiveContext)
    /**
      * 循环处理1-12月的数据，构建各月子图，并保存到HDFS中
      **/
    //    for (i <- 1 to 12) {
    //          println("=========================================")
    //          println("month:" + i)
    //          getGraphInPerMonth(sc,oldTpin,MonthsTradeData(i-1), savePath+i)
    //        }

    // ===================================================== 分割线 =====================================================
    /**
      * 循环处理1-12的子图，进行模式匹配，并对匹配结果中交易边关联的纳税人的税负指标进行异常分析
      **/
    //    hiveContext.sql("truncate table tpin_pattern_month_wwd")
    hiveContext.sql(" DROP TABLE IF EXISTS tpin_pattern_month_wwd")
    hiveContext.sql("CREATE TABLE tpin_pattern_month_wwd(id BIGINT, type INT, src_id BIGINT, dst_id BIGINT, src_flag  INT, dst_flag  INT, src_sf DOUBLE, dst_sf DOUBLE, month Int, vertices STRING, edges STRING, community_id BIGINT )")

    for (i <- 2 to 12) {

      val monthNum = sc.broadcast(i)

      println("month:" + i)
      //      val verticesPath = savePath + "1/vertices"
      //      val edgesPath = savePath + "1/edges"
      // 加载i月份的子图
      /*      val verticesPath = savePath + i + "/vertices"
            val edgesPath = savePath + i + "/edges"
            val tpin_month = InputOutputTools.getFromHDFS_Object(sc, verticesPath, edgesPath)
            println("the vertex number of month " + i + " is:\n " + tpin_month.vertices.count)
            println(tpin_month.edges.count)

            val pattern1_before = PatternMatching.Match_Pattern_1(sc, tpin_month).map { case e =>
              e.month = monthNum.value
              e
            }

      //      val pattern1_withYCHZ = pattern1.keyBy(_.src_id)
      //        .leftOuterJoin(AbnormalVertices).map { case (id, (pattern, opt)) =>
      //        pattern.src_flag = opt.getOrElse(0)
      //        (pattern.dst_id, pattern)
      //      }
      //        .leftOuterJoin(AbnormalVertices).map { case (id, (pattern, ychz)) =>
      //        pattern.dst_flag = ychz.getOrElse(0)
      //        pattern
      //      }.coalesce(20).persist()
      //      println("the pattern1 number of month " + i + " is " + pattern1_withYCHZ.count)


            val pattern3_before = PatternMatching.Match_Pattern_3(sc, tpin_month).map { case e =>
              e.month = monthNum.value
              e
            }
            InputOutputTools.checkDirExist(sc,savePath + i + "/pattern1")
            InputOutputTools.checkDirExist(sc,savePath + i + "/pattern3")
            pattern1_before.saveAsObjectFile(savePath + i + "/pattern1")
            pattern3_before.saveAsObjectFile(savePath + i + "/pattern3")*/


      val pattern3 = sc.objectFile[Pattern](savePath + i + "/pattern3")
      val AbnormalVertices = getAbnormalVerticesFromHiveTable(hiveContext, i)
      val pattern3_withYCHZ = pattern3.keyBy(_.src_id)
        .leftOuterJoin(AbnormalVertices).map { case (id, (pattern, ychz)) =>
        pattern.src_flag = ychz.getOrElse((0, 0D))._1
        pattern.src_sf = ychz.getOrElse((0, 0D))._2
        (pattern.dst_id, pattern)
      }.leftOuterJoin(AbnormalVertices).map { case (id, (pattern, ychz)) =>
        pattern.dst_flag = ychz.getOrElse((0, 0D))._1
        pattern.dst_sf = ychz.getOrElse((0, 0D))._2
        pattern
      }.coalesce(20).persist()
      println("the pattern3 number of month " + i + " is " + pattern3_withYCHZ.count)

      //      Pattern.saveAsHiveTable2(hiveContext, pattern1_withYCHZ)
      Pattern.saveAsHiveTable2(hiveContext, pattern3_withYCHZ)


    }
    println("Add TaxIndicators to Patterns Finishing!")
    InterlockExperiment.analysis(sc, hiveContext, OutputPath,args(0).toDouble)
    println("Patterns TaxIndicators Analyze Finishing!")

  }
  def FindFiveYC_mode1(args: Array[String], hiveContext: HiveContext, sc: SparkContext): Unit = {
    // 设定各月子图的保存路径，用于子图的保存与加载
    val savePath = "/tpin/wwd/month"
    val OutputPath = args(1)

//    ComputeTaxIndicators.run(sc, hiveContext, args(0).toDouble)
//    println("Compute TaxIndicators Finishing!")
//
//    //    val oldTpin = InputOutputTools.getFromHiveTable(hiveContext)
//
//    // 交易数据集合 List[RDD[Array[String]]]
//    //    val MonthsTradeData =InputOutputTools.getMonthsTradeDataFromHive(hiveContext)
//    /**
//      * 循环处理1-12月的数据，构建各月子图，并保存到HDFS中
//      **/
//    //    for (i <- 1 to 12) {
//    //          println("=========================================")
//    //          println("month:" + i)
//    //          getGraphInPerMonth(sc,oldTpin,MonthsTradeData(i-1), savePath+i)
//    //        }
//
//    // ===================================================== 分割线 =====================================================
//    /**
//      * 循环处理1-12的子图，进行模式匹配，并对匹配结果中交易边关联的纳税人的税负指标进行异常分析
//      **/
//    //    hiveContext.sql("truncate table tpin_pattern_month_wwd")
//    hiveContext.sql(" DROP TABLE IF EXISTS tpin_pattern_month_wwd")
//    hiveContext.sql("CREATE TABLE tpin_pattern_month_wwd(id BIGINT, type INT, src_id BIGINT, dst_id BIGINT, src_flag  INT, dst_flag  INT, src_sf DOUBLE, dst_sf DOUBLE, month Int, vertices STRING, edges STRING, community_id BIGINT )")
//
//    for (i <- 2 to 12) {
//
//      val monthNum = sc.broadcast(i)
//
//      println("month:" + i)
//      //      val verticesPath = savePath + "1/vertices"
//      //      val edgesPath = savePath + "1/edges"
//      // 加载i月份的子图
//      /*      val verticesPath = savePath + i + "/vertices"
//            val edgesPath = savePath + i + "/edges"
//            val tpin_month = InputOutputTools.getFromHDFS_Object(sc, verticesPath, edgesPath)
//            println("the vertex number of month " + i + " is:\n " + tpin_month.vertices.count)
//            println(tpin_month.edges.count)
//
//            val pattern1_before = PatternMatching.Match_Pattern_1(sc, tpin_month).map { case e =>
//              e.month = monthNum.value
//              e
//            }
//
//      //      val pattern1_withYCHZ = pattern1.keyBy(_.src_id)
//      //        .leftOuterJoin(AbnormalVertices).map { case (id, (pattern, opt)) =>
//      //        pattern.src_flag = opt.getOrElse(0)
//      //        (pattern.dst_id, pattern)
//      //      }
//      //        .leftOuterJoin(AbnormalVertices).map { case (id, (pattern, ychz)) =>
//      //        pattern.dst_flag = ychz.getOrElse(0)
//      //        pattern
//      //      }.coalesce(20).persist()
//      //      println("the pattern1 number of month " + i + " is " + pattern1_withYCHZ.count)
//
//
//            val pattern3_before = PatternMatching.Match_Pattern_3(sc, tpin_month).map { case e =>
//              e.month = monthNum.value
//              e
//            }
//            InputOutputTools.checkDirExist(sc,savePath + i + "/pattern1")
//            InputOutputTools.checkDirExist(sc,savePath + i + "/pattern3")
//            pattern1_before.saveAsObjectFile(savePath + i + "/pattern1")
//            pattern3_before.saveAsObjectFile(savePath + i + "/pattern3")*/
//
//
//      val pattern1 = sc.objectFile[Pattern](savePath + i + "/pattern1")
//      val AbnormalVertices = getAbnormalVerticesFromHiveTable(hiveContext, i)
//      val pattern3_withYCHZ = pattern1.keyBy(_.src_id)
//        .leftOuterJoin(AbnormalVertices).map { case (id, (pattern, ychz)) =>
//        pattern.src_flag = ychz.getOrElse((0, 0D))._1
//        pattern.src_sf = ychz.getOrElse((0, 0D))._2
//        (pattern.dst_id, pattern)
//      }.leftOuterJoin(AbnormalVertices).map { case (id, (pattern, ychz)) =>
//        pattern.dst_flag = ychz.getOrElse((0, 0D))._1
//        pattern.dst_sf = ychz.getOrElse((0, 0D))._2
//        pattern
//      }.coalesce(20).persist()
//      println("the pattern3 number of month " + i + " is " + pattern3_withYCHZ.count)
//
//      //      Pattern.saveAsHiveTable2(hiveContext, pattern1_withYCHZ)
//      Pattern.saveAsHiveTable2(hiveContext, pattern3_withYCHZ)
//
//
//    }
//    println("Add TaxIndicators to Patterns Finishing!")
    InterlockExperiment.analysis(sc, hiveContext, OutputPath+"run_result.csv",args(0).toDouble)
    println("Patterns TaxIndicators Analyze Finishing!")

  }
  def getTradeNeighbor(tpin_month: Graph[VertexAttr, EdgeAttr], pattern3_withYCHZ: RDD[Long]) = {
    val message_src = pattern3_withYCHZ.map(e => (e, 1))
    val graph = tpin_month.outerJoinVertices(message_src) { case (vid, vattr, opt) =>
      (vattr, opt.getOrElse(0))
    }
    val Message = graph.aggregateMessages[Int](ctx => {
      if (ctx.srcAttr._2 == 1 && ctx.attr.isTrade()) {
        ctx.sendToDst(1)
      }
      if (ctx.dstAttr._2 == 1 && ctx.attr.isTrade()) {
        ctx.sendToSrc(1)
      }
    }, _ + _)
    Message
  }

  def ConditionAnalysis(condition: RDD[(VertexId, PartitionID)], AbnormalVertices: RDD[(Long, (Double, Double))], OutPutPath: String, delta: Double, month: Int) = {
    val fw = CSVProcess.openCsvFileHandle(OutPutPath, true)
    val count = condition.count()
    val Gather = condition.leftOuterJoin(AbnormalVertices).map { case (vid, attr) =>
      attr._2.getOrElse(100D, 1D)
    }
    val count1 = Gather.filter { case (sf, yjz) => sf < (yjz * delta) }.count
    val count2 = Gather.filter { case (sf, yjz) => sf > (yjz * delta) && sf < (yjz * (2 - delta)) }.count
    val count3 = Gather.filter { case (sf, yjz) => sf > (yjz * (2 - delta)) }.count
    CSVProcess.saveAsCsvFile(fw, s"$month,$count1,$count2,$count3,$count")
    CSVProcess.closeCsvFileHandle(fw)
  }

  def FindNeighbor(args: Array[String], hiveContext: HiveContext, sc: SparkContext): Unit = {

    //    val oldTpin = InputOutputTools.getFromHiveTable(hiveContext)

    // 交易数据集合 List[RDD[Array[String]]]
    //    val MonthsTradeData =InputOutputTools.getMonthsTradeDataFromHive(hiveContext)
    /**
      * 循环处理1-12月的数据，构建各月子图，并保存到HDFS中
      **/
    //    for (i <- 1 to 12) {
    //          println("=========================================")
    //          println("month:" + i)
    //          getGraphInPerMonth(sc,oldTpin,MonthsTradeData(i-1), savePath+i)
    //        }
    val OutputPath = args(1)
    val savePath = "/tpin/wwd/month"
    for (i <- 2 to 12) {
      val verticesPath = savePath + i + "/vertices"
      val edgesPath = savePath + i + "/edges"
      val tpin_month = InputOutputTools.getFromHDFS_Object(sc, verticesPath, edgesPath)
      val pattern3_withYCHZ = Pattern.getFromHiveTable(hiveContext, i, 3)
      val AbnormalVertices = getAbnormalVerticesFromHiveTable2(hiveContext, i)
      //annotation of david:第一种情况


      val condition1 = getTradeNeighbor(tpin_month, pattern3_withYCHZ.filter(e => e.src_sf < e.dst_sf).map(_.src_id))
      ConditionAnalysis(condition1, AbnormalVertices, OutputPath + "SrcSrc.csv", args(0).toDouble, i)

      //annotation of david:第二种情况

      val condition2 = getTradeNeighbor(tpin_month, pattern3_withYCHZ.filter(e => e.src_sf < e.dst_sf).map(_.dst_id))
      ConditionAnalysis(condition2, AbnormalVertices, OutputPath + "SrcDst.csv", args(0).toDouble, i)

      //annotation of david:第三种情况

      val condition3 = getTradeNeighbor(tpin_month, pattern3_withYCHZ.filter(e => e.src_sf > e.dst_sf).map(_.src_id))
      ConditionAnalysis(condition3, AbnormalVertices, OutputPath + "DstSrc.csv", args(0).toDouble, i)

      //annotation of david:第四种情况

      val condition4 = getTradeNeighbor(tpin_month, pattern3_withYCHZ.filter(e => e.src_sf > e.dst_sf).map(_.dst_id))
      ConditionAnalysis(condition4, AbnormalVertices, OutputPath + "DstDst.csv", args(0).toDouble, i)
    }

  }
  //annotation of david:以模式的源点为中心，找出所有周围满足购方税率高于销方税率的企业
  def getNeighborH2L_Src(tpin_month: Graph[VertexAttr, EdgeAttr], pattern3_withYCHZ: RDD[VertexId]) = {
    val message_src = pattern3_withYCHZ.map(e => (e, 1))
    val graph = tpin_month.outerJoinVertices(message_src) { case (vid, vattr, opt) =>
      (vattr, opt.getOrElse(0))
    }
    val Message3=graph.aggregateMessages[Int](ctx => {
      if (ctx.srcAttr._2 == 1 && ctx.attr.isTrade()) {
        ctx.sendToDst(1)
      }
    }, (a,b)=>a)
    val graph2=graph.joinVertices(Message3){case (vid,(vattr,vtype),opt)=>
      (vattr,2)
    }
    val subgraph=graph2.subgraph(epred = triplets => triplets.srcAttr._2 == 2 || triplets.dstAttr._2 == 2 )

    val Message1 = subgraph.aggregateMessages[Double](ctx => {
      if (ctx.srcAttr._2 == 1 && ctx.attr.isTrade()) {
        ctx.sendToDst(ctx.attr.taxrate)
      }
    }, (a,b)=>a)
    val subgraph2=subgraph.outerJoinVertices(Message1){case (vid,(attr,vtype),opt)=>
      (attr,vtype,opt.getOrElse(0.0))
    }
    val Message2 = subgraph2.aggregateMessages[Seq[Double]](ctx => {
      if (ctx.srcAttr._2 == 2 && ctx.attr.isTrade()) {
        ctx.sendToSrc(Seq(ctx.attr.taxrate))
      }
    }, _ ++_)
    val subgraph3=subgraph2.outerJoinVertices(Message2){case (vid,(attr,vtype,src_taxrate),list)=>
      if (!list.isEmpty&&list.get.filter(_<src_taxrate).size > 0)
        (2,true)
      else
        (vtype,false)
    }
    subgraph3.vertices.filter(e=> e._2._1 == 2&& e._2._2 == true).map(e=>(e._1,1))


  }
  //annotation of david:以模式的终点为中心，找出所有周围满足购方税率高于销方税率的企业
  def getNeighborH2L_Dst(tpin_month: Graph[VertexAttr, EdgeAttr], pattern3_withYCHZ: RDD[VertexId]) = {
    val message_src = pattern3_withYCHZ.map(e => (e, 1))
    val graph = tpin_month.outerJoinVertices(message_src) { case (vid, vattr, opt) =>
      (vattr, opt.getOrElse(0))
    }
    val subgraph=graph.subgraph(epred = triplets => triplets.srcAttr._2 ==1 || triplets.dstAttr._2 ==1 )

    val Message1 = subgraph.aggregateMessages[Seq[(Long,Double)]](ctx => {
      if (ctx.dstAttr._2 == 1 && ctx.attr.isTrade()) {
        ctx.sendToDst(Seq((ctx.srcId,ctx.attr.taxrate)))
      }
    }, _++_)
    val Message2 = subgraph.aggregateMessages[Seq[Double]](ctx => {
      if( ctx.srcAttr._2 ==1 && ctx.attr.isTrade()){
        ctx.sendToSrc(Seq(ctx.attr.taxrate))
      }
    },_++_)
    Message1.join(Message2).map{case (dstid,(src_taxrate,dst_taxrate))=>
        src_taxrate.filter{case (srcid,e)=>dst_taxrate.filter(dst=>dst<e).size >0}.map(e=>(e._1,1))
    }.collect.flatten.distinct
  }
  //annotation of david:以模式的源点为中心，找出所有周围满足销方税率高于购方税率的企业
  def getNeighborL2H_Src(tpin_month: Graph[VertexAttr, EdgeAttr], pattern3_withYCHZ: RDD[VertexId]) = {
    val message_src = pattern3_withYCHZ.map(e => (e, 1))
    val graph = tpin_month.outerJoinVertices(message_src) { case (vid, vattr, opt) =>
      (vattr, opt.getOrElse(0))
    }
    val Message3=graph.aggregateMessages[Int](ctx => {
      if (ctx.srcAttr._2 == 1 && ctx.attr.isTrade()) {
        ctx.sendToDst(1)
      }
    }, (a,b)=>a)
    val graph2=graph.joinVertices(Message3){case (vid,(vattr,vtype),opt)=>
      (vattr,2)
    }
    val subgraph=graph2.subgraph(epred = triplets => triplets.srcAttr._2 == 2 || triplets.dstAttr._2 == 2 )

    val Message1 = subgraph.aggregateMessages[Double](ctx => {
      if (ctx.srcAttr._2 == 1 && ctx.attr.isTrade()) {
        ctx.sendToDst(ctx.attr.taxrate)
      }
    }, (a,b)=>a)
    val subgraph2=subgraph.outerJoinVertices(Message1){case (vid,(attr,vtype),opt)=>
      (attr,vtype,opt.getOrElse(0.0))
    }
    val Message2 = subgraph2.aggregateMessages[Seq[Double]](ctx => {
      if (ctx.srcAttr._2 == 2 && ctx.attr.isTrade()) {
        ctx.sendToSrc(Seq(ctx.attr.taxrate))
      }
    }, _ ++_)
    val subgraph3=subgraph2.outerJoinVertices(Message2){case (vid,(attr,vtype,src_taxrate),list)=>
      if (!list.isEmpty&&list.get.filter(_>src_taxrate).size > 0)
        (2,true)
      else
        (vtype,false)
    }
    subgraph3.vertices.filter(e=> e._2._1 == 2&& e._2._2 == true).map(e=>(e._1,1))


  }
  //annotation of david:以模式的终点为中心，找出所有周围满足销方税率高于购方税率的企业
  def getNeighborL2H_Dst(tpin_month: Graph[VertexAttr, EdgeAttr], pattern3_withYCHZ: RDD[VertexId]) = {
    val message_src = pattern3_withYCHZ.map(e => (e, 1))
    val graph = tpin_month.outerJoinVertices(message_src) { case (vid, vattr, opt) =>
      (vattr, opt.getOrElse(0))
    }
    val subgraph=graph.subgraph(epred = triplets => triplets.srcAttr._2 ==1 || triplets.dstAttr._2 ==1 )

    val Message1 = subgraph.aggregateMessages[Seq[(Long,Double)]](ctx => {
      if (ctx.dstAttr._2 == 1 && ctx.attr.isTrade()) {
        ctx.sendToDst(Seq((ctx.srcId,ctx.attr.taxrate)))
      }
    }, _++_)
    val Message2 = subgraph.aggregateMessages[Seq[Double]](ctx => {
      if( ctx.srcAttr._2 ==1 && ctx.attr.isTrade()){
        ctx.sendToSrc(Seq(ctx.attr.taxrate))
      }
    },_++_)
    Message1.join(Message2).map{case (dstid,(src_taxrate,dst_taxrate))=>
      src_taxrate.filter{case (srcid,e)=>dst_taxrate.filter(dst=>dst>e).size >0}.map(e=>(e._1,1))
    }.collect.flatten.distinct
  }
  def getTradeNeighbor_Src(tpin_month: Graph[VertexAttr, EdgeAttr], pattern3_withYCHZ: RDD[Long]) = {
    val message_src = pattern3_withYCHZ.map(e => (e, 1))
    val graph = tpin_month.outerJoinVertices(message_src) { case (vid, vattr, opt) =>
      (vattr, opt.getOrElse(0))
    }
    val Message = graph.aggregateMessages[Int](ctx => {
      if (ctx.dstAttr._2 == 1 && ctx.attr.isTrade()) {
        ctx.sendToSrc(1)
      }
    }, _ + _)
    Message
  }
  def getTradeNeighbor_Dst(tpin_month: Graph[VertexAttr, EdgeAttr], pattern3_withYCHZ: RDD[Long]) = {
    val message_src = pattern3_withYCHZ.map(e => (e, 1))
    val graph = tpin_month.outerJoinVertices(message_src) { case (vid, vattr, opt) =>
      (vattr, opt.getOrElse(0))
    }
    val Message = graph.aggregateMessages[Int](ctx => {
      if (ctx.srcAttr._2 == 1 && ctx.attr.isTrade()) {
        ctx.sendToDst(1)
      }
    }, _ + _)
    Message
  }
  def FilterNeighbor(args: Array[String], hiveContext: HiveContext, sc: SparkContext): Unit = {
    val OutputPath = args(1)
    val savePath = "/tpin/wwd/month"
    for (i <- 2 to 12) {
      val verticesPath = savePath + i + "/vertices"
      val edgesPath = savePath + i + "/edges"
      val tpin_month = InputOutputTools.getFromHDFS_Object(sc, verticesPath, edgesPath)
      val pattern3_withYCHZ = Pattern.getFromHiveTable(hiveContext, i, 3)
      val AbnormalVertices = getAbnormalVerticesFromHiveTable2(hiveContext, i)
      //annotation of david:第一种情况


      val condition1 = getNeighborH2L_Src(tpin_month, pattern3_withYCHZ.map(_.src_id))
      ConditionAnalysis(condition1, AbnormalVertices, OutputPath + "Src.csv", args(0).toDouble, i)

      //annotation of david:第二种情况

      val condition2 = sc.parallelize(getNeighborH2L_Dst(tpin_month, pattern3_withYCHZ.map(_.dst_id)))
      ConditionAnalysis(condition2, AbnormalVertices, OutputPath + "Dst.csv", args(0).toDouble, i)

    }

  }
  def FilterNeighbor_2(args: Array[String], hiveContext: HiveContext, sc: SparkContext): Unit = {
    val OutputPath = args(1)
    val savePath = "/tpin/wwd/month"
    for (i <- 2 to 12) {
      val verticesPath = savePath + i + "/vertices"
      val edgesPath = savePath + i + "/edges"
      val tpin_month = InputOutputTools.getFromHDFS_Object(sc, verticesPath, edgesPath)
      val pattern3_withYCHZ = Pattern.getFromHiveTable(hiveContext, i, 3)
      val AbnormalVertices = getAbnormalVerticesFromHiveTable2(hiveContext, i)
      //annotation of david:第一种情况


      val condition1 = getNeighborL2H_Src(tpin_month, pattern3_withYCHZ.map(_.src_id))
      ConditionAnalysis(condition1, AbnormalVertices, OutputPath + "Src.csv", args(0).toDouble, i)

      //annotation of david:第二种情况

      val condition2 = sc.parallelize(getNeighborL2H_Dst(tpin_month, pattern3_withYCHZ.map(_.dst_id)))
      ConditionAnalysis(condition2, AbnormalVertices, OutputPath + "Dst.csv", args(0).toDouble, i)

    }

  }
  def FilterNeighbor_3(args: Array[String], hiveContext: HiveContext, sc: SparkContext): Unit = {
    val OutputPath = args(1)
    val savePath = "/tpin/wwd/month"
    for (i <- 2 to 12) {
      val verticesPath = savePath + i + "/vertices"
      val edgesPath = savePath + i + "/edges"
      val tpin_month = InputOutputTools.getFromHDFS_Object(sc, verticesPath, edgesPath)
      val pattern3_withYCHZ = Pattern.getFromHiveTable(hiveContext, i, 3)
      val AbnormalVertices = getAbnormalVerticesFromHiveTable2(hiveContext, i)
      //annotation of david:第一种情况


      val condition1 = getTradeNeighbor_Src(tpin_month, pattern3_withYCHZ.map(_.src_id))
      ConditionAnalysis(condition1, AbnormalVertices, OutputPath + "Src.csv", args(0).toDouble, i)

      //annotation of david:第二种情况

//      val condition2 = sc.parallelize(getTradeNeighbor_Dst(tpin_month, pattern3_withYCHZ.map(_.dst_id)))
//      ConditionAnalysis(condition2, AbnormalVertices, OutputPath + "Dst.csv", args(0).toDouble, i)

    }

  }

  def main(args: Array[String]) {
    //    args.foreach(println)

    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)
    val hiveContext = new HiveContext(sc)
//    ComputeTaxIndicators.PrepareTPINandPatternAndIndicator(hiveContext,sc,args)

    ComputeTaxIndicators.FindBestWeightFilter(hiveContext,sc,args)

    //annotation of david:实验1，模式 1 与 模式 2 重叠的数量 （去重为前提）
//    ResultAnalysis.Experiment1(sc:SparkContext,hiveContext:HiveContext,args)

    //annotation of david:实验2 模式1与模式2的查准率
//    ResultAnalysis.Experiment2(sc:SparkContext,hiveContext:HiveContext,args)

    //annotation of david:模式3实验2
    //    FindFiveYC(args,hiveContext ,sc )

    //annotation of david:模式1实验2
//        FindFiveYC_mode1(args,hiveContext ,sc )

    //    FindNeighbor(args, hiveContext, sc)

    //annotation of david:模式3实验3 进项高于销项
//    FilterNeighbor(args, hiveContext, sc)
    //annotation of david:模式3实验3 销项高于进项
//        FilterNeighbor_2(args, hiveContext, sc)
    //annotation of david:模式3实验3 销项进项税率不限制
//            FilterNeighbor_3(args, hiveContext, sc)
  }
}
