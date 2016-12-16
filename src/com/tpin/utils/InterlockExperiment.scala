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
    def getConfirmedPattern(tpin_month: Graph[VertexAttr, EdgeAttr], pattern: RDD[Pattern], delta: Double): RDD[(Long, Long)] = {

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
    def Experiment1(sc: SparkContext, hiveContext: HiveContext) {

        for (i <- 2 to 12) {
            println("=======================================")
            println("month " + i + " Processing!")

            val patterns1 = Pattern.getFromHiveTable(hiveContext, i, 1)
            val patterns2 = Pattern.getFromHiveTable(hiveContext, i, 2)
            Pattern.CheckIntersect(patterns1, patterns2)

        }

    }

    //annotation of david:实验2 :按照getConfirmedPattern的规则，得到模式中被确定存在问题的模式个数，并输出到命令行和本地文件
    def Experiment2(sc: SparkContext, hiveContext: HiveContext,weight_sfyc: Double = 0.5): Unit = {
        val savePath = "/tpin/wwd/month"
        for (i <- 2 to 12) {
            println("=======================================")
            println("month " + i + " Processing!")
            val verticesPath = savePath + i + "/vertices"
            val edgesPath = savePath + i + "/edges"
            val tpin_month = InputOutputTools.getFromHdfsObject(sc, verticesPath, edgesPath)

            val patterns1 = Pattern.getFromHiveTable(hiveContext, i, 1).distinct()
            val cp1 = getConfirmedPattern(tpin_month, patterns1, weight_sfyc).count()
            println("pattern1 confirmed num: " + cp1)

            val patterns2 = Pattern.getFromHiveTable(hiveContext, i, 2).distinct()
            val cp2 = getConfirmedPattern(tpin_month, patterns2, weight_sfyc).count()
            println("pattern2 confirmed num: " + cp2)
        }
    }

    //annotation of david:实验1和实验2的二合一，减少了模式数据的输入输出，但是实验2将只输出到命令行
    //args: 第一个参数为 delta，税负预警 ，第二个参数为 weight，前件路径的权重要求
    def FindBestWeightFilter(hiveContext: HiveContext, sc: SparkContext, args: Array[String]): Unit = {
        // 设定各月子图的保存路径，用于子图的保存与加载
        val savePath = "/tpin/wwd/month"

        for (i <- 2 to 12) {

            val monthNum = sc.broadcast(i)
            println("=======================================")
            println("month:" + i)
            // 加载i月份的子图
            val verticesPath = savePath + i + "/vertices"
            val edgesPath = savePath + i + "/edges"
            val tpin_month = InputOutputTools.getFromHdfsObject(sc, verticesPath, edgesPath)
            println("the vertex number of month " + i + " is:\n " + tpin_month.vertices.count)
            println(tpin_month.edges.count)

            val pattern1_before = PatternMatching.Match_Director_Interlock(sc, tpin_month, args(1).toDouble).map { case e =>
                e.month = monthNum.value
                e
            }
            val AbnormalVertices1 = ComputeTaxIndicators.getAbnormalVerticesFromHiveTable(hiveContext, i)
            val pattern1_withYCHZ = pattern1_before.keyBy(_.src_id)
                .leftOuterJoin(AbnormalVertices1).map { case (id, (pattern, ychz)) =>
                pattern.src_flag = ychz.getOrElse((0, 0D, 0D))._1
                pattern.src_sf = ychz.getOrElse((0, 0D, 0D))._2
                pattern.src_hyyj = ychz.getOrElse((0, 0D, 0D))._3
                (pattern.dst_id, pattern)
            }.leftOuterJoin(AbnormalVertices1).map { case (id, (pattern, ychz)) =>
                pattern.dst_flag = ychz.getOrElse((0, 0D, 0D))._1
                pattern.dst_sf = ychz.getOrElse((0, 0D, 0D))._2
                pattern.dst_hyyj = ychz.getOrElse((0, 0D, 0D))._3
                pattern
            }.coalesce(20).persist()
            println("the pattern1 number of month " + i + " is " + pattern1_withYCHZ.count)



            val pattern2_before = PatternMatching.Match_Pattern_2(sc, tpin_month, 0.0).map { case e =>
                e.month = monthNum.value
                e
            }
            val AbnormalVertices = ComputeTaxIndicators.getAbnormalVerticesFromHiveTable(hiveContext, i)
            val pattern2_withYCHZ = pattern2_before.keyBy(_.src_id)
                .leftOuterJoin(AbnormalVertices).map { case (id, (pattern, ychz)) =>
                pattern.src_flag = ychz.getOrElse((0, 0D, 0D))._1
                pattern.src_sf = ychz.getOrElse((0, 0D, 0D))._2
                pattern.src_hyyj = ychz.getOrElse((0, 0D, 0D))._3
                (pattern.dst_id, pattern)
            }.leftOuterJoin(AbnormalVertices).map { case (id, (pattern, ychz)) =>
                pattern.dst_flag = ychz.getOrElse((0, 0D, 0D))._1
                pattern.dst_sf = ychz.getOrElse((0, 0D, 0D))._2
                pattern.dst_hyyj = ychz.getOrElse((0, 0D, 0D))._3
                pattern
            }.coalesce(20).persist()
            println("the pattern2 number of month " + i + " is " + pattern2_withYCHZ.count)

            Pattern.CheckIntersect(pattern1_withYCHZ, pattern2_withYCHZ)

            val pattern1_distinct = pattern1_withYCHZ.distinct()
            val cp1 = InterlockExperiment.getConfirmedPattern(tpin_month, pattern1_distinct, args(0).toDouble).count()
            println("pattern1 confirmed num: " + cp1)

            val pattern2_distinct = pattern2_withYCHZ.distinct()
            val cp2 = InterlockExperiment.getConfirmedPattern(tpin_month, pattern2_distinct, args(0).toDouble).count()
            println("pattern2 confirmed num: " + cp2)


        }
        println("FindBestWeightFilter Finishing!")

    }
}
