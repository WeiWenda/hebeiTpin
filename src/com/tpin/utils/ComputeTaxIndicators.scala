package com.tpin.utils

import com.tpin.entity.{EdgeAttr, VertexAttr, Pattern}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}

// 启动shell
// /opt/spark-1.5.1-bin-hadoop2.6/bin/spark-shell --executor-memory 12G --total-executor-cores 20 --executor-cores 4

/**
  * 数据预处理，保存到数据库中，此程序考虑了税率差异，税率差异考虑了源节点作为购方的交易记录和目标节点作为销方的交易记录
  * Created by yzk on 2016/7/29.
  */
object ComputeTaxIndicators {
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

    //annotation of david:从hive的tpin_zhibiao_wwd中读入每个点的异常汇总，税负值，税负预警值
    def getAbnormalVerticesFromHiveTable(hiveContext: HiveContext, month: Int): RDD[(Long, (Int, Double, Double))] = {
        //import org.apache.spark.sql.hive.HiveContext
        //val hiveContext = new HiveContext(sc)
        //hiveContext.sql("select count(*) from tpin_zhibiao_wwd where month = 3 ").rdd.map(row=> row.getAs[Long](0)).collect
        val verticesInfo = hiveContext.sql("SELECT id,sf,ychz,hyyjz FROM tpin_zhibiao_wwd where month = " + month).rdd

        val vertices = verticesInfo.map(row => ((row.getAs[Long]("id"), (row.getAs[Int]("ychz"), row.getAs[Double]("sf"), row.getAs[Double]("hyyjz")))))

        vertices
    }

    //annotation of david:将每个月的tpin数据存入HDFS、模式1和模式3及指标存入Hive和HDFS
    def PrepareTPINandPatternAndIndicator(hiveContext: HiveContext, sc: SparkContext): Unit = {
        // 设定各月子图的保存路径，用于子图的保存与加载
        val savePath = "/tpin/wwd/month"

        val oldTpin = InputOutputTools.getFromHiveTable(hiveContext)

        // 交易数据集合 List[RDD[Array[String]]]
        val MonthsTradeData = InputOutputTools.getMonthsTradeDataFromHive(hiveContext)

        /**
          * 循环处理1-12月的数据，构建各月子图，并保存到HDFS中
          **/
        for (i <- 1 to 12) {
            println("=========================================")
            println("month:" + i)
            getGraphInPerMonth(sc, oldTpin, MonthsTradeData(i - 1), savePath + i)
        }

        //annotation of david:计算标准情况下的异常指标
//        ComputeTaxIndicators.run(sc, hiveContext, MonthsTradeData)


        // ===================================================== 分割线 =====================================================
        /**
          * 循环处理1-12的子图，进行模式匹配，并对匹配结果中交易边关联的纳税人的税负指标进行异常分析
          **/
        hiveContext.sql(" DROP TABLE IF EXISTS tpin_pattern_month_wwd")
        hiveContext.sql("CREATE TABLE tpin_pattern_month_wwd(id BIGINT, type INT, src_id BIGINT, dst_id BIGINT, src_flag  INT, dst_flag  INT, src_sf DOUBLE, dst_sf DOUBLE,src_hyyj DOUBLE,dst_hyyj DOUBLE, month Int, vertices STRING, edges STRING, community_id BIGINT )")

        for (i <- 2 to 12) {

            val monthNum = sc.broadcast(i)

            println("month:" + i)
            // 加载i月份的子图
            val verticesPath = savePath + i + "/vertices"
            val edgesPath = savePath + i + "/edges"
            val tpin_month = InputOutputTools.getFromHdfsObject(sc, verticesPath, edgesPath)
            println("the vertex number of month " + i + " is:\n " + tpin_month.vertices.count)
            println(tpin_month.edges.count)

            val pattern1_before = PatternMatching.Match_Director_Interlock(sc, tpin_month, 0.0).map { case e =>
                e.month = monthNum.value
                e
            }
            InputOutputTools.checkDirExist(sc, savePath + i + "/pattern1")
            pattern1_before.saveAsObjectFile(savePath + i + "/pattern1")
            val pattern1 = sc.objectFile[Pattern](savePath + i + "/pattern1")
            val AbnormalVertices1 = getAbnormalVerticesFromHiveTable(hiveContext, i)
            val pattern1_withYCHZ = pattern1.keyBy(_.src_id)
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
            Pattern.saveAsHiveTable2(hiveContext, pattern1_withYCHZ)

            val pattern2_before = PatternMatching.matchPattern_2(tpin_month, 0.0).map { case e =>
                e.month = monthNum.value
                e
            }
            InputOutputTools.checkDirExist(sc, savePath + i + "/pattern2")
            pattern2_before.saveAsObjectFile(savePath + i + "/pattern2")
            val pattern2 = sc.objectFile[Pattern](savePath + i + "/pattern2")
            val AbnormalVertices = getAbnormalVerticesFromHiveTable(hiveContext, i)
            val pattern2_withYCHZ = pattern2.keyBy(_.src_id)
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
            Pattern.saveAsHiveTable2(hiveContext, pattern2_withYCHZ)

            //      Pattern.CheckIntersect(pattern1_withYCHZ, pattern2_withYCHZ)

        }
        println("PrepareTPINandPatternAndIndicator Finishing!")

    }

    def run(sc: SparkContext, hiveContext: HiveContext, dataList: List[RDD[(String, String, Double, Double, String)]], delta: Double = 0.5) {

        val savePath = "/tpin/wwd/zhibiao/month"
        // 对各月信息进行汇总处理，得到纳税人节点的相关信息，最终格式为：(id,(type,sbh,community_id,不含税销售收入，进项税额，销项税额，应纳税额，税负，发票用量，行业编号，税负指标行业预警值))
        for (i <- 0 to 11) {
            val data = dataList(i)

            // 交易信息预处理
            val data0 = data.map(x => (x._1, x._2, x._3, x._4))
            // 汇总计算交易金额、销项税额、发票使用数量
            val data1 = data0.map(x => (x._1, (x._3, x._4, 1))).reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3))
            // 汇总计算进项税额
            val data2 = data0.map(x => (x._2, x._4)).reduceByKey((a, b) => (a + b))
            // 合并以上汇总结果，并计算应纳税额，税负指标，形成以下格式：(sbh,(不含税销售收入，进项税额，销项税额，应纳税额，税负，发票用量))
            val data3 = data1.leftOuterJoin(data2).map { case (sbh, ((je, xxse, count), opt)) => (sbh, ((je, xxse, count), opt.getOrElse(0D))) }
                .map { case (sbh, ((je, xxse, count), jxse)) => (sbh, (je, jxse, xxse, xxse - jxse, (xxse - jxse) / je, count)) }
            // 添加纳税人行业编号属性
            val hyInfo = hiveContext.sql("SELECT nsrsbh,hy_dm FROM wd_nsr_kz").rdd.repartition(120).map(row => ((row.getAs[String]("nsrsbh"), (row.getAs[String]("hy_dm")))))
            val data4 = data3.join(hyInfo).map { case (nsrsbh, (attr, hydm)) => (hydm, (nsrsbh, attr)) }
            // 添加税负指标的行业预警值，形成以下格式：(sbh,(不含税销售收入，进项税额，销项税额，应纳税额，税负，发票用量，行业编号，税负指标行业预警值))
            val taxIncidenceAlertedValue = sc.textFile("/root/tpinDataCsv/TaxIncidence.csv").map(x => x.split(",")).map(x => (x(0), x(1).toDouble / 100))
            val data5 = data4.join(taxIncidenceAlertedValue).map { case (hydm, ((nsrsbh, attr), hyyjz)) =>
                (nsrsbh, (attr._1, attr._2, attr._3, attr._4, attr._5, attr._6, hydm, hyyjz))
            }
            // 获取纳税人识别号对应的纳税人节点的信息，并添加到上述结果中，形成以下格式：(id,type,sbh,community_id,不含税销售收入，进项税额，销项税额，应纳税额，税负，发票用量，行业编号，税负指标行业预警值)
            val verticesInfo = hiveContext.sql("SELECT id,type,sbh,community_id FROM tpin_vertex_wwd").rdd.repartition(120).map(row => (row.getAs[String]("sbh"), ((row.getAs[Int]("type"), (row.getAs[Long]("id"), row.getAs[Long]("community_id"))))))
                .filter(e => (e._2._1 & 1) == 1)
            val data6 = data5.join(verticesInfo).map(x => (x._2._2._2._1, x._2._2._1, x._1, x._2._2._2._2, x._2._1._1, x._2._1._2, x._2._1._3, x._2._1._4, x._2._1._5, x._2._1._6, x._2._1._7, x._2._1._8))
            // 保存stage1结果到hdfs中
            InputOutputTools.checkDirExist(sc, savePath + (i + 1) + "/stage1")
            data6.saveAsTextFile(savePath + (i + 1) + "/stage1")
        }

        // ===================================================================================================================
        // 对上一阶段各月的信息进行指标计算，并判断是否存在异常，将最终结果保存到hive中（对于非纳税人节点，赋值为0或空）
        // ==============================此处对指标判断进行了更正，本期－上期，且对于未匹配到上月纳税人的节点判断了税负指标的异常与否
        hiveContext.sql("truncate table tpin_zhibiao_wwd")
        for (i <- 2 to 12) {
            //      val i = 2
            // (id,(type,sbh,community_id,不含税销售收入，进项税额，销项税额，应纳税额，税负，发票用量，行业编号，税负指标行业预警值))
            val thisMonth = sc.textFile(savePath + i + "/stage1").map(x => x.replace("(", "")).map(x => x.replace(")", "")).map(x => x.split(","))
                .map(x => (x(0).toLong, (x(1).toInt, x(2), x(3).toLong, x(4).toDouble, x(5).toDouble, x(6).toDouble, x(7).toDouble, x(8).toDouble, x(9).toInt, x(10), x(11).toDouble)))
            val lastMonth = sc.textFile(savePath + (i - 1) + "/stage1").map(x => x.replace("(", "")).map(x => x.replace(")", "")).map(x => x.split(","))
                .map(x => (x(0).toLong, (x(1).toInt, x(2), x(3).toLong, x(4).toDouble, x(5).toDouble, x(6).toDouble, x(7).toDouble, x(8).toDouble, x(9).toInt, x(10), x(11).toDouble)))

            // 左外连接，对于右侧匹配不到的，赋值为None
            val data = thisMonth.leftOuterJoin(lastMonth).map(x => (x._1, (x._2._1, x._2._2.getOrElse((0, "", 0L, 0.0, 0.0, 0.0, 0.0, 0.0, 0, "", 0.0)))))
            // 指标计算1：第二组属性依次为（税负变动率，发票变动量，发票变动率，进项税额变动率，销项税额变动率，应纳税额变动率，销售额变动率）
            // 指标计算2：利用指标计算1的结果进一步计算指标，属性依次为（税负变动率，发票变动量，发票变动率，进项税额变动率，销项税额变动率，进销变动指标，应纳税额变动率，销售额变动率，弹性系数指标）
            val data1 = data.filter(_._2._2._1 != 0).map { case (id, ((vtype1, sbh1, community_id1, sr1, jxse1, xxse1, se1, sf1, count1, hy1, threshold1),
            (vtype2, sbh2, community_id2, sr2, jxse2, xxse2, se2, sf2, count2, hy2, threshold2))) =>
                (id, ((vtype1, sbh1, community_id1, sr1, jxse1, xxse1, se1, sf1, count1, hy1, threshold1),
                    ((sf1 - sf2) / sf2, count1 - count2, (count1 - count2) / count2.toDouble, (jxse1 - jxse2) / jxse2, (xxse1 - xxse2) / xxse2, (se1 - se2) / se2, (sr1 - sr2) / sr2)))
            }.map { case (id, (thisMonth11, (sfL, countD, countL, jxseL, xxseL, seL, srL))) =>
                (id, (thisMonth11, (sfL, countD, countL, jxseL, xxseL, (jxseL - xxseL) / xxseL, seL, srL, srL / seL)))
            }

            // 指标异常分析，分析结果保存至第三组属性，（税负异常，税负变动异常，发票变动异常，进销变动异常，弹性系数异常，异常汇总[二进制形式，由低位到高位依次表示前述异常]）
            def binaryException(flags: (Boolean, Boolean, Boolean, Boolean, Boolean)) = {
                var temp = 0
                if (flags._1) temp = temp + 1
                if (flags._2) temp = temp + 2
                if (flags._3) temp = temp + 4
                if (flags._4) temp = temp + 8
                if (flags._5) temp = temp + 16
                temp
            }

            val data2 = data1.map { case (id, ((vtype1, sbh1, community_id1, sr1, jxse1, xxse1, se1, sf1, count1, hy1, threshold1),
            (sfL, countD, countL, jxseL, xxseL, bdzb, seL, srL, txxs))) =>
                (id, ((vtype1, sbh1, community_id1, sr1, jxse1, xxse1, se1, sf1, count1, hy1, threshold1), (sfL, countD, countL, jxseL, xxseL, bdzb, seL, srL, txxs),
                    (sf1 < (threshold1 * delta), sfL.abs > 0.3, countD > 10 && countL >= 0.3, bdzb > 0.1, (txxs > 1 && seL > 0 && srL > 0) || (txxs > 0 && txxs < 1 && seL < 0 && srL < 0) || (txxs < 0 && seL > 0 && srL < 0))))
            }.map(x => (x._1, (x._2._1, x._2._2, x._2._3, binaryException(x._2._3))))

            // 处理未匹配到的纳税人节点信息
            val data3 = data.filter(_._2._2._1 == 0).map(x => (x._1, (x._2._1, (0.0, 0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0), (x._2._1._8 < (x._2._1._11 * delta), false, false, false, false))))
                .map(x => (x._1, (x._2._1, x._2._2, x._2._3, if (binaryException(x._2._3) != 0) binaryException(x._2._3) else -1)))
            //        .map(x => (x._1, (x._2._1, x._2._2, x._2._3, binaryException(x._2._3))))

            // 获取非纳税人节点信息，并将其格式转换为和上述纳税人节点一致
            val verticesInfo = hiveContext.sql("SELECT id,type,sbh,community_id FROM tpin_vertex_w_IL").rdd.repartition(120)
            val vertices = verticesInfo.map(row => ((row.getAs[Long]("id"), (row.getAs[Int]("type"), row.getAs[String]("sbh"), row.getAs[Long]("community_id")))))
            val data4 = vertices.filter(e => (e._2._1 & 1) == 0).
                map(x => (x._1, ((x._2._1, x._2._2, x._2._3, 0.0, 0.0, 0.0, 0.0, -1.0, 0, "", 0.0), (0.0, 0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0), (false, false, false, false, false), -2)))

            val vertices_all = data2 ++ data3 ++ data4
            //      vertices_all.saveAsObjectFile(savePath + i + "/vertices")
            hiveContext.createDataFrame(vertices_all.map(x => Row(x._1, x._2._1._1, x._2._1._2, x._2._1._3, x._2._1._4, x._2._1._5, x._2._1._6, x._2._1._7, x._2._1._8, x._2._1._9, x._2._1._10, x._2._1._11, x._2._2._1, x._2._2._2, x._2._2._3, x._2._2._4, x._2._2._5, x._2._2._6, x._2._2._7, x._2._2._8, x._2._2._9, x._2._3._1, x._2._3._2, x._2._3._3, x._2._3._4, x._2._3._5, x._2._4, i)),
                StructType(StructField("id", LongType) :: StructField("type", IntegerType) :: StructField("sbh", StringType)
                    :: StructField("community_id", LongType) :: StructField("bhsxssr", DoubleType) :: StructField("jxse", DoubleType)
                    :: StructField("xxse", DoubleType) :: StructField("ynse", DoubleType) :: StructField("sf", DoubleType) :: StructField("fpyl", IntegerType)
                    :: StructField("hybh", StringType) :: StructField("hyyjz", DoubleType) :: StructField("sfbdl", DoubleType)
                    :: StructField("fpbd", IntegerType) :: StructField("fpbdl", DoubleType) :: StructField("jxsebdl", DoubleType)
                    :: StructField("xxsebdl", DoubleType) :: StructField("jxbdzb", DoubleType) :: StructField("ynsebdl", DoubleType)
                    :: StructField("xsebdl", DoubleType) :: StructField("txxszb", DoubleType) :: StructField("sfyc", BooleanType)
                    :: StructField("sfbdyc", BooleanType) :: StructField("fpbdyc", BooleanType) :: StructField("jxbdyc", BooleanType)
                    :: StructField("txxsyc", BooleanType) :: StructField("ychz", IntegerType) :: StructField("month", IntegerType) :: Nil)).write.insertInto("tpin_zhibiao_wwd")
        }
        //    sc.stop()
    }

}
