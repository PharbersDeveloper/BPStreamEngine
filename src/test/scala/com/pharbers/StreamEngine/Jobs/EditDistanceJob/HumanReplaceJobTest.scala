package com.pharbers.StreamEngine.Jobs.EditDistanceJob

import com.pharbers.StreamEngine.Utils.Component2.BPSComponentConfig
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数
  * @author dcs
  * @version 0.0
  * @since 2020/04/23 13:23
  * @note 一些值得注意的地方
  */
class HumanReplaceJobTest extends FunSuite{
    test("test createHumanReplaceDfV3"){
        val localSpark = SparkSession.builder().config(new SparkConf().setMaster("local[8]")).enableHiveSupport().getOrCreate()
        localSpark.sparkContext.setLogLevel("INFO")
        val job = HumanReplaceJob(BPSComponentConfig("", "", Nil, Map()))

        val df = localSpark.read.format("csv")
                .option("header", "true")
                .load("file:///D:\\文件\\excel数据表\\产品\\humanReplace\\CPA_no_replace 第8次提数0417.csv")
        df.show()
        job.createHumanReplaceDfV3(df)
        println("over")
        spark.close()
    }
}
