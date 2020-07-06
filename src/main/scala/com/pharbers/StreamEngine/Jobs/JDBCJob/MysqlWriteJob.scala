package com.pharbers.StreamEngine.Jobs.JDBCJob

import com.pharbers.StreamEngine.Utils.Component2.BPSConcertEntry
import com.pharbers.StreamEngine.Utils.Strategy.Session.Spark.BPSparkSession
import com.pharbers.StreamEngine.sql.spark

/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数
  * @author dcs
  * @version 0.0
  * @since 2020/07/03 15:28
  * @note 一些值得注意的地方
  */
object MysqlWriteJob extends App {
    if(args.length < 1) throw new Exception("need table name")
    val tableName = args.head
    val spark = BPSConcertEntry.queryComponentWithId("spark").get.asInstanceOf[BPSparkSession]
    val df = spark.sql(s"select * from $tableName")
    df.write.format("jdbc")
            .mode("overwrite")
            .option("url","jdbc:mysql://ph-dw-ins-cluster.cluster-cngk1jeurmnv.rds.cn-northwest-1.amazonaws.com.cn:3306")
            .option("driver","com.mysql.jdbc.Driver")
            .option("dbtable", s"data.$tableName")
            .option("user", "pharbers")
            .option("password", "Abcde196125")
            .option("useUnicode", "true")
            .option("characterEncoding", "utf8")
            .option("truncate", "true")
            .save()
}
