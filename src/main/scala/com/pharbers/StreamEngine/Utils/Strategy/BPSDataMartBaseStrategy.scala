package com.pharbers.StreamEngine.Utils.Strategy

import java.util.Collections

//import com.pharbers.StreamEngine.Jobs.CpaCleanJob.BPSCpaCleanJob.PARENTS_CONFIG_KEY
import com.pharbers.StreamEngine.Utils.Strategy.Blood.BPSSetBloodStrategy
import com.pharbers.StreamEngine.Utils.Strategy.JobStrategy.BPSCommonJobStrategy
import com.pharbers.kafka.schema.{AssetDataMart, DataSet}
import org.apache.kafka.common.config.ConfigDef
import org.apache.spark.sql.SparkSession
import org.bson.types.ObjectId

import collection.JavaConverters._

/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数
  * @author dcs
  * @version 0.0
  * @since 2020/03/19 15:36
  * @note 一些值得注意的地方
  */
class BPSDataMartBaseStrategy(config: Map[String, String], @transient inoutConfigDef: ConfigDef = new ConfigDef()) {
    final val PARENTS_CONFIG_KEY = "dataSets"
    val bloodStrategy: BPSSetBloodStrategy = new BPSSetBloodStrategy(config)
    
    val strategy: BPSCommonJobStrategy = BPSCommonJobStrategy(config, inoutConfigDef)
    
    def pushDataSet(tableName: String, version: String, url: String, saveMode: String): Unit ={
        val spark = SparkSession.getActiveSession.getOrElse(throw new Exception("需要先初始化spark"))
        val mongoOId = new ObjectId().toString
        val dfs = new DataSet(
            List[CharSequence](strategy.jobConfig.getList(PARENTS_CONFIG_KEY).asScala: _*).asJava,
            mongoOId,
            strategy.getJobId,
            Collections.emptyList(),
            "",
            spark.sql(s"select * from $tableName").count(),
            url,
            "hive table")
        bloodStrategy.pushBloodInfo(dfs, "", "")

        val dataMartValue = new AssetDataMart(
            tableName,
            "",
            version,
            "mart",
            List[CharSequence]("*").asJava,
            List[CharSequence]("*").asJava,
            List[CharSequence]("*").asJava,
            List[CharSequence]("*").asJava,
            List[CharSequence]("*").asJava,
            List[CharSequence]("*").asJava,
            List[CharSequence](mongoOId).asJava,
            tableName,
            s"/common/public/$tableName/$version",
            "hive",
            saveMode
        )
        bloodStrategy.pushBloodInfo(dataMartValue, "", "", msgTyp = "AssetDataMart")
    }
}
