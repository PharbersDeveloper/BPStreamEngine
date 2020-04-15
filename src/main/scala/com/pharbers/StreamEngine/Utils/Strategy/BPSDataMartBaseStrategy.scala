package com.pharbers.StreamEngine.Utils.Strategy

import java.util.Collections

import com.pharbers.StreamEngine.Utils.Component2.{BPComponentConfig, BPSConcertEntry}
import com.pharbers.StreamEngine.Utils.Strategy.Session.Spark.BPSparkSession
import com.pharbers.StreamEngine.Utils.Strategy.Blood.BPSSetBloodStrategy
import com.pharbers.kafka.schema.{AssetDataMart, DataSet}
import org.apache.kafka.common.config.ConfigDef
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
class BPSDataMartBaseStrategy(override val componentProperty: BPComponentConfig) extends BPStrategyComponent {

    def pushDataSet(tableName: String, version: String, url: String, saveMode: String, jobId: String, traceId: String, dataSet: List[String]): Unit ={
        val bloodStrategy: BPSSetBloodStrategy = new BPSSetBloodStrategy(componentProperty.config)
        val spark = BPSConcertEntry.queryComponentWithId("spark").get.asInstanceOf[BPSparkSession]
        val mongoOId = new ObjectId().toString
        val dfs = new DataSet(
            List[CharSequence](dataSet: _*).asJava,
            mongoOId,
            jobId,
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
        bloodStrategy.pushBloodInfo(dataMartValue, jobId, traceId, msgTyp = "AssetDataMart")
    }

    override val strategyName: String = "data mark blood strategy"

    override def createConfigDef(): ConfigDef = new ConfigDef()
}

object BPSDataMartBaseStrategy {
    def apply(componentProperty: BPComponentConfig): BPSDataMartBaseStrategy = new BPSDataMartBaseStrategy(componentProperty)
}
