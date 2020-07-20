package com.pharbers.StreamEngine.Utils.Strategy

import com.pharbers.StreamEngine.Utils.Component2.{BPComponentConfig, BPSConcertEntry}
import com.pharbers.StreamEngine.Utils.Job.Status.BPSJobStatus
import com.pharbers.StreamEngine.Utils.Module.bloodModules.{AssetDataMartModel, BloodModel}
import com.pharbers.StreamEngine.Utils.Strategy.Session.Spark.BPSparkSession
import com.pharbers.StreamEngine.Utils.Strategy.Blood.BPSSetBloodStrategy
import org.apache.kafka.common.config.ConfigDef
import org.bson.types.ObjectId


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
//        val dfs = new DataSet(
//            List[CharSequence](dataSet: _*).asJava,
//            mongoOId,
//            jobId,
//            Collections.emptyList(),
//            "",
//            spark.sql(s"select * from $tableName").count(),
//            url,
//            "hive table")
        // TODO @老邓  现在血缘记录开始和结束，以MongoDBID做为更新条件，所以要在你这Job开始和结束调用这个Func
        val dfs = BloodModel(
            mongoOId,
            "",
            List[String](dataSet: _*),
            jobId,
            Nil,
            "",
            spark.sql(s"select * from $tableName").count(),
            url,
            "hive table", BPSJobStatus.End.toString)
        
        bloodStrategy.pushBloodInfo(dfs, "", "")

//        val dataMartValue = new AssetDataMart(
//            tableName,
//            "",
//            version,
//            "mart",
//            List[CharSequence]("*").asJava,
//            List[CharSequence]("*").asJava,
//            List[CharSequence]("*").asJava,
//            List[CharSequence]("*").asJava,
//            List[CharSequence]("*").asJava,
//            List[CharSequence]("*").asJava,
//            List[CharSequence](mongoOId).asJava,
//            tableName,
//            s"/common/public/$tableName/$version",
//            "hive",
//            saveMode
//        )
       val dataMartValue = AssetDataMartModel(tableName,
            "",
            version,
            "mart",
            "*" :: Nil,
            "*" :: Nil,
            "*" :: Nil,
            "*" :: Nil,
            "*" :: Nil,
            "*" :: Nil,
            mongoOId :: Nil,
            tableName,
            url,
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
