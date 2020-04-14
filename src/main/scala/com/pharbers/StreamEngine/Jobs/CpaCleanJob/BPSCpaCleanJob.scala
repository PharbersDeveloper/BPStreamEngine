package com.pharbers.StreamEngine.Jobs.CpaCleanJob

import java.util.{Collections, UUID}

import BPSCpaCleanJob._
import com.pharbers.StreamEngine.Utils.Component2
import com.pharbers.StreamEngine.Utils.Config.BPSConfig

import collection.JavaConverters._
import com.pharbers.StreamEngine.Utils.Job.{BPSJobContainer, BPStreamJob}
import com.pharbers.StreamEngine.Utils.Strategy.BPSDataMartBaseStrategy
import com.pharbers.kafka.schema.{AssetDataMart, DataSet}
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.{Importance, Type}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.bson.types.ObjectId

/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数
  * @author dcs
  * @version 0.0
  * @since 2020/01/02 15:10
  * @note 一些值得注意的地方
  */
case class BPSCpaCleanJob(jobContainer: BPSJobContainer, spark: SparkSession, config_map: Map[String, String]) extends BPStreamJob {

    import spark.implicits._

    override val componentProperty: Component2.BPComponentConfig = null
    override def createConfigDef(): ConfigDef = new ConfigDef()
            .define(HOSP_MAPPING_PATH_KEY, Type.STRING, UUID.randomUUID().toString, Importance.HIGH, HOSP_MAPPING_PATH_DOC)
            .define(MKT_MAPPING_PATH_KEY, Type.STRING, UUID.randomUUID().toString, Importance.HIGH, MKT_MAPPING_PATH_DOC)
            .define(PARENTS_CONFIG_KEY, Type.LIST,  Importance.HIGH, PARENTS_CONFIG_DOC)

    override type T = BPSDataMartBaseStrategy
    override val strategy: BPSDataMartBaseStrategy = new BPSDataMartBaseStrategy(config_map, configDef)
    private val jobConfig: BPSConfig = strategy.getJobConfig
    val jobId: String = strategy.getJobId
    val runId: String = strategy.getRunId
    override val id: String = jobId
    override val description: String = "cpa_clean_job"

    override def open(): Unit = {
        spark.read
                .format("csv")
                .option("header", "true")
                .option("delimiter", ",")
                .load(jobConfig.getString(HOSP_MAPPING_PATH_KEY))
                .select("`PHA.ID.x`", "BI_hospital_code")
                .groupBy("BI_hospital_code").agg(first("`PHA.ID.x`") as "PHA_ID_x")
                .createTempView("hosp_mapping")

        spark.read
                .format("csv")
                .option("header", "true")
                .option("delimiter", ",")
                .load(jobConfig.getString(MKT_MAPPING_PATH_KEY))
                .select("mkt", "molecule_name")
                .groupBy("molecule_name").agg(first("mkt") as "mkt")
                .createTempView("mkt_mapping")
    }

    override def exec(): Unit = {
        val janssen = spark.sql(joinMkt)
        val tableName = "CPA_Janssen"
        val tables = spark.sql("show tables").select("tableName").collect().map(x => x.getString(0))
        val version = if (tables.contains(tableName)) {
            val old = spark.sql(s"select version from $tableName limit 1").take(1).head.getString(0).split("\\.")
            s"${old.head}.${old(1)}.${old(2).toInt + 1}"
        } else {
            "0.0.1"
        }
        val url = s"/common/public/$tableName/$version"
        spark.sql(s"drop table $tableName")
        spark.sql("select *, '' as PHA_ID from cpa where company != 'Janssen'")
                .unionByName(janssen)
                .write
                .mode("overwrite")
                .option("path", url)
                .saveAsTable(tableName)
        strategy.pushDataSet(tableName, version, url, "overwrite")
    }

    override def close(): Unit = {
        super.close()
    }
}

object BPSCpaCleanJob {
    final val HOSP_MAPPING_PATH_KEY = "hospMapping"
    final val HOSP_MAPPING_PATH_DOC = "hosp mapping csv path"
    final val MKT_MAPPING_PATH_KEY = "marketMapping"
    final val MKT_MAPPING_PATH_DOC = "market mapping csv file path"
    final val PARENTS_CONFIG_KEY = "dataSets"
    final val PARENTS_CONFIG_DOC = "parent dataset id list"

    val filterCompanyAndCleanYearMonth: String =
        """SELECT
          | cast(substr(YEAR, 0, 4) as int) as YEAR,
          | cast(right(MONTH, 2) as int) as MONTH,
          | cast(cast(right(MONTH, 2) as int) / 3.1 + 1 as int) as QUARTER,
          | COMPANY,
          | SOURCE,
          | PROVINCE_NAME,
          | CITY_NAME,
          | PREFECTURE_NAME,
          | HOSP_NAME,
          | HOSP_CODE,
          | ATC,
          | MOLE_NAME,
          | KEY_BRAND,
          | PRODUCT_NAME,
          | PACK,
          | SPEC,
          | DOSAGE,
          | (case when reverse(split(reverse(SPEC), ' ')[0]) rlike '^\\d+$' then reverse(split(reverse(SPEC), ' ')[0]) else 1 end) as PACK_QTY,
          | SALES_QTY,
          | SALES_VALUE,
          | DELIVERY_WAY,
          | MANUFACTURER_NAME,
          | version
          | FROM cpa
          | WHERE company = 'Janssen'
        """.stripMargin

    val joinHospMapping: String =
        s"""
           |SELECT * FROM
           | ($filterCompanyAndCleanYearMonth) cpa_janssen
           | LEFT JOIN hosp_mapping ON
           | cpa_janssen.HOSP_CODE = hosp_mapping.BI_hospital_code
        """.stripMargin

    val joinHosp: String =
        s"""
           | SELECT * FROM
           | ($joinHospMapping) cpa_hosp_mapping
           | LEFT JOIN
           | (select PHA_ID, first(HOSP_NAME) as NEW_HOSP_NAME, first(HOSP_LEVEL) as HOSP_LEVEL from hosp group by PHA_ID) hosp_join
           | ON
           | cpa_hosp_mapping.PHA_ID_x = hosp_join.PHA_ID
        """.stripMargin

    val joinMkt: String =
        s"""
           | SELECT
           | YEAR,
           | MONTH,
           | QUARTER,
           | COMPANY,
           | SOURCE,
           | PROVINCE_NAME,
           | CITY_NAME,
           | PREFECTURE_NAME,
           | PHA_ID,
           | NEW_HOSP_NAME as HOSP_NAME,
           | HOSP_CODE,
           | HOSP_LEVEL,
           | ATC,
           | MOLE_NAME,
           | KEY_BRAND,
           | cpa_hosp.PRODUCT_NAME as PRODUCT_NAME,
           | PACK,
           | SPEC,
           | DOSAGE,
           | PACK_QTY,
           | SALES_QTY,
           | SALES_VALUE,
           | DELIVERY_WAY,
           | MANUFACTURER_NAME,
           | mkt as MKT,
           | version
           | FROM
           | ($joinHosp) cpa_hosp
           | LEFT JOIN
           | mkt_mapping
           | on
           | cpa_hosp.MOLE_NAME = mkt_mapping.molecule_name
         """.stripMargin
}
