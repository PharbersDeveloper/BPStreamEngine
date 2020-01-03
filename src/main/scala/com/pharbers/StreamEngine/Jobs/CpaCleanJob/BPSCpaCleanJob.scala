package com.pharbers.StreamEngine.Jobs.CpaCleanJob

import java.util.UUID
import BPSCpaCleanJob._
import com.pharbers.StreamEngine.Utils.Config.BPSConfig
import com.pharbers.StreamEngine.Utils.StreamJob.JobStrategy.BPSJobStrategy
import com.pharbers.StreamEngine.Utils.StreamJob.{BPSJobContainer, BPStreamJob}
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.{Importance, Type}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数
  * @author dcs
  * @version 0.0
  * @since 2020/01/02 15:10
  * @note 一些值得注意的地方
  */
case class BPSCpaCleanJob(jobContainer: BPSJobContainer, spark: SparkSession, config: Map[String, String]) extends BPStreamJob {

    import spark.implicits._

    override type T = BPSJobStrategy
    override val strategy: BPSJobStrategy = null
    private val jobConfig: BPSConfig = BPSConfig(configDef, config)
    val jobId: String = jobConfig.getString(JOB_ID_CONFIG_KEY)
    val runId: String = jobConfig.getString(RUN_ID_CONFIG_KEY)
    override val id: String = jobId

    override def open(): Unit = {
        spark.read
                .format("csv")
                .option("header", true)
                .option("delimiter", ",")
                .load(jobConfig.getString(HOSP_MAPPING_PATH_KEY))
                .select("`PHA.ID.x`", "BI_hospital_name_cn")
                .groupBy("BI_hospital_name_cn").agg(first("`PHA.ID.x`") as "PHA_ID_x")
                .createTempView("hosp_mapping")

        spark.read
                .format("csv")
                .option("header", true)
                .option("delimiter", ",")
                .load(jobConfig.getString(MKT_MAPPING_PATH_KEY))
                .select("mkt", "molecule_name")
                .groupBy("molecule_name").agg(first("mkt") as "mkt")
                .createTempView("mkt_mapping")
    }

    override def exec(): Unit = {
        val janssen = spark.sql(joinMkt)
        spark.sql("select *, '' as PHA_ID from cpa where company != 'Janssen'")
                .unionByName(janssen)
                .write
                .mode("append")
                .option("path", s"/common/public/CPA_Janssen/0.0.4")
                .saveAsTable("CPA_Janssen")
    }

    override def close(): Unit = {
        super.close()
    }
}

object BPSCpaCleanJob {
    final val JOB_ID_CONFIG_KEY = "jobId"
    final val JOB_ID_CONFIG_DOC = "job id"
    final val RUN_ID_CONFIG_KEY = "runId"
    final val RUN_ID_CONFIG_DOC = "run id"
    val CPA_VERSION_KEY = "version"
    final val CPA_VERSION_DOC = "save cpa version"
    val HOSP_MAPPING_PATH_KEY = "hospMapping"
    final val HOSP_MAPPING_PATH_DOC = "hosp mapping csv path"
    val MKT_MAPPING_PATH_KEY = "marketMapping"
    final val MKT_MAPPING_PATH_DOC = "market mapping csv file path"
    val configDef: ConfigDef = new ConfigDef()
            .define(JOB_ID_CONFIG_KEY, Type.STRING, UUID.randomUUID().toString, Importance.HIGH, JOB_ID_CONFIG_DOC)
            .define(RUN_ID_CONFIG_KEY, Type.STRING, UUID.randomUUID().toString, Importance.HIGH, RUN_ID_CONFIG_DOC)
            .define(CPA_VERSION_KEY, Type.STRING, UUID.randomUUID().toString, Importance.HIGH, CPA_VERSION_DOC)
            .define(HOSP_MAPPING_PATH_KEY, Type.STRING, UUID.randomUUID().toString, Importance.HIGH, HOSP_MAPPING_PATH_DOC)
            .define(MKT_MAPPING_PATH_KEY, Type.STRING, UUID.randomUUID().toString, Importance.HIGH, MKT_MAPPING_PATH_DOC)

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
           | cpa_janssen.HOSP_NAME = hosp_mapping.BI_hospital_name_cn
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

object test extends App {

    import com.pharbers.StreamEngine.Utils.Session.Spark.BPSparkSession

    val spark = BPSparkSession()
    val job = BPSCpaCleanJob(null, spark, Map(
        "jobId" -> "test",
        "runId" -> "test",
        "version" -> "0",
        "hospMapping" -> "/user/dcs/jassenClean/Hospital_Code_PHA_final_2.csv",
        "marketMapping" -> "/user/dcs/jassenClean/Product_matching_table_packid_v2.csv"
    ))
    job.open()
    job.exec()
}