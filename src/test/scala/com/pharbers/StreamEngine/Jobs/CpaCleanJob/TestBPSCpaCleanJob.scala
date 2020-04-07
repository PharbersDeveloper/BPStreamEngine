//package com.pharbers.StreamEngine.Jobs.CpaCleanJob
//import com.pharbers.StreamEngine.Utils.Session.Spark.BPSparkSession
///** 功能描述
//  *
//  * @param args 构造参数
//  * @tparam T 构造泛型参数
//  * @author dcs
//  * @version 0.0
//  * @since 2020/03/19 14:05
//  * @note 一些值得注意的地方
//  */
//object TestBPSCpaCleanJob extends App {
//
//    val spark = BPSparkSession()
//    val job = BPSCpaCleanJob(null, spark, Map(
//        "jobId" -> "test",
//        "runId" -> "test",
//        "hospMapping" -> "/user/dcs/jassenClean/Hospital_Code_PHA_final_2.csv",
//        "marketMapping" -> "/user/dcs/jassenClean/Product_matching_table_packid_v2.csv",
//        "dataSets" -> ""
//    ))
//    job.open()
//    job.exec()
//}
//
////Janssen补数用
//object add extends App{
//    import org.apache.spark.sql.functions._
//    val spark = BPSparkSession()
//    val df = spark.read.format("csv")
//            .option("header", true)
//            .option("delimiter", ",")
//            .load("/user/dcs/jassenClean/jassen_add.csv")
//            .selectExpr(
//                "'Janssen' as COMPANY",
//                "'CPA&GYC' as SOURCE",
//                "province_name as PROVINCE_NAME",
//                "city_name as CITY_NAME",
//                "BI_Code as HOSP_CODE",
//                "hospital_name as HOSP_NAME",
//                "atc3_code as ATC",
//                "molecule_name as MOLE_NAME",
//                "product_name as PRODUCT_NAME",
//                "company_name as MANUFACTURER_NAME",
//                "pack_description as SPEC",
//                "formulation_name as DOSAGE",
//                "year_month as YEAR",
//                "year_month as QUARTER",
//                "year_month as MONTH",
//                "cast(regexp_replace(sales_value, ',', '') as double) as SALES_VALUE" ,
//                "total_units as SALES_QTY"
//            ).withColumn("PREFECTURE_NAME", lit(null))
//            .withColumn("HOSP_LEVEL", lit(null))
//            .withColumn("KEY_BRAND", lit(null))
//            .withColumn("PACK", lit(null))
//            .withColumn("PACK_QTY", lit(null))
//            .withColumn("DELIVERY_WAY", lit(null))
//            .withColumn("MKT", lit(null))
//            .withColumn("version", lit("0.0.9"))
//            .write.mode("append")
//            .option("path", "/common/public/cpa/0.0.9")
//            .saveAsTable("cpa")
//}