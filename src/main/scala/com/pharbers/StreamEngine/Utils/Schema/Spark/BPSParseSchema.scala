package com.pharbers.StreamEngine.Utils.Schema.Spark

import scala.util.parsing.json.JSON
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/** 利用 Spark 解析各种来源的 Schema
 *
 * @author clock
 * @version 0.1
 * @since 2019/11/06 15:41
 */
object BPSParseSchema {
    private def parseMetadataByTxt(metadataPath: String)(implicit spark: SparkSession): Map[String, Any] = {
        spark.sparkContext
                .textFile(metadataPath)
                .collect()
                .map(_.toString())
                .map { row =>
                    if (row.startsWith("{")) {
                        val tmp = row.split(":")
                        tmp(0).tail.replace("\"", "") -> tmp(1).init.replace("\"", "")
                    } else if (row.startsWith("[")) {
                        "schema" -> JSON.parseFull(row).get
                    } else {
                        ???
                    }
                }.toMap
    }

    def parseByMetadata(metadataPath: String)(implicit spark: SparkSession): StructType = {
        val matadataMap = parseMetadataByTxt(metadataPath)

        val fields = matadataMap("schema").asInstanceOf[List[_]].map { x =>
            val tmp = x.asInstanceOf[Map[String, String]]
            tmp("type") match {
                case "String" => StructField(tmp("key"), StringType)
                case _ => ???
            }
        }.toArray

        new StructType(fields)
    }
}
