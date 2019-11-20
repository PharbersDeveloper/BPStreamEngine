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

    /** 从 hdfs 的 txt 中，解析出 Map 格式的 metadata
     *
     * @param metadataPath 元数据在 HDFS 的位置
     * @param spark SparkSession 实例
     * @return _root_.org.apache.spark.sql.SparkSession => Map[String, Any]
     * @author clock
     * @version 0.1
     * @since 2019/11/6 18:34
     * @example {{{BPSParseSchema.parseMetadataByTxt()()}}}
     */
    def parseMetadata(metadataPath: String)(implicit spark: SparkSession): Map[String, Any] = {
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

    /** 动态的生成 StructType
     *
     * @param lst List 形式的 Schema
     * @return List[Any] => _root_.org.apache.spark.sql.types.StructType
     * @author clock
     * @version 0.1
     * @since 2019/11/6 17:34
     * @example {{{BPSParseSchema.parseByMetadata()()}}}
     */
    def parseSchema(lst: List[Any]): StructType = {
        val fields = lst.map { x =>
            val tmp = x.asInstanceOf[Map[String, String]]
            tmp("type") match {
                case "String" => StructField(tmp("key"), StringType)
                case _ => ???
            }
        }.toArray

        new StructType(fields)
    }

    /** 从 hdfs 的 txt 中，解析出 Map 格式的 metadata，然后在根据 key = schema的数据结构中，动态的生成 StructType
     *
     * @param metadataPath 元数据在 HDFS 的位置
     * @param spark SparkSession 实例
     * @return _root_.org.apache.spark.sql.SparkSession => _root_.org.apache.spark.sql.types.StructType
     * @author clock
     * @version 0.1
     * @since 2019/11/6 17:34
     * @example {{{BPSParseSchema.parseByMetadata()()}}}
     */
    def parseSchemaByMetadata(metadataPath: String)(implicit spark: SparkSession): StructType = {
        val matadataMap = parseMetadata(metadataPath)
        parseSchema(matadataMap("schema").asInstanceOf[List[_]])
    }
}
