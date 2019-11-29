package com.pharbers.StreamEngine.Utils.Schema.Spark

import org.scalatest.FunSuite
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import com.pharbers.StreamEngine.Utils.Session.Spark.BPSparkSession

/** Parse Schema Test
 *
 * @author clock
 * @version 0.1
 * @since 2019/11/06 15:43
 */
class BPSParseSchemaTest extends FunSuite {
    val id = ""
    val matedataPath = "hdfs:///jobs/83ee0f2a-360a-4236-ba26-afa09d58e01d/ea293f1b-a66d-44fb-95ff-49a009840ed4/metadata"

    test("解析在 Parquet 中 Metadata 的 Schema") {
        implicit val spark: SparkSession = BPSparkSession()
        val metadata = BPSParseSchema.parseMetadata(matedataPath)
        assert(metadata.isDefinedAt("schema"))

        val st: StructType = BPSParseSchema.parseSchema(metadata("schema").asInstanceOf[List[_]])
        assert(st != null)
    }
}
