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
    val id = "57fe0-2bda-4880-8301-dc55a0"
    val matedataPath = "hdfs:///test/alex/07b8411a-5064-4271-bfd3-73079f2b42b2/metadata/"

    test("解析在 Parquet 中 Metadata 的 Schema") {
        implicit val spark: SparkSession = BPSparkSession()

        val st: StructType = BPSParseSchema.parseByMetadata(matedataPath + id)
        assert(st != null)
        st.foreach(x => println(x))
    }
}
