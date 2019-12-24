import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetInputFormat
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Duration, StreamingContext}

/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数
  * @author dcs
  * @version 0.0
  * @since 2019/11/27 11:27
  * @note 一些值得注意的地方
  */
object TestSpark extends App {
    val conf = new SparkConf().setMaster("yarn").setAppName("NetworkWordCount")
    val ssc: StreamingContext = new StreamingContext(conf, Duration(1000))
    ssc.sparkContext.hadoopConfiguration.set("parquet.read.support.class", "parquet.avro.AvroReadSupport")
    val stream = ssc.fileStream[Void, GenericRecord, ParquetInputFormat[GenericRecord]](
        "/user/alex/jobs/6cdb14ff-7897-4bd8-b9c5-7785c98935d1/45b95705-50fd-43bd-9fd2-29f1634f88b2/contents/a551d9fa-bb9b-43fd-aade-b60995611c8e", { path: Path => path.toString.endsWith("parquet") }, true, ssc.sparkContext.hadoopConfiguration)
    stream.foreachRDD(x => x.pipe(""))
}
