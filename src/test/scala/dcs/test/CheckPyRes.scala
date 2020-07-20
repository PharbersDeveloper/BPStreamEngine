package dcs.test

import java.util

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.{ObjectListing, S3ObjectSummary}
import com.pharbers.StreamEngine.Utils.Component2.BPSConcertEntry
import com.pharbers.StreamEngine.Utils.Strategy.Session.Spark.BPSparkSession
import com.pharbers.StreamEngine.Utils.Strategy.s3a.BPS3aFile
import org.scalatest.FunSuite
import org.apache.spark.sql.functions.first

import collection.JavaConverters._
import scala.collection.mutable

/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数
  * @author dcs
  * @version 0.0
  * @since 2020/06/03 10:01
  * @note 一些值得注意的地方
  */
class CheckPyRes extends FunSuite{
    val s3aFile: BPS3aFile = BPSConcertEntry.queryComponentWithId("s3a").get.asInstanceOf[BPS3aFile]
    val s3Client: AmazonS3 = s3aFile.getLowAipClient
    val bucketName = "ph-stream"
    val prefix = "jobs/runId_5f11296bf3629815707020a3/BPSPyCleanJob/"
    var objectListing: ObjectListing = s3Client.listObjects(bucketName, prefix)
    val files: mutable.Buffer[S3ObjectSummary] = objectListing.getObjectSummaries.asScala
    while (objectListing.isTruncated){
        objectListing = s3Client.listNextBatchOfObjects(objectListing)
        files ++= objectListing.getObjectSummaries.asScala
    }
    val filesIterator: Iterator[S3ObjectSummary] = files.iterator

    test("find error"){
        var keys: List[String] = List()
        var size = 0
        while (filesIterator.hasNext){
            val file = filesIterator.next()
            val key = file.getKey
            if(key.contains("err") && !key.contains("_SUCCESS")) {
                if(file.getSize > 0) keys = keys :+ key
            }
            size += 1
        }
        keys.map(x => x.split("/")(3).replace("jobId_", "")).distinct.foreach(println)
        println(size)
    }

    test("res df"){
        var paths: List[String] = Nil
        while (filesIterator.hasNext){
            val file = filesIterator.next()
            val key = file.getKey
            if(key.contains("contents") && !key.contains("_SUCCESS") && file.getSize > 0) {
                paths = paths :+ s"s3a://$bucketName/" + prefix + key.split("/").drop(3).mkString("/")
            } else {
                Nil
            }
        }
        val spark = BPSConcertEntry.queryComponentWithId("spark").get.asInstanceOf[BPSparkSession]
        val df = spark.read.json(paths: _*)
    }

}

