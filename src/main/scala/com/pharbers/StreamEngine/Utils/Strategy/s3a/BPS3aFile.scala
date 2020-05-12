package com.pharbers.StreamEngine.Utils.Strategy.s3a

import java.io.{BufferedReader, ByteArrayInputStream, InputStreamReader}
import java.util.UUID
import java.util.function.Consumer

import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.s3.AmazonS3Client
import com.pharbers.StreamEngine.Utils.Annotation.Component
import com.pharbers.StreamEngine.Utils.Component2
import com.pharbers.StreamEngine.Utils.Strategy.BPStrategyComponent
import org.apache.kafka.common.config.ConfigDef
import com.amazonaws.regions.Region
import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.services.s3.transfer.TransferManager

import collection.JavaConverters._



/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数
  * @author dcs
  * @version 0.0
  * @since 2020/05/12 10:26
  * @note 一些值得注意的地方
  */

@Component(name = "BPS3aFile", `type` = "BPS3aFile")
case class BPS3aFile(override val componentProperty: Component2.BPComponentConfig)
        extends BPStrategyComponent {
    override val strategyName: String = "BPS3aFile"

    override def createConfigDef(): ConfigDef = new ConfigDef

    private val s3: AmazonS3Client = new AmazonS3Client(new BasicAWSCredentials(sys.props("S3_ACCESS_KEY"), sys.props("S3_SECRET_KEY")))
    s3.setRegion(new Region("cn-northwest-1", "amazonaws.com"))
    private val transfer = new TransferManager(s3)
    
    def checkPath(path: String): Boolean = {
        val (bucketName, prefix) = getBucketNameAndPrefix(path)
        s3.listObjects(bucketName, prefix).getMaxKeys > 0
    }

    def appendLine(path: String, line: String): Unit ={
        val (bucketName, prefix) = getBucketNameAndPrefix(path)
        val inputStream =  new ByteArrayInputStream(line.getBytes)
        s3.putObject(bucketName, s"$prefix/${UUID.randomUUID().toString}", inputStream, new ObjectMetadata())
    }

    def readS3A(path: String): List[String] = {
        val (bucketName, prefix) = getBucketNameAndPrefix(path)
        if(!checkPath(path)) return Nil
        s3.listObjects(bucketName, prefix).getObjectSummaries.asScala.flatMap(x => {
            val stream = s3.getObject(bucketName, x.getKey).getObjectContent
            new BufferedReader(new InputStreamReader(stream)).lines().iterator().asScala
        }).toList
    }

    private def getBucketNameAndPrefix(path: String): (String, String) = {
        val pathSplit = path.split("/").filter(x => x != "s3a:")
        val bucketName = pathSplit.head
        val prefix = pathSplit.tail.mkString("")
        (bucketName, prefix)
    }

}
