package com.pharbers.StreamEngine.Utils.Strategy.s3a

import java.io.{BufferedReader, ByteArrayInputStream, File, InputStream, InputStreamReader}
import java.util.UUID

import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.pharbers.StreamEngine.Utils.Annotation.Component
import com.pharbers.StreamEngine.Utils.Component2
import com.pharbers.StreamEngine.Utils.Strategy.BPStrategyComponent
import org.apache.kafka.common.config.ConfigDef
import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.services.s3.transfer.{TransferManager, TransferManagerBuilder}
import org.apache.hadoop.fs.s3a.BasicAWSCredentialsProvider

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

    private val s3: AmazonS3 = AmazonS3ClientBuilder
            .standard()
            .withCredentials(new BasicAWSCredentialsProvider(sys.env("S3_ACCESS_KEY"), sys.env("S3_SECRET_KEY")))
            .withRegion("cn-northwest-1")
            .build()

    private val transferManager: TransferManager = TransferManagerBuilder.standard()
            .withS3Client(s3)
            .build()

    def checkPath(path: String): Boolean = {
        val (bucketName, prefix) = getBucketNameAndPrefix(path)
        s3.listObjects(bucketName, prefix).getObjectSummaries.size() > 0
    }

    def appendLine(path: String, line: String): Unit ={
        val (bucketName, prefix) = getBucketNameAndPrefix(path)
        val inputStream =  new ByteArrayInputStream(line.getBytes)
        s3.putObject(bucketName, s"$prefix/${UUID.randomUUID().toString}", inputStream, new ObjectMetadata())
        inputStream.close()
    }
	
    def copyHDFSFiles(path: String, fileName: String, file: InputStream): Unit = {
        val (bucketName, prefix) = getBucketNameAndPrefix(path)
        s3.putObject(bucketName, s"$prefix/$fileName", file, new ObjectMetadata())
        file.close()
    }

    def readFiles(path: String): List[String] = {
        val (bucketName, prefix) = getBucketNameAndPrefix(path)
        if(!checkPath(path)) return Nil
        s3.listObjects(bucketName, prefix).getObjectSummaries.asScala.flatMap(x => {
            val stream = s3.getObject(bucketName, x.getKey).getObjectContent
            new BufferedReader(new InputStreamReader(stream)).lines().iterator().asScala
        }).toList
    }

    def getLowAipClient: AmazonS3 = {
        s3
    }

    def getHighAipClient: TransferManager = {
        transferManager
    }

    private def getBucketNameAndPrefix(path: String): (String, String) = {
        val pathSplit = path.split("/").filter(x => x != "s3a:" && x != "")
        val bucketName = pathSplit.head
        val prefix = pathSplit.tail.mkString("/")
        (bucketName, prefix)
    }

}
