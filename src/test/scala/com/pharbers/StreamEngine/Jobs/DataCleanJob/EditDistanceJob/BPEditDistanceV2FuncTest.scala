package com.pharbers.StreamEngine.Jobs.DataCleanJob.EditDistanceJob

import java.io.File
import java.util.Scanner

import com.pharbers.StreamEngine.Utils.Component2
import com.pharbers.StreamEngine.Utils.Component2.{BPSComponentConfig, BPSConcertEntry}
import com.pharbers.StreamEngine.Utils.Job.BPSJobContainer
import com.pharbers.StreamEngine.Utils.Strategy.JobStrategy.BPSCommonJobStrategy
import com.pharbers.StreamEngine.Utils.Strategy.Session.Spark.BPSparkSession
import org.ansj.library.{AmbiguityLibrary, DicLibrary}
import org.ansj.recognition.impl.UserDicNatureRecognition
import org.apache.kafka.common.config.ConfigDef
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, Word2Vec}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.nlpcn.commons.lang.tire.domain.Value

import collection.JavaConverters._
import org.scalatest.FunSuite

/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数
  * @author dcs
  * @version 0.0
  * @since 2020/07/13 15:24
  * @note 一些值得注意的地方
  */
class BPEditDistanceV2FuncTest extends FunSuite{
    test("test getCheckRes for spec"){
        val file = new File("src/test/resources/test_spec.csv")
        val scanner = new Scanner(file)
        val col = "SPEC"
        var count = 0
        var errCount = 0
        while (scanner.hasNextLine){
            val line = scanner.nextLine().split(",")
            val check =BPEditDistanceV2Func.getCheckRes(line.head, line.last, col)(2)
            if(check != "0"){
                errCount += 1
                println(line.mkString(","))
            }
            count += 1
        }
        println(errCount)
        assert(count * 0.03 > errCount)
    }

    test("test getCheckRes for MANUFACTURER_NAME"){
        val file = new File("src/test/resources/pfizer.csv")
        val scanner = new Scanner(file)
        val col = "MANUFACTURER_NAME"
        var count = 0
        while (scanner.hasNextLine){
            val line = scanner.nextLine().split(",")
            val check = BPEditDistanceV2Func.getCheckRes(line.head, line.last, col)(2)
            if(check.toInt > line.head.length / 5 + 1){
                count += 1
                println(line.mkString(",") + s",$check")
            }
        }
        println(count)
    }

    test("test jieba"){
        import com.huaban.analysis.jieba.JiebaSegmenter
        val segment = new JiebaSegmenter
        val file = new File("src/test/resources/test_manufacturer.csv")
        val scanner = new Scanner(file)
        val col = "MANUFACTURER_NAME"
        var count = 0
        while (scanner.hasNextLine){
            val line = scanner.nextLine().split(",")
            val check = segment.sentenceProcess(line.head).asScala.intersect(segment.sentenceProcess(line(1)).asScala).length
            println(line.mkString(",") + s",$check")
        }
    }

    test("test one col"){
        val col = "MANUFACTURER_NAME"
        println(BPEditDistanceV2Func.getCheckRes("湖南恒生制药有限公司", "湖南恒生制药股份有限公司", col)(2))
    }

    test("mnf"){
        val spark = BPSConcertEntry.queryComponentWithId("spark").get.asInstanceOf[BPSparkSession]
        import spark.spark.implicits._
        val sentenceUDF = udf((s: String) => Funcs.sentence(s))
        val mnf = spark.read.option("header", "true").csv("file:///D:\\code\\pharbers\\BPStream\\BPStreamEngine\\src\\test\\resources\\manufacturer.csv")
                .withColumn("mnf", sentenceUDF($"mnf"))

        val word2Vec = new Word2Vec()
                .setInputCol("mnf")
                .setOutputCol("features")
                .setVectorSize(6)
                .setMinCount(1)

        val labelIndexer = new StringIndexer()
                .setInputCol("mnf_ch")
                .setOutputCol("indexedLabel")
                .fit(mnf)
        val layers = Array[Int](6,5,2571)
        val mlpc = new MultilayerPerceptronClassifier()
                .setLayers(layers)
                .setBlockSize(512)
                .setSeed(1234L)
                .setMaxIter(128)
                .setFeaturesCol("features")
                .setLabelCol("indexedLabel")
                .setPredictionCol("prediction")

        val labelConverter = new IndexToString()
                .setInputCol("prediction")
                .setOutputCol("predictedLabel")
                .setLabels(labelIndexer.labels)

        val Array(trainingData, testData) = mnf.randomSplit(Array(0.8, 0.2))

        val pipeline = new Pipeline().setStages(Array(labelIndexer,word2Vec,mlpc,labelConverter))
        val model = pipeline.fit(mnf)

        val predictionResultDF = model.transform(testData)

        predictionResultDF.show(false)
        model.transform(trainingData).show(false)


//        val evaluator = new MulticlassClassificationEvaluator()
//                .setLabelCol("indexedLabel")
//                .setPredictionCol("prediction")
//                .setMetricName("precision")
//        val predictionAccuracy = evaluator.evaluate(predictionResultDF)
//        println("Testing Accuracy is %2.4f".format(predictionAccuracy * 100) + "%")

    }

    test("analysis"){
        import org.ansj.splitWord.analysis._
        import org.ansj.recognition.impl.StopRecognition
        import org.ansj.recognition.impl.NatureRecognition
        import com.huaban.analysis.jieba.JiebaSegmenter
        import collection.JavaConverters._
        import org.nlpcn.commons.lang.tire.library.Library

        val value = new Value("华东", "华东", "2")
        Library.insertWord(DicLibrary.get(), value)
        val filter = new StopRecognition()
        filter.insertStopNatures("ns")
        val s = "杭州中美华东制药有限公司"
        val str = ToAnalysis.parse(s)
        println(str.toString)
        val segment = new JiebaSegmenter
        println(new NatureRecognition().recognition(segment.sentenceProcess(s)).asScala
                .filter(x => x.getNatureStr != "ns").map(x => x.getName))
    }



}

object RunBPEditDistanceV2 extends App{
     val id = "0622"
     val config = Map("jobId" -> id, "dataSets" -> "", "tableName" -> "pfizer_check")
     val jobContainer = new BPSJobContainer() {
         override type T = BPSCommonJobStrategy
         override val strategy: BPSCommonJobStrategy = null
         override val id: String = ""
         override val description: String = ""
         override val spark: SparkSession = spark
         override val componentProperty: Component2.BPComponentConfig = null

         override def createConfigDef(): ConfigDef = new ConfigDef()
     }
     jobContainer.inputStream = Some(spark.sql("select * from pfizer_check").drop("id"))
     val bPEditDistance = new BPEditDistanceV2Func(jobContainer, BPSComponentConfig(id, "", Nil, config))
     bPEditDistance.open()
     bPEditDistance.exec()
     spark.spark.stop()
}

object Funcs{
    def sentence(s: String): Seq[String] = {
        import com.huaban.analysis.jieba.JiebaSegmenter
        val segment = new JiebaSegmenter
        segment.sentenceProcess(s).asScala.toList
    }
}
