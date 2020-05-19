package dcs.test

import java.io.File
import java.util.Scanner
import com.pharbers.StreamEngine.Utils.Strategy.Session.Spark.BPSparkSession
import com.pharbers.StreamEngine.Utils.Log.PhLogable
import org.scalatest.FunSuite

/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数
  * @author dcs
  * @version 0.0
  * @since 2019/12/20 16:40
  * @note 一些值得注意的地方
  */
class BPStreamOverListener extends FunSuite with PhLogable{
    val spark = BPSparkSession(null)
    test("test pyjob ok"){
        val scan = new Scanner(new File("D:\\文件\\weixin\\WeChat Files\\dengcao1993\\FileStorage\\File\\2019-12\\data.csv"))
        while (scan.hasNextLine){
            val line = scan.nextLine().split(",")
            val jobId = line.head
            val length = line(1).toInt
            val path = line(2)
            trigger(path.replace("contents", "row_record"), path, length, jobId)
        }
    }

    test("find err"){
        val scan = new Scanner(new File("C:\\Users\\EDZ\\Desktop\\err"))
        while (scan.hasNextLine){
            val line = scan.nextLine()
            val path = "/user/alex/jobs/00cc201c-eba2-4a41-b72e-542387d5839f/" + line + "/err"
            val err = spark.sparkContext.textFile(path).take(1).head
            println(err)
        }
    }
    def trigger(rowRecordPath: String, path: String, length: Int, jobId: String): Unit = {
//        val rows = BPSHDFSFile.readHDFS(rowRecordPath).map(_.toLong).sum
//        val count = spark.read.csv(path).count()
//        logger.debug(s"row record path: $rowRecordPath")
//        logger.debug(s"rows: $rows")
//        logger.debug(s"count: $count")
//        logger.debug(s"length: $length")
//        if (count >= length) {
//            logger.info(s"启动py ok")
//        } else {
//            logger.error(s"jobId: $jobId, count: $count, length: $length, path: $path")
//        }
    }
}
