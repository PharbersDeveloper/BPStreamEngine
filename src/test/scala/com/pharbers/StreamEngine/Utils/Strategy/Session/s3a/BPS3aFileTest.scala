package com.pharbers.StreamEngine.Utils.Strategy.Session.s3a

import java.io.{BufferedReader, InputStreamReader}

import com.pharbers.StreamEngine.Utils.Component2.BPSConcertEntry
import com.pharbers.StreamEngine.Utils.Strategy.s3a.BPS3aFile
import org.scalatest.FunSuite

/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数
  * @author dcs
  * @version 0.0
  * @since 2020/05/12 15:11
  * @note 一些值得注意的地方
  */
class BPS3aFileTest extends FunSuite{
    val s3aFile: BPS3aFile = BPSConcertEntry.queryComponentWithId("s3a").get.asInstanceOf[BPS3aFile]
    test("test checkPath"){
        assert(s3aFile.checkPath("s3a://ph-stream/test"))
        assert(!s3aFile.checkPath("s3a://ph-stream/test/2234"))
    }

    test("test appendLine"){
        s3aFile.appendLine("s3a://ph-stream/test/appendLine", "test")
        assert(s3aFile.checkPath("s3a://ph-stream/test/appendLine"))
        val s3 = s3aFile.getLowAipClient
        val stream = s3.getObject("ph-stream", s3.listObjects("ph-stream", "test/appendLine").getObjectSummaries.get(0).getKey).getObjectContent
        assert(new BufferedReader(new InputStreamReader(stream)).readLine() == "test")
        s3.deleteObject("ph-stream", s3.listObjects("ph-stream", "test/appendLine").getObjectSummaries.get(0).getKey)
    }
}
