package com.pharbers.StreamEngine.Jobs.PyJob.Py4jServer

import py4j.GatewayServer
import java.io.BufferedWriter
import org.json4s.DefaultFormats
import scala.util.parsing.json.JSON
import org.json4s.jackson.Serialization.write

/** 实现 Py4j 的 GatewayServer 的实例
 *
 * @author clock
 * @version 0.0
 * @since 2019/11/14 19:04
 * @note 一些值得注意的地方
 */
case class BPSPy4jServer(var isFirst: Boolean,
                         var csvTitle: List[String])
                        (successBufferedWriter: Option[BufferedWriter],
                         errBufferedWriter: Option[BufferedWriter],
                         metadataBufferedWriter: Option[BufferedWriter]) extends Serializable {

    def startServer(): Unit = {
        val server = new GatewayServer(this)
        server.start(false)
    }

    def map2csv(title: List[String], m: Map[String, Any]): List[Any] = title.map(m)

    def writeHdfs(str: String): Unit = {
        errBufferedWriter.get.write(str)
//        JSON.parseFull(str) match {
//            case Some(result: Map[String, AnyRef]) =>
//                if (result("tag").asInstanceOf[Double] == 1) {
//                    if (isFirst) {
//                        val metadata = result("metadata").asInstanceOf[Map[String, Any]]
//                        csvTitle = metadata("schema").asInstanceOf[List[Any]].map { x =>
//                            x.asInstanceOf[Map[String, String]]("key").toString
//                        }
//
//                        successBufferedWriter.get.write(csvTitle.mkString(","))
//                        successBufferedWriter.get.newLine()
//
//                        metadataBufferedWriter.get.write(write(metadata)(DefaultFormats))
//                        isFirst = false
//                    }
//
//                    successBufferedWriter.get.write(
//                        map2csv(csvTitle, result("data").asInstanceOf[Map[String, Any]]).mkString(",")
//                    )
//                    successBufferedWriter.get.write("\n")
//                } else {
//                    errBufferedWriter.get.write(str)
//                    errBufferedWriter.get.write("\n")
//                }
//            case None =>
//                errBufferedWriter.get.write(str)
//                errBufferedWriter.get.write("\n")
//        }
    }
}
