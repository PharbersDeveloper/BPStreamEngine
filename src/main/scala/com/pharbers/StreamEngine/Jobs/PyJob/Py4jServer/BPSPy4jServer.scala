package com.pharbers.StreamEngine.Jobs.PyJob.Py4jServer

import py4j.GatewayServer
import java.io.BufferedWriter

import com.pharbers.util.log.PhLogable
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
//object ts extends Serializable {
//    var isStarted = false
//    var server: GatewayServer = null
//}

object BPSPy4jServer extends Serializable {

    var csvTitle: List[String] = Nil
    var successBufferedWriter: Option[BufferedWriter] = None
    var errBufferedWriter: Option[BufferedWriter] = None
    var metadataBufferedWriter: Option[BufferedWriter] = None
    val lock = new Object
    var data: List[String] = Nil

    var server: GatewayServer = _

    def startServer(
                       csvTitle: List[String],
                       sw: Option[BufferedWriter],
                       ew: Option[BufferedWriter],
                       mw: Option[BufferedWriter]): Unit = {
        if (server == null) {
            successBufferedWriter = sw
            errBufferedWriter = ew
            metadataBufferedWriter = mw
            server = new GatewayServer(this)
            server.start(true)
        }
    }

    def closeServer(): Unit = {
        successBufferedWriter.get.flush()
        successBufferedWriter.get.close()
        errBufferedWriter.get.flush()
        errBufferedWriter.get.close()
        metadataBufferedWriter.get.flush()
        metadataBufferedWriter.get.close()
        if (server != null) {
            server.shutdown()
            server = null
        }
    }

    def startEndpoint(argv: String): Unit = {
        Runtime.getRuntime.exec(Array[String]("/usr/bin/python", "./main.py", argv))
    }

    def isServerStarted: Boolean = server != null

    def map2csv(title: List[String], m: Map[String, Any]): List[Any] = title.map(m)

    def writeHdfs(str: String): Unit = {
        errBufferedWriter.get.write(str)
        errBufferedWriter.get.flush()
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

     def push(message: String): Unit = {
         lock.synchronized {
            successBufferedWriter.get.write("push\n")
            data = data :+ message
         }
    }

    def pop(): String = {
        lock.synchronized {
            if (data.nonEmpty) {
                successBufferedWriter.get.write("pop\n")
                val result = data.head
                successBufferedWriter.get.write(result)
                data = data.tail
                result
            } else "error"
        }
    }
}
