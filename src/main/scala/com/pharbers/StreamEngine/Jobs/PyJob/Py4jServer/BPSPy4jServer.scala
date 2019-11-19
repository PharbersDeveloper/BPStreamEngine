package com.pharbers.StreamEngine.Jobs.PyJob.Py4jServer

import py4j.GatewayServer
import org.json4s.DefaultFormats
import scala.util.parsing.json.JSON
import java.nio.charset.StandardCharsets
import org.apache.hadoop.conf.Configuration
import org.json4s.jackson.Serialization.write
import java.io.{BufferedWriter, OutputStreamWriter}
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}

// 需要使用单例保存Server启动状态，原因可看 ForeachWriter 的类加载原理
object BPSPy4jServer extends Serializable {
    // Py4j Gateway Server 的引用，用来和 Python 通信
    var gateway: GatewayServer = _

    def isGatewayStarted: Boolean = gateway != null

    // 用来记录 Server 的一些执行信息
    var server: BPSPy4jServer = _

    def isServerStarted: Boolean = server != null
}

/** 实现 Py4j 的 GatewayServer 的实例
 *
 * @author clock
 * @version 0.1
 * @since 2019/11/14 19:04
 */
case class BPSPy4jServer(totalRow: Long) extends Serializable {
    // 计数器，统计处理的行数
    var curRow: Long = 0L

    // 有三种处理类型，分别写入三个流中
    var metadataBufferedWriter: Option[BufferedWriter] = None
    var successBufferedWriter: Option[BufferedWriter] = None
    var errBufferedWriter: Option[BufferedWriter] = None

    private def openHdfs(hdfsAddr: String)(path: String): Option[BufferedWriter] = {
        val configuration: Configuration = new Configuration()
        configuration.set("fs.defaultFS", hdfsAddr)

        val fileSystem: FileSystem = FileSystem.get(configuration)
        val hdfsWritePath = new Path(path)

        val fsDataOutputStream: FSDataOutputStream =
            if (fileSystem.exists(hdfsWritePath))
                fileSystem.append(hdfsWritePath)
            else
                fileSystem.create(hdfsWritePath)

        Some(new BufferedWriter(new OutputStreamWriter(fsDataOutputStream, StandardCharsets.UTF_8)))
    }

    def openBuffer(hdfsAddr: String)(metadataPath: String, successPath: String, errPath: String): BPSPy4jServer = {
        metadataBufferedWriter = openHdfs(hdfsAddr)(metadataPath)
        successBufferedWriter = openHdfs(hdfsAddr)(successPath)
        errBufferedWriter = openHdfs(hdfsAddr)(errPath)
        this
    }

    def closeBuffer(): BPSPy4jServer = {
        successBufferedWriter.get.flush()
        successBufferedWriter.get.close()
        errBufferedWriter.get.flush()
        errBufferedWriter.get.close()
        metadataBufferedWriter.get.flush()
        metadataBufferedWriter.get.close()
        this
    }

    def startServer(): BPSPy4jServer = {
        if (!BPSPy4jServer.isGatewayStarted) {
            BPSPy4jServer.gateway = new GatewayServer(this)
            BPSPy4jServer.gateway.start(true)
        }
        if (!BPSPy4jServer.isServerStarted) {
            BPSPy4jServer.server = this
        }
        this
    }

    def closeServer(): BPSPy4jServer = {
        closeBuffer()
        if (BPSPy4jServer.isGatewayStarted) {
            BPSPy4jServer.gateway = null
        }
        if (BPSPy4jServer.isServerStarted) {
            BPSPy4jServer.server = null
        }
        this
    }

    def startEndpoint(argv: String*): BPSPy4jServer = {
        val args = List("/usr/bin/python", "./main.py") ::: argv.toList
        Runtime.getRuntime.exec(args.toArray)
        this
    }

    // 保存流中的数据，并可以给 Python 访问
    var dataQueue: List[String] = Nil

    def push(message: String): Unit = {
        synchronized {
            dataQueue = dataQueue ::: message :: Nil
        }
    }

    def pop(): String = {
        synchronized {
            if (dataQueue.nonEmpty) {
                val result = dataQueue.head
                dataQueue = dataQueue.tail
                result
            } else "EMPTY"
        }
    }

    private def writeErr(str: String): Unit = {
        errBufferedWriter.get.write(str)
        errBufferedWriter.get.newLine()
        errBufferedWriter.get.flush()
    }

    private def writeMetadata(metadata: Map[String, Any]): Unit = {
        metadataBufferedWriter.get.write(write(metadata)(DefaultFormats))
        metadataBufferedWriter.get.newLine()
        metadataBufferedWriter.get.flush()
    }

    private def writeTitle(metadata: Map[String, Any]): List[String] = {
        val csvTitle = metadata("schema").asInstanceOf[List[Any]].map { x =>
            x.asInstanceOf[Map[String, String]]("key").toString
        }
        successBufferedWriter.get.write(csvTitle.mkString(","))
        successBufferedWriter.get.newLine()
        successBufferedWriter.get.flush()
        csvTitle
    }

    private def map2csv(title: List[String], m: Map[String, Any]): List[Any] = title.map(m)

    var csvTitle: List[String] = Nil
    def writeHdfs(str: String): Unit = {
        JSON.parseFull(str) match {
            case Some(result: Map[String, AnyRef]) =>
                if (result("tag").asInstanceOf[Double] == 1) {
                    synchronized {
                        curRow += 1
                        if(curRow == totalRow) {
                            this.push("EOF")
                        } else if (curRow == 1L) {
                            val metadata = result("metadata").asInstanceOf[Map[String, Any]]
                            writeMetadata(metadata)
                            csvTitle = writeTitle(metadata)
                        }
                    }

                    val b = result("data").asInstanceOf[Map[String, Any]] ++ Map("MKT" -> curRow)
                    successBufferedWriter.get.write(
                        map2csv(csvTitle, b).mkString(",")
                    )
                    successBufferedWriter.get.newLine()
                    successBufferedWriter.get.flush()
                } else writeErr(str)
            case _ => writeErr(str)
        }
    }
}
