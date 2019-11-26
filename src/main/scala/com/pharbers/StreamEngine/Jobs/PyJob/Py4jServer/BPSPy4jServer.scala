package com.pharbers.StreamEngine.Jobs.PyJob.Py4jServer

import java.util.UUID
import java.net.ServerSocket

import org.json4s.DefaultFormats

import scala.util.parsing.json.JSON
import java.nio.charset.StandardCharsets

import org.apache.hadoop.conf.Configuration
import org.json4s.jackson.Serialization.write
import py4j.{GatewayServer, Py4JNetworkException}
import java.io.{BufferedWriter, OutputStreamWriter}

import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}

object BPSPy4jServer extends Serializable {

    // 保存当前 JVM 上运行的所有 Py4j 服务
    private var servers: Map[String, BPSPy4jServer] = Map.empty

    def open(serverConf: Map[String, Any] = Map().empty): Unit = {
        synchronized {
            val server = BPSPy4jServer(serverConf).openBuffer().startServer().startEndpoint()
            servers = servers + (server.jobId -> server)
        }
    }


    // 保存流中的数据，并可以给 Python 访问
    var dataQueue: List[String] = Nil

    def push(message: String): Unit = {
        synchronized {
            BPSPy4jServer.dataQueue = BPSPy4jServer.dataQueue ::: message :: Nil
        }
    }
}

/** 实现 Py4j 的 GatewayServer 的实例
 *
 * @author clock
 * @version 0.1
 * @since 2019/11/14 19:04
 */
case class BPSPy4jServer(serverConf: Map[String, Any] = Map().empty) extends Serializable {
    final val RETRY_COUNT: Int = 3

    val jobId: String = serverConf.getOrElse("id", UUID.randomUUID().toString).toString
    val hdfsAddr: String = serverConf.getOrElse("hdfsAddr", "hdfs://spark.master:9000").toString

    val threadId: String = UUID.randomUUID().toString
    val rowRecordPath: String = serverConf.getOrElse("rowRecordPath", s"/tmp/pyJob/$jobId/row_record/$threadId").toString
    val metadataPath: String = serverConf.getOrElse("metadataPath", s"/tmp/pyJob/$jobId/metadata/$threadId").toString
    val successPath: String = serverConf.getOrElse("successPath", s"/tmp/pyJob/$jobId/success/$threadId").toString
    val errPath: String = serverConf.getOrElse("errPath", s"/tmp/pyJob/$jobId/err/$threadId").toString


    // Buffer 写入处理部分
    // 有三种处理类型，分别写入三个流中
    var rowRecordBufferedWriter: Option[BufferedWriter] = None
    var metadataBufferedWriter: Option[BufferedWriter] = None
    var successBufferedWriter: Option[BufferedWriter] = None
    var errBufferedWriter: Option[BufferedWriter] = None

    private def openHdfs(path: String): Option[BufferedWriter] = {
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

    def writeRowRecord(row: Long): Unit = {
        rowRecordBufferedWriter.get.write(row.toString)
        rowRecordBufferedWriter.get.newLine()
        rowRecordBufferedWriter.get.flush()
    }

    def writeMetadata(str: String): Unit = {
        metadataBufferedWriter.get.write(str)
        metadataBufferedWriter.get.newLine()
        metadataBufferedWriter.get.flush()
    }

    def writeSuccess(str: String): Unit = {
        successBufferedWriter.get.write(str)
        successBufferedWriter.get.newLine()
        successBufferedWriter.get.flush()
    }

    def writeErr(str: String): Unit = {
        errBufferedWriter.get.write(str)
        errBufferedWriter.get.newLine()
        errBufferedWriter.get.flush()
    }

    def openBuffer(): BPSPy4jServer = {
        if (rowRecordBufferedWriter.isEmpty) rowRecordBufferedWriter = openHdfs(rowRecordPath)
        if (metadataBufferedWriter.isEmpty) metadataBufferedWriter = openHdfs(metadataPath)
        if (successBufferedWriter.isEmpty) successBufferedWriter = openHdfs(successPath)
        if (errBufferedWriter.isEmpty) errBufferedWriter = openHdfs(errPath)
        this
    }

    def closeBuffer(): BPSPy4jServer = {
        if (rowRecordBufferedWriter.isDefined) {
            rowRecordBufferedWriter.get.flush()
            rowRecordBufferedWriter.get.close()
        }
        if (metadataBufferedWriter.isDefined) {
            metadataBufferedWriter.get.flush()
            metadataBufferedWriter.get.close()
        }
        if (successBufferedWriter.isDefined) {
            successBufferedWriter.get.flush()
            successBufferedWriter.get.close()
        }
        if (errBufferedWriter.isDefined) {
            errBufferedWriter.get.flush()
            errBufferedWriter.get.close()
        }
        this
    }


    // Py4j Server 部分
    var server: GatewayServer = _

    private var startServerCount = 0

    def startServer(): BPSPy4jServer = {
        val socket = new ServerSocket(0)
        val py4jPort = socket.getLocalPort // 获得一个可用端口
        socket.close()

        try {
            server = new GatewayServer(this, py4jPort)
            server.start(true)
            startServerCount = 0
        } catch {
            case _: Py4JNetworkException =>
                if (startServerCount >= RETRY_COUNT) {
                    throw new Exception(s"start py4j server failure, port = $py4jPort")
                }
                Thread.sleep(1000)
                startServerCount += 1
                startServer()
        }
        this
    }

    def shutdownServer(): BPSPy4jServer = {
        server.shutdown()
        this
    }

    //    var startEndpointCount = 0
    def startEndpoint(argv: String*): BPSPy4jServer = {
        val socket = new ServerSocket(0)
        val callbackPort = socket.getLocalPort // 获得一个可用端口
        socket.close()

        val args = List("/usr/bin/python", "./main.py") :::
                server.getPort.toString ::
                callbackPort.toString ::
                argv.toList

        val pr = Runtime.getRuntime.exec(args.toArray)
//        val in = new BufferedReader(new InputStreamReader(pr.getErrorStream))
//        val result = in.readLine()
//        in.close()
//
//        // 如果错误输出流不为空，证明Python启动出错，则重试
//        if (result != null) {
//            println("funck" + callbackPort)
//            if (startEndpointCount >= RETRY_COUNT) {
//                throw new Exception(s"start py4j endpoint failure, port = $callbackPort")
//            }
//            Thread.sleep(2000)
//            startEndpointCount += 1
//            startEndpoint(argv: _*)
//        } else {
//            startEndpointCount = 0
//        }

        this
    }


    // Py4j 提供的 API
    def py4j_pop(): String = {
        synchronized {
            if (BPSPy4jServer.dataQueue.nonEmpty) {
                val result = BPSPy4jServer.dataQueue.head
                BPSPy4jServer.dataQueue = BPSPy4jServer.dataQueue.tail
                result
            } else "EMPTY"
        }
    }

    // 计数器，统计处理的行数
    var curRow: Long = 0L
    var csvTitle: List[String] = Nil

    def py4j_writeHdfs(str: String): Unit = {
        // python 可能调用多次，即一条数据清洗出多条来
        // 2019-11-26 补充: 这里计数没问题，记录处理后的数据条目，而且只多不少，所以Listener判断输出数据条目大于等于输入数据条数
        synchronized(curRow += 1)
        JSON.parseFull(str) match {
            case Some(result: Map[String, AnyRef]) =>
                if (result("tag").asInstanceOf[Double] == 1) {
                    if (curRow == 1L) {
                        val metadata = result("metadata").asInstanceOf[Map[String, Any]]
                        writeMetadata(write(metadata)(DefaultFormats))
                        csvTitle = writeTitle(metadata)
                    }
                    writeSuccess(map2csv(csvTitle, result("data").asInstanceOf[Map[String, Any]]).mkString(","))
                } else writeErr(str)
            case _ => writeErr(str)
        }

        def writeTitle(metadata: Map[String, Any]): List[String] = {
            val csvTitle = metadata("schema").asInstanceOf[List[Any]].map { x =>
                x.asInstanceOf[Map[String, String]]("key").toString
            }
            writeSuccess(csvTitle.mkString(","))
            csvTitle
        }

        def map2csv(title: List[String], m: Map[String, Any]): List[Any] = title.map(m)
    }

    def py4j_stopServer(): Unit = {
        writeRowRecord(curRow) // 写入当前patch的处理条数
        closeBuffer()
        shutdownServer()
        synchronized(BPSPy4jServer.servers = BPSPy4jServer.servers - jobId)
    }
}
