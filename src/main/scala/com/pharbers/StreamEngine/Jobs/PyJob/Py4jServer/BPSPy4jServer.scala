package com.pharbers.StreamEngine.Jobs.PyJob.Py4jServer

import java.util.UUID

import py4j.{GatewayServer, Py4JNetworkException}
import java.net.ServerSocket

import org.json4s.DefaultFormats

import scala.util.parsing.json.JSON
import java.nio.charset.StandardCharsets

import org.apache.hadoop.conf.Configuration
import org.json4s.jackson.Serialization.write
import java.io.{BufferedWriter, OutputStreamWriter}

import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}

object BPSPy4jServer extends Serializable {
    var server: Option[BPSPy4jServer] = None

    def isStarted: Boolean = server.nonEmpty
}

/** 实现 Py4j 的 GatewayServer 的实例
 *
 * @author clock
 * @version 0.1
 * @since 2019/11/14 19:04
 */
case class BPSPy4jServer(serverConf: Map[String, Any] = Map().empty) extends Serializable {
    val jobId: String = serverConf.getOrElse("id", UUID.randomUUID().toString).toString
    val hdfsAddr: String = serverConf.getOrElse("hdfsAddr", "hdfs://spark.master:9000").toString
    val fileId: String = UUID.randomUUID().toString
    val rowRecordPath: String = serverConf.getOrElse("rowRecordPath", s"/tmp/pyJob/$jobId/row_record/${fileId}").toString
    val metadataPath: String = serverConf.getOrElse("metadataPath", s"/tmp/pyJob/$jobId/metadata/${fileId}").toString
    val successPath: String = serverConf.getOrElse("successPath", s"/tmp/pyJob/$jobId/success/${fileId}").toString
    val errPath: String = serverConf.getOrElse("errPath", s"/tmp/pyJob/$jobId/err/${fileId}").toString

    // Buffer 写入处理部分
    // 有三种处理类型，分别写入三个流中
    val rowRecordBufferedWriter: Option[BufferedWriter] = openHdfs(rowRecordPath)
    val metadataBufferedWriter: Option[BufferedWriter] = openHdfs(metadataPath)
    val successBufferedWriter: Option[BufferedWriter] = openHdfs(successPath)
    val errBufferedWriter: Option[BufferedWriter] = openHdfs(errPath)

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

    private def writeMetadata(str: String): Unit = {
        metadataBufferedWriter.get.write(str)
        metadataBufferedWriter.get.newLine()
        metadataBufferedWriter.get.flush()
    }

    private def writeSuccess(str: String): Unit = {
        successBufferedWriter.get.write(str)
        successBufferedWriter.get.newLine()
        successBufferedWriter.get.flush()
    }

    def writeErr(str: String): Unit = {
        errBufferedWriter.get.write(str)
        errBufferedWriter.get.newLine()
        errBufferedWriter.get.flush()
    }

    def closeBuffer(): BPSPy4jServer = {
        rowRecordBufferedWriter.get.flush()
        rowRecordBufferedWriter.get.close()
        metadataBufferedWriter.get.flush()
        metadataBufferedWriter.get.close()
        successBufferedWriter.get.flush()
        successBufferedWriter.get.close()
        errBufferedWriter.get.flush()
        errBufferedWriter.get.close()
        this
    }


    // Py4j Server 部分
    var server: GatewayServer = _

    def startServer(): BPSPy4jServer = {
        try {
            val socket = new ServerSocket(0)
            val py4jPort = socket.getLocalPort // 获得一个可用端口
            socket.close()
            server = new GatewayServer(this, py4jPort)
            server.start(true)
        } catch {
            case _: Py4JNetworkException =>
                Thread.sleep(500)
                startServer()
        }
        this
    }

    def startEndpoint(argv: String*): BPSPy4jServer = {
        val args = List("/usr/bin/python", "./main.py") ::: argv.toList
        Runtime.getRuntime.exec(args.toArray)
        this
    }

    def closeServer(): BPSPy4jServer = {
        server.shutdown()
        closeBuffer()
        this
    }


    // Py4j 提供的 API
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


    // 计数器，统计处理的行数
    var curRow: Long = 0L
    var csvTitle: List[String] = Nil

    def writeHdfs(str: String): Unit = {
        synchronized(curRow += 1) // TODO python 可能调用多次，即一条数据清洗出多条来
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

    def stopServer(): Unit = {
        writeRowRecord(curRow) // 写入当前patch的处理条数
        closeServer()
    }
}
