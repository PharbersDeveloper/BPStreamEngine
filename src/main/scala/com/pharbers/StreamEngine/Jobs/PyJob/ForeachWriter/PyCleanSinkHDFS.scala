package com.pharbers.StreamEngine.Jobs.PyJob.ForeachWriter

import java.util.UUID
import org.json4s.DefaultFormats
import org.apache.spark.sql.types.StringType
import org.json4s.jackson.Serialization.write
import org.apache.spark.sql.{ForeachWriter, Row}
import com.pharbers.StreamEngine.Jobs.PyJob.Py4jServer.BPSPy4jManager

case class PyCleanSinkHDFS(fileSuffix: String,
                           retryCount: String,
                           jobId: String,
                           rowRecordPath: String,
                           successPath: String,
                           errPath: String,
                           metadataPath: String,
                           lastMetadata: Map[String, Any],
                           py4jManager: BPSPy4jManager) extends ForeachWriter[Row]{

    override def open(partitionId: Long, version: Long): Boolean = {
        val threadId = UUID.randomUUID().toString
        val genPath: String => String = path => s"$path/part-$partitionId-$threadId.$fileSuffix"

        py4jManager.open(Map(
            "retryCount" -> retryCount,
            "jobId" -> jobId,
            "threadId" -> threadId,
            "rowRecordPath" -> genPath(rowRecordPath),
            "successPath" -> genPath(successPath),
            "errPath" -> genPath(errPath),
            "metadataPath" -> genPath(metadataPath)
        ))

        true
    }

    override def process(value: Row): Unit = {
        val data = value.schema.map { schema =>
            schema.dataType match {
                case StringType =>
                    schema.name -> value.getAs[String](schema.name)
                case _ => ???
            }
        }.toMap

        py4jManager.push(
            write(Map("metadata" -> lastMetadata, "data" -> data))(DefaultFormats)
        )
    }

    override def close(errorOrNull: Throwable): Unit = {
        py4jManager.push("EOF")
        while (py4jManager.dataQueue.nonEmpty) Thread.sleep(1000)
    }
}
