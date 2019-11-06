package com.pharbers.StreamEngine.Jobs.OssPartitionJob.OssPartitionMeta

import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import java.io.BufferedWriter
import java.io.OutputStreamWriter
import java.nio.charset.StandardCharsets

import org.apache.hadoop.conf.Configuration

object BPSOssPartitionMeta {

    def pushLineToHDFS(runId: String, jobId: String, line: String): Unit = {
        val configuration: Configuration = new Configuration
        configuration.set("fs.defaultFS", "hdfs://192.168.100.137:9000")
        val fileSystem: FileSystem = FileSystem.get(configuration)
        //Create a path
        val fileName: String = "_metadata"
        val hdfsWritePath: Path = new Path("/workData/streamingV2/" + runId + "/metadata/" + jobId + "")
//        val hdfsWritePath: Path = new Path("/test/alex/" + runId + "/metadata/" + jobId + "")
        val fsDataOutputStream: FSDataOutputStream =
            if (fileSystem.exists(hdfsWritePath))
                fileSystem.append(hdfsWritePath)
            else
                fileSystem.create(hdfsWritePath)

        val bufferedWriter: BufferedWriter = new BufferedWriter(new OutputStreamWriter(fsDataOutputStream, StandardCharsets.UTF_8))
        bufferedWriter.write(line)
        bufferedWriter.newLine()
        bufferedWriter.close()
    }

    def pushSchema(runId: String, jobId: String, schema: String): Unit = pushLineToHDFS(runId, jobId, schema)
    def pushLength(runId: String, jobId: String, length: Int): Unit = pushLineToHDFS(runId, jobId, length.toString)
}
