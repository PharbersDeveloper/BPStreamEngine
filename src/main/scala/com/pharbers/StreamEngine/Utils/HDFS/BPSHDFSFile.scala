package com.pharbers.StreamEngine.Utils.HDFS

import java.io.{BufferedWriter, OutputStreamWriter}
import java.nio.charset.StandardCharsets

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}

object BPSHDFSFile {
	val configuration: Configuration = new Configuration
	configuration.set("fs.defaultFS", "hdfs://192.168.100.137:9000")
    def checkPath(path: String): Boolean = {
        val fileSystem: FileSystem = FileSystem.get(configuration)
	    fileSystem.exists(new Path(path))
    }
	
	def appendLine2HDFS(path: String, line: String): Unit = {
		val fileSystem: FileSystem = FileSystem.get(configuration)
		val hdfsWritePath: Path = new Path(path)
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
}
