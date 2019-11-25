package com.pharbers.StreamEngine.Utils.HDFS

import java.net.URI
import java.nio.charset.StandardCharsets

import org.apache.hadoop.conf.Configuration
import java.io.{BufferedReader, BufferedWriter, InputStreamReader, OutputStreamWriter}

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

    // 支持文件和目录，但不支持递归型目录
    def readHDFS(path: String): List[String] = {
        if(!checkPath(path)) return Nil

        var result: List[String] = Nil
        val fs = FileSystem.get(new URI(path), configuration)

        // 判断是否是目录
        if(fs.isDirectory(new Path(path))) {
            val status = fs.listStatus(new Path(path))
            for(file <- status) {
                result = result ::: readFile(file.getPath)
            }
        } else {
            result = readFile(new Path(path))
        }

        def readFile(path: Path): List[String] = {
            var result: List[String] = Nil

            val inBuf = fs.open(path)
            val inReader = new BufferedReader(new InputStreamReader(inBuf))

            var line = inReader.readLine()
            while(line != null) {
                result = result ::: line :: Nil
                line = inReader.readLine()
            }

            result
        }

        result
    }
}
