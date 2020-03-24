package com.pharbers.StreamEngine.Utils.HDFS

import java.net.URI
import java.nio.charset.StandardCharsets
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import java.io.{BufferedReader, BufferedWriter, InputStreamReader, OutputStreamWriter}

object BPSHDFSFile {
    val hdfsAddr: String = "hdfs://starLord:8020"

    val configuration: Configuration = new Configuration
    configuration.set("fs.defaultFS", hdfsAddr)
    configuration.set("dfs.client.block.write.replace-datanode-on-failure.enable","true")
    def openHdfsBuffer(path: String): Option[BufferedWriter] = {
        val fileSystem: FileSystem = FileSystem.newInstance(configuration)

        val hdfsWritePath = new Path(path)
        val fsDataOutputStream: FSDataOutputStream =
            if (fileSystem.exists(hdfsWritePath))
                fileSystem.append(hdfsWritePath)
            else
                fileSystem.create(hdfsWritePath)

        Some(new BufferedWriter(new OutputStreamWriter(fsDataOutputStream, StandardCharsets.UTF_8)))
    }

    def checkPath(path: String): Boolean = {
        val fileSystem: FileSystem = FileSystem.get(configuration)
        fileSystem.exists(new Path(path))
    }

    def appendLine2HDFS(path: String, line: String): Unit = {
        val fileSystem: FileSystem = FileSystem.newInstance(configuration)
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
        fileSystem.close()
    }
    
    // TODO: 临时
    def createPath(path: String): Unit = {
        val fileSystem: FileSystem = FileSystem.newInstance(configuration)
        val hdfsWritePath: Path = new Path(path)
        if (!fileSystem.exists(hdfsWritePath))
            fileSystem.mkdirs(hdfsWritePath)
        fileSystem.close()
    }
    
    // 支持文件和目录，但不支持递归型目录
    def readHDFS(path: String): List[String] = {
        if(!checkPath(path)) return Nil

        var result: List[String] = Nil
        val fs = FileSystem.newInstance(configuration)

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
            inBuf.close()
            inReader.close()
            result
        }
        fs.close()
        result
    }
}
