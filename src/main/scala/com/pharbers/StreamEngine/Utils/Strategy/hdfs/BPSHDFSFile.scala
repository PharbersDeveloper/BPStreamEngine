package com.pharbers.StreamEngine.Utils.Strategy.hdfs

import java.nio.charset.StandardCharsets

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FSDataOutputStream, FileSystem, Path}
import java.io.{BufferedReader, BufferedWriter, File, InputStreamReader, OutputStreamWriter}

import com.pharbers.StreamEngine.Utils.Annotation.Component
import com.pharbers.StreamEngine.Utils.Component2
import com.pharbers.StreamEngine.Utils.Strategy.BPStrategyComponent
import org.apache.kafka.common.config.ConfigDef

@Component(name = "BPSHDFSFile", `type` = "BPSHDFSFile")
case class BPSHDFSFile(override val componentProperty: Component2.BPComponentConfig)
    extends BPStrategyComponent {
    
    val hdfsAddr: String = componentProperty.config("hdfsAddr")
    
    val configuration: Configuration = new Configuration
    configuration.set("fs.defaultFS", hdfsAddr)
//    configuration.set("dfs.client.block.write.replace-datanode-on-failure.policy","NEVER")
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
        val r = fileSystem.exists(new Path(path))
        fileSystem.close()
        r
    }
    
    def appendLine2HDFS(path: String, line: String): Unit = {
        try {
            val fileSystem: FileSystem = FileSystem.newInstance(configuration)
            val hdfsWritePath: Path = new Path(path)
            val fsDataOutputStream: FSDataOutputStream =
                if (fileSystem.exists(hdfsWritePath))
                    fileSystem.append(hdfsWritePath)
                else
                    fileSystem.create(hdfsWritePath)
            val outPut = new OutputStreamWriter(fsDataOutputStream, StandardCharsets.UTF_8)
            val bufferedWriter: BufferedWriter = new BufferedWriter(outPut)
            bufferedWriter.write(line)
            bufferedWriter.newLine()
            bufferedWriter.flush()
            outPut.flush()
            fsDataOutputStream.flush()
            bufferedWriter.close()
            fsDataOutputStream.close()
            outPut.close()
            fileSystem.close()
        } catch {
            case e: Exception =>
                println(e)
        }
        
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
    
    // 递归读取一个目录下的所有文件
    def recursiveFiles(hdfsPath: String): Option[List[recursiveFile]] = {
        if(!checkPath(hdfsPath)) return None
        val fs = FileSystem.newInstance(configuration)
        val files = fs.listFiles(new Path(hdfsPath), true)
        var results: List[recursiveFile] = Nil
        while (files.hasNext) {
            val fileStatus = files.next()
            val path = fileStatus.getPath.toString
          results = List(recursiveFile(
              path.substring(0, path.lastIndexOf("/")).substring(path.indexOf("jobs") - 1),
              path.substring(path.lastIndexOf("/") + 1),
              fs,
              fs.open(new Path(path)))) ++ results
        }
        if(results.isEmpty) {
            None
        } else {
            Some(results)
        }
    }
    
    override val strategyName: String = "hdfs"
    override def createConfigDef(): ConfigDef = ???
    case class recursiveFile(path: String, name: String, fs: FileSystem, input: FSDataInputStream)
}
