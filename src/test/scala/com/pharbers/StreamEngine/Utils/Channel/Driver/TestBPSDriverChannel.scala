package com.pharbers.StreamEngine.Utils.Channel.Driver

import java.net.InetAddress
import java.util.Date
import java.util.concurrent.Executors

import com.pharbers.StreamEngine.Utils.Channel.Worker.BPSWorkerChannel
import com.pharbers.StreamEngine.Utils.Event.BPSEvents
import com.pharbers.StreamEngine.Utils.Event.StreamListener.BPStreamRemoteListener
import com.pharbers.StreamEngine.Utils.Job.BPStreamJob
import com.pharbers.StreamEngine.Utils.ThreadExecutor.ThreadExecutor
import org.apache.spark.sql.DataFrame
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.write
import org.scalatest.FunSuite

/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数
  * @author dcs
  * @version 0.0
  * @since 2020/01/06 14:39
  * @note 一些值得注意的地方
  */
class TestBPSDriverChannel extends FunSuite{
    val checkData = "{\"tag\": -1, \"data\": {\"data\": {\"2#ID\": \"3504002\", \"0#Units\": \"1800\", \"6#Prod_CNAME\": \"凯伦粉针剂CO2.25G1中国通用技术(集团)控股有限责任公司\", \"9#DOIE\": \"Sulperazon_1M\", \"8#DOI\": \"Sulperazon_1M\", \"5#Prod_Name\": \"凯伦粉针剂CO2.25G1中国通用技术(集团)控股有限责任公司\", \"4#HOSP_ID\": \"PHA0012485\", \"3#Hosp_name\": \"三明市第二医院\", \"10#Date\": \"201504\", \"7#Strength\": \"凯伦粉针剂CO2.25G1中国通用技术(集团)控股有限责任公司\", \"1#Sales\": \"26640.00\"}, \"metadata\": {\"geoCover\": [], \"providers\": [\"Pfizer\", \"CPA&GYC\"], \"assetId\": \"5dd5f670173a3e29a0f71d3d\", \"fileName\": \"2015年辉瑞337个通用名产品数据.xlsx\", \"dataCover\": [\"201601\", \"201610\"], \"length\": 3576.0, \"molecules\": [], \"label\": [\"原始数据\"], \"markets\": [], \"sheetName\": \"Sheet1\", \"schema\": [{\"type\": \"String\", \"key\": \"0#Units\"}, {\"type\": \"String\", \"key\": \"1#Sales\"}, {\"type\": \"String\", \"key\": \"2#ID\"}, {\"type\": \"String\", \"key\": \"3#Hosp_name\"}, {\"type\": \"String\", \"key\": \"4#HOSP_ID\"}, {\"type\": \"String\", \"key\": \"5#Prod_Name\"}, {\"type\": \"String\", \"key\": \"6#Prod_CNAME\"}, {\"type\": \"String\", \"key\": \"7#Strength\"}, {\"type\": \"String\", \"key\": \"8#DOI\"}, {\"type\": \"String\", \"key\": \"9#DOIE\"}, {\"type\": \"String\", \"key\": \"10#Date\"}]}}, \"errMsg\": \"{\\\"exType\\\": \\\"<type 'exceptions.Exception'>\\\", \\\"exTrace\\\": \\\"Traceback (most recent call last):\\\\n  File \\\\\\\"./main.py\\\\\\\", line 25, in facade\\\\n    return process(event)\\\\n  File \\\\\\\"/usr/soft/hadoopData/tmp/nm-local-dir/usercache/qianpeng/appcache/application_1575788630737_0463/container_1575788630737_0463_01_000003/cleaning.py\\\\\\\", line 93, in process\\\\n    raise Exception(m[\\\\\\\"ColName\\\\\\\"] + \\\\\\\" is None，Please check the file or completion mapping rules\\\\\\\")\\\\nException: YEAR is None，Please check the file or completion mapping rules\\\\n\\\", \\\"exValue\\\": \\\"YEAR is None，Please check the file or completion mapping rules\\\"}\", \"metadata\": {}}{\"tag\": -1, \"data\": {\"data\": {\"2#ID\": \"3504002\", \"0#Units\": \"1800\", \"6#Prod_CNAME\": \"凯伦粉针剂CO2.25G1中国通用技术(集团)控股有限责任公司\", \"9#DOIE\": \"Sulperazon_1M\", \"8#DOI\": \"Sulperazon_1M\", \"5#Prod_Name\": \"凯伦粉针剂CO2.25G1中国通用技术(集团)控股有限责任公司\", \"4#HOSP_ID\": \"PHA0012485\", \"3#Hosp_name\": \"三明市第二医院\", \"10#Date\": \"201504\", \"7#Strength\": \"凯伦粉针剂CO2.25G1中国通用技术(集团)控股有限责任公司\", \"1#Sales\": \"26640.00\"}, \"metadata\": {\"geoCover\": [], \"providers\": [\"Pfizer\", \"CPA&GYC\"], \"assetId\": \"5dd5f670173a3e29a0f71d3d\", \"fileName\": \"2015年辉瑞337个通用名产品数据.xlsx\", \"dataCover\": [\"201601\", \"201610\"], \"length\": 3576.0, \"molecules\": [], \"label\": [\"原始数据\"], \"markets\": [], \"sheetName\": \"Sheet1\", \"schema\": [{\"type\": \"String\", \"key\": \"0#Units\"}, {\"type\": \"String\", \"key\": \"1#Sales\"}, {\"type\": \"String\", \"key\": \"2#ID\"}, {\"type\": \"String\", \"key\": \"3#Hosp_name\"}, {\"type\": \"String\", \"key\": \"4#HOSP_ID\"}, {\"type\": \"String\", \"key\": \"5#Prod_Name\"}, {\"type\": \"String\", \"key\": \"6#Prod_CNAME\"}, {\"type\": \"String\", \"key\": \"7#Strength\"}, {\"type\": \"String\", \"key\": \"8#DOI\"}, {\"type\": \"String\", \"key\": \"9#DOIE\"}, {\"type\": \"String\", \"key\": \"10#Date\"}]}}, \"errMsg\": \"{\\\"exType\\\": \\\"<type 'exceptions.Exception'>\\\", \\\"exTrace\\\": \\\"Traceback (most recent call last):\\\\n  File \\\\\\\"./main.py\\\\\\\", line 25, in facade\\\\n    return process(event)\\\\n  File \\\\\\\"/usr/soft/hadoopData/tmp/nm-local-dir/usercache/qianpeng/appcache/application_1575788630737_0463/container_1575788630737_0463_01_000003/cleaning.py\\\\\\\", line 93, in process\\\\n    raise Exception(m[\\\\\\\"ColName\\\\\\\"] + \\\\\\\" is None，Please check the file or completion mapping rules\\\\\\\")\\\\nException: YEAR is None，Please check the file or completion mapping rules\\\\n\\\", \\\"exValue\\\": \\\"YEAR is None，Please check the file or completion mapping rules\\\"}\", \"metadata\": {}}"
    test("drive channel read"){
//        BPSDriverChannel(Map())
        BPSDriverChannel(null)
        BPSDriverChannel.registerListener(new BPStreamRemoteListener {
            override val job: BPStreamJob = null

            override def hit(e: BPSEvents): Boolean = true

            override def trigger(e: BPSEvents): Unit = {
                assert(e.data == checkData)
                assert(e.jobId == "testJobId")
                assert(e.traceId == "testTraceId")
                assert(e.`type` == "testType")
            }

            override def active(s: DataFrame): Unit = ???

            override def deActive(): Unit = ???
        })

        implicit val formats = DefaultFormats
        val event = BPSEvents(
            "testJobId",
            "testTraceId",
            "testType",
            checkData,
            new java.sql.Timestamp(new Date().getTime)
        )

        val executorService = Executors.newFixedThreadPool(8)
        for(i <- 1 to 1){
            val runnable = new Runnable {
                override def run(): Unit = {
                    while (true){
                        val bPSWorkerChannel = BPSWorkerChannel(InetAddress.getLocalHost.getHostAddress)
                        bPSWorkerChannel.pushMessage(write(event))
                        bPSWorkerChannel.close()
                        Thread.sleep(100)
                    }
                }
            }
            executorService.execute(runnable)
        }
        Thread.sleep(30000)
        executorService.shutdownNow()
        Thread.sleep(100000)
    }

    test("1"){
        val bPSWorkerChannel = BPSWorkerChannel(InetAddress.getLocalHost.getHostAddress)
        bPSWorkerChannel.pushMessage("123456")
        bPSWorkerChannel.close()
    }

    test("跨进程发送"){
        implicit val formats = DefaultFormats
        val event = BPSEvents(
            "testJobId",
            "testTraceId",
            "testType",
            checkData,
            new java.sql.Timestamp(new Date().getTime)
        )

        val executorService = Executors.newFixedThreadPool(8)
        for(i <- 1 to 1){
            val runnable = new Runnable {
                override def run(): Unit = {
                    while (true){
                        val bPSWorkerChannel = BPSWorkerChannel(InetAddress.getLocalHost.getHostAddress)
                        bPSWorkerChannel.pushMessage(write(event))
                        bPSWorkerChannel.close()
                        Thread.sleep(100)
                    }
                }
            }
            executorService.execute(runnable)
        }
        Thread.sleep(10000)
    }

}
