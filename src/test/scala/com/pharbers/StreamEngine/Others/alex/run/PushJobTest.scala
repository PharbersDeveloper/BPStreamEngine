//package com.pharbers.StreamEngine.Others.alex.run
//
//import java.util
//import java.util.{Collections, UUID}
//import java.util.concurrent.TimeUnit
//
//import com.pharbers.kafka.producer.PharbersKafkaProducer
//import com.pharbers.kafka.schema.OssTask
//import org.scalatest.FunSuite
//import collection.JavaConverters._
//
//class PushJobTest extends FunSuite {
//	test("push job") {
//		var count = 0
//		while (count < 100){
//			val task = new OssTask("5e79e004d589564646ace709",
//				"1111111111111112",
//				"1111111111111112",
//				"5c2e3756-2363-4d28-97e2-cdc5b20c82ad/1574249019807",
//				"xlsx",
//				"2017年1月辉瑞头孢孟多产品数据.xlsx",
//				"",
//				"owner",
//				10000.toLong,
//				Collections.emptyList(),
//				Collections.emptyList(),
//				Collections.emptyList(),
//				Collections.emptyList(),
//				Collections.emptyList(),
//				List[CharSequence]("Pfizer", "CPA&GYC").asJava
//			)
//			val pkp = new PharbersKafkaProducer[String, OssTask]
//			val fu = pkp.produce("oss_task_submit", UUID.randomUUID().toString, task)
//			val res = fu.get(10, TimeUnit.SECONDS)
//			println(res)
//			count += 1
//			println(count)
//			Thread.sleep(1000)
//		}
//	}
//}

import java.util
import java.util.{Collections, UUID}
import java.util.concurrent.TimeUnit

import com.pharbers.kafka.producer.PharbersKafkaProducer
import com.pharbers.kafka.schema.OssTask
import org.scalatest.FunSuite
import collection.JavaConverters._

class PushJobTest extends FunSuite {
//	test("push job") {
//		var count = 0
//		while (count < 1){
//			val task = new OssTask("5eb386d93c5d7a00970f9541",
//				"1111111111111112",
//				"1111111111111112",
//				"5c2e3756-2363-4d28-97e2-cdc5b20c82ad/1574249019807",
//				"xlsx",
//				"2017年1月辉瑞头孢孟多产品数据.xlsx",
//				"",
//				"owner",
//				10000.toLong,
//				Collections.emptyList(),
//				Collections.emptyList(),
//				Collections.emptyList(),
//				Collections.emptyList(),
//				Collections.emptyList(),
//				List[CharSequence]("Pfizer", "CPA&GYC").asJava
//			)
//			val pkp = new PharbersKafkaProducer[String, OssTask]
//			val fu = pkp.produce("oss_task_submit_test", UUID.randomUUID().toString, task)
//			val res = fu.get(10, TimeUnit.SECONDS)
//			println(res)
//			count += 1
//			println(count)
//			Thread.sleep(1000)
//		}
//	}
}
