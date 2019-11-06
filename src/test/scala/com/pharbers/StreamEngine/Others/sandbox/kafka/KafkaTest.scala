package com.pharbers.StreamEngine.Others.sandbox.kafka

import java.io.File
import java.util.concurrent.TimeUnit

import com.pharbers.kafka.producer.PharbersKafkaProducer
import com.pharbers.kafka.schema.FileMetaData
import org.scalatest.FunSuite
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}

class KafkaTest extends FunSuite {
	test("Avro Producer SandBox File Meta Data Test") {
		val gr = new FileMetaData()
		gr.setJobId("da0fb-c055-4d27-9d1a-fc9890")
		gr.setMetaDataPath("/test/alex/test001/metadata")
		gr.setSampleDataPath("/test/alex/test001/files/jobId=")
		gr.setConvertType("")
		val pkp = new PharbersKafkaProducer[String, FileMetaData]
		
		1 to 1 foreach { x =>
			val fu = pkp.produce("sb_file_meta_job_test_1", "value", gr)
			println(fu.get(10, TimeUnit.SECONDS))
		}
	}
	
	test("Avro Producer SandBox Convert Schema") {
		val gr = new FileMetaData()
		gr.setJobId("da0fb-c055-4d27-9d1a-fc9890")
		gr.setMetaDataPath("/test/alex/test001/metadata")
		gr.setSampleDataPath("/test/alex/test001/files/jobId=")
		gr.setConvertType("convert_schema")
		val pkp = new PharbersKafkaProducer[String, FileMetaData]
		
		1 to 1 foreach { x =>
			val fu = pkp.produce("sb_file_meta_job_test_1", "value", gr)
			println(fu.get(10, TimeUnit.SECONDS))
		}
	}
}
