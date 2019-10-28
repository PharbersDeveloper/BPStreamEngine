package com.pharbers.sandbox.kafka

import java.io.File
import java.util.concurrent.TimeUnit

import com.pharbers.kafka.producer.PharbersKafkaProducer
import com.pharbers.kafka.schema.FileMetaData
import org.scalatest.FunSuite
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}

class KafkaTest extends FunSuite {
	test("Avro Producer Test") {
//		val sche: Schema = Schema.parse(new File("src/main/avro/file_mete_data.avsc"))
//		val gr: GenericRecord = new GenericData.Record(sche)
		val gr = new FileMetaData()
		gr.setJobId("da0fb-c055-4d27-9d1a-fc9890")
		gr.setMetaDataPath("/test/alex/test001/metadata")
		gr.setSampleDataPath("/test/alex/test001/files/jobId=")
		val pkp = new PharbersKafkaProducer[String, FileMetaData]
		
		val fu = pkp.produce("sb_file_meta_job_test_1", "value", gr)
		println(fu.get(10, TimeUnit.SECONDS))
	}
}
