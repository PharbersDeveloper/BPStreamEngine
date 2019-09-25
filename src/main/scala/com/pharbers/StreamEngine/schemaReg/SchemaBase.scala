package com.pharbers.StreamEngine.schemaReg

import org.apache.spark.sql.types.{StringType, StructField, StructType}

object SchemaReg {
    val logSchema: StructType = StructType(Seq(
        StructField("Time", StringType),
        StructField("Hostname", StringType),
        StructField("ProjectName",StringType),
        StructField("File", StringType),
        StructField("Func", StringType),
        StructField("JobId", StringType),
        StructField("TraceId", StringType),
        StructField("UserId", StringType),
        StructField("Message", StringType),
        StructField("Level", StringType)
    ))

    val baseSchema: StructType = StructType(Seq(
        StructField("jobId", StringType),
        StructField("traceId", StringType),
        StructField("type", StringType),
        StructField("data", StringType)
    ))

    val tmpSche: StructType = StructType(Seq(
        StructField("value", StringType)
    ))

    val SchemaRegisterAddr = "59.110.31.50:8081"
    
    val tmp =
        """
          | {
          |     "type": "record",
          |     "name": "ConnectDefault",
          |     "namespace":"io.confluent.connect.avro",
          |     "fields": [
          |         { "name": "jobId", "type": "string" },
          |         { "name": "traceId","type":"string" },
          |         { "name": "type", "type": "string" },
          |         { "name": "data", "type":"string" }
          |     ]
          |}
        """.stripMargin
}
