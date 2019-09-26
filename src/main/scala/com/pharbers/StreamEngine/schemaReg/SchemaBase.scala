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
}
