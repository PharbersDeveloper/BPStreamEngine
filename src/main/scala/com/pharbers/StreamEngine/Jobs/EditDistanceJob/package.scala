package com.pharbers.StreamEngine.Jobs

import com.pharbers.StreamEngine.Utils.Component2.BPSConcertEntry
import com.pharbers.StreamEngine.Utils.Strategy.Session.Spark.BPSparkSession

package object EditDistanceJob {
    lazy val spark: BPSparkSession = BPSConcertEntry.queryComponentWithId("spark").get.asInstanceOf[BPSparkSession]
    def getVersion(tableName: String, mode: String = "append"): String = {
        val mode2Version = Map("append" -> 0, "overwrite" -> 1)
        val tables = spark.sql("show tables").select("tableName").collect().map(x => x.getString(0))
        if (tables.contains(s"$tableName")) {
            val old = spark.sql(s"select version from $tableName limit 1").take(1).head.getString(0).split("\\.")
            s"${old.head}.${old(1)}.${old(2).toInt + mode2Version.getOrElse(mode, 0)}"
        } else {
            "0.0.1"
        }
    }

    def getTableSavePath(tableName: String, inVersion: String, checkVersion: String, version: String): String ={
        val rootPath = "/common/public"
        s"$rootPath/$tableName/${tableName}_${inVersion}_$checkVersion/$version"
    }
}
