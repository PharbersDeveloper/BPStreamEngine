package com.pharbers.StreamEngine.BPStreamJob.FileJobs.OssListener

import com.pharbers.StreamEngine.BPJobChannels.LocalChannel.LocalChannel
import com.pharbers.StreamEngine.BPStreamJob.BPStreamJob
import com.pharbers.StreamEngine.Common.Events
import com.pharbers.StreamEngine.Common.StreamListener.BPStreamListener
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.util.AccumulatorV2

case class BPSOssEndListener(
                                val spark: SparkSession,
                                val job: BPStreamJob,
                                val queryName: String,
                                val length: Int
                            ) extends BPStreamListener {
    override def trigger(e: Events): Unit = {
        val tmp = spark.sql("select * from " + queryName).collect()
        //todo: log
        println(s"queryName:$queryName")
        println(s"length:$length")
        if (tmp.length > 0) println(s"count: ${tmp.head.getAs[Long]("count")}")
        if (tmp.length > 0 && tmp.head.getAs[Long]("count") == length) {
            job.close()
        }
    }

    override def active(s: DataFrame): Unit = LocalChannel.registerListener(this)

    override def deActive(): Unit = LocalChannel.unRegisterListener(this)
}

//object JobCountAccumulator extends AccumulatorV2[String, Map[String, Int]]{
//    private val jobCountVector = Map[String, Int]()
//
//    def isZero: Boolean = {
//        jobCountVector.isEmpty
//    }
//
//    override def copy(): AccumulatorV2[String, Map[Int, Int]] = {
//        jobCountVector
//    }
//
//    /**
//      * Resets this accumulator, which is zero value. i.e. call `isZero` must
//      * return true.
//      */
//    def reset(): Unit = {
//
//    }
//
//    /**
//      * Takes the inputs and accumulates.
//      */
//    def add(v: String): Unit = {
//
//    }
//
//    /**
//      * Merges another same-type accumulator into this one and update its state, i.e. this should be
//      * merge-in-place.
//      */
//    override def merge(other: AccumulatorV2[String, Map[Int, Int]]): Unit = {
//
//    }
//
//    /**
//      * Defines the current value of this accumulator
//      */
//    override def value: Map[Int, Int] = {
//
//    }
//}
