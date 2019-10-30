package com.pharbers.config

import java.io.FileInputStream
import java.util

import com.pharbers.StreamEngine.Utils.Channel.Driver.BPSDriverChannel
import com.pharbers.StreamEngine.Utils.Channel.Local.BPSLocalChannel
import com.pharbers.StreamEngine.Utils.Component.ComponentContext
import com.pharbers.StreamEngine.Utils.Config.AppConfig
import com.pharbers.StreamEngine.Utils.StreamJob.BPStreamJob
import com.pharbers.StreamEngine.Utils.StreamJob.JobStrategy.BPSKfkJobStrategy
import io.confluent.common.config.ConfigDef
import io.confluent.common.config.ConfigDef.Type
import io.confluent.common.config.ConfigDef.Importance
import org.scalatest.FunSuite

class AppConfigTest extends FunSuite {

    test("config define") {
        println("start")

        val definition = new ConfigDef()
            .define("host", Type.STRING, Importance.LOW, "docs")
            .define("port", Type.INT, Importance.HIGH, "docs")

        val props = new util.Properties()
        props.load(new FileInputStream("/Users/jeorch/Documents/test/appConfig.properties"))

        val parsedProps = definition.parse(props)
        println(parsedProps.get("host"))
        println(parsedProps.get("port"))

    }

    test("app config define") {
        println("start app config define")

        val ac = AppConfig()

        val p = ac.getString(AppConfig.PROJECT_NAME_KEY)
        val h = ac.getString(AppConfig.HOSTNAME_KEY)

        println("p=" + p)
        println("h=" + h)

    }
}
