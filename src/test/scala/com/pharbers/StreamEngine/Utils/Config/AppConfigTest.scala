package com.pharbers.StreamEngine.Utils.Config

import java.util
import org.scalatest.FunSuite
import java.io.FileInputStream
import io.confluent.common.config.ConfigDef
import io.confluent.common.config.ConfigDef.{Importance, Type}

class AppConfigTest extends FunSuite {

    test("config define") {
        println("start")

        val definition = new ConfigDef()
            .define("project.name", Type.STRING, Importance.LOW, "docs")
            .define("hostname", Type.STRING, Importance.HIGH, "docs")

        val props = new util.Properties()
        props.load(new FileInputStream("src/main/resources/app.config.properties"))
        val parsedProps = definition.parse(props)

        println(parsedProps.get("project.name"))
        println(parsedProps.get("hostname"))

    }

    test("app config define") {
        println("start app config define")

        val ac = AppConfig()

        val p = ac.getString(AppConfig.PROJECT_NAME_KEY)
        val h = ac.getString(AppConfig.HOSTNAME_KEY)

        println("project=" + p)
        println("hostname=" + h)

    }
}
