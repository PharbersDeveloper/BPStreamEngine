package com.pharbers.StreamEngine.Others.clock

import java.io.{BufferedReader, InputStreamReader}

/** 调用 Python 脚本
 *
 * @author clock
 * @version 0.0
 * @since 2019/10/31 20:01
 * @note
 */
object CallPyScript extends App {
    val argv = Array[String]("/usr/bin/python", "./pyClean/main.py", """{"_metadata":{"a":1,"b":2},"data":{}}""")
    val pr = Runtime.getRuntime.exec(argv)
    val in = new BufferedReader(new InputStreamReader(pr.getInputStream))
    println(in.readLine())
}
