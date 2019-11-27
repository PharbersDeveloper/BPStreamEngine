package com.pharbers.StreamEngine.Utils.Session.Spark

import org.scalatest.FunSuite

/** Spark Session Test
 *
 * @author clock
 * @version 0.0.1
 * @since 2019/11/06 10:04
 * @note 注意配置文件的优先级和使用方式
 */
class BPSparkSessionTest extends FunSuite {
    test("Create BPSparkSession By Default Config") {
        assert(BPSparkSession() != null)
    }

    test("Create BPSparkSession By Args Config") {
        assert(BPSparkSession(Map("app.name" -> "BPSparkSessionTest")) != null)
    }
}
