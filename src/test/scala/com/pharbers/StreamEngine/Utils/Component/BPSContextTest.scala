package com.pharbers.StreamEngine.Utils.Component

import com.pharbers.StreamEngine.Utils.Log.BPSLogContext
import org.scalatest.FunSuite

/** Spark Session Test
 *
 * @author clock
 * @version 0.1
 * @since 2019/11/06 10:04
 * @note 注意配置文件的优先级和使用方式
 */
class BPSContextTest extends FunSuite {
    test("Init ComponentContext") {
        ComponentContext.init()
    }

    test("Init BPSLogContext") {
        BPSLogContext.init()
    }
}
