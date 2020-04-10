package com.pharbers.StreamEngine.Utils.Event

import org.scalatest.FunSuite

/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数
  * @author dcs
  * @version 0.0
  * @since 2020/04/08 11:24
  * @note 一些值得注意的地方
  */
class TestBPSEvents extends FunSuite{
    case class testObj(id: String, name: String)
    test("test apply function"){
        val obj = testObj("1", "test")
        val event = BPSEvents("", "", "", obj)
        assert(event.data == "{\"id\":\"1\",\"name\":\"test\"}")
    }
}
