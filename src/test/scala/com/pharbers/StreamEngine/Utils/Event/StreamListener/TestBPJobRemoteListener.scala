package com.pharbers.StreamEngine.Utils.Event.StreamListener

import com.pharbers.StreamEngine.Utils.Event.{BPSEvents, BPSTypeEvents}
import org.scalatest.FunSuite

import scala.reflect.ClassTag

/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数
  * @author dcs
  * @version 0.0
  * @since 2020/04/07 17:06
  * @note 一些值得注意的地方
  */
class TestBPJobRemoteListener extends FunSuite{
    test("test trigger map type"){
        val trig : BPSTypeEvents[Map[String, String]] => Unit = event => assert(event.date.get("key").get == "value")
        val listener = BPJobRemoteListener[Map[String, String]](null, List(""))(x => trig(x))
        listener.trigger(BPSEvents("", "", "", "{\"key\":\"value\"}"))
    }

    case class TestEvent(key: String, name: Option[String])
    test("test trigger object type"){
        val trig : BPSTypeEvents[TestEvent] => Unit = event => assert(event.date.key == "value" && event.date.name.isEmpty)
        val listener = BPJobRemoteListener[TestEvent](null, List(""))(x => trig(x))
        listener.trigger(BPSEvents("", "", "", "{\"key\":\"value\", \"age\":\"value\"}"))
    }

    def testGeneric[T: Manifest](): Unit ={
        val listener = BPJobRemoteListener[T](null, List(""))(x => trigWithGeneric[T](x))
        listener.trigger(BPSEvents("", "", "", "{\"key\":\"value\", \"age\":\"value\"}"))
    }

    def trigWithGeneric[T: Manifest](event: BPSTypeEvents[T]): Unit ={
        assert(event.date.toString == "TestEvent(value,None)")
    }
    test("test function with Generic"){
        testGeneric[TestEvent]()
    }

}
