package com.pharbers.StreamEngine.Utils.Strategy.Session

import com.pharbers.StreamEngine.Utils.Component2.BPComponent
import com.pharbers.util.log.PhLogable

trait SessionStrategy extends BPComponent with PhLogable{
    val sessionType: String
}
