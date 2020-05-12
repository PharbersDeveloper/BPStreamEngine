package com.pharbers.StreamEngine.Utils.Strategy.Session

import com.pharbers.StreamEngine.Utils.Component2.BPComponent

trait SessionStrategy extends BPComponent{
    val sessionType: String
}
