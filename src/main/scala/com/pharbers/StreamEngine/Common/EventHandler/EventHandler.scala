package com.pharbers.StreamEngine.Common.EventHandler

import com.pharbers.StreamEngine.Common.Events

trait EventHandler {
    def exec(e: Events): Unit
}
