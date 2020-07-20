package com.pharbers.models.entity

import com.pharbers.macros.api.commonEntity
import com.pharbers.macros.common.connecting.One2OneConn
import com.pharbers.models.service.paperinputstep

@One2OneConn[paperinputstep]("paperinputstep")
class paperinput() extends commonEntity {
    var paper_id: String = ""
    var region_id: String = ""
    var hint: String = ""
    var sorting: String = ""
    var predicted_target: Long = -1L
    var field_work_days: Int = -1
    var national_meeting: Int = -1
    var city_meeting: Int = -1
    var depart_meeting: Int = -1
    var action_plans: List[String] = Nil
}