package com.pharbers.models.entity

import com.pharbers.macros.api.commonEntity
import com.pharbers.macros.common.connecting.ToStringMacro

@ToStringMacro
class examrequire() extends commonEntity {
    var field_work_days: Int = 0
    var national_meeting: Int = 0
    var city_meeting: Int = 0
    var depart_meeting: Int = 0
}