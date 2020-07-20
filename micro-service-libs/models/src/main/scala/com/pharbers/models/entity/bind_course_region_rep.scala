package com.pharbers.models.entity

import com.pharbers.macros.api.commonEntity
import com.pharbers.macros.common.connecting.ToStringMacro

@ToStringMacro
class bind_course_region_rep() extends commonEntity {
    var course_id: String = ""
    var region_id: String = ""
    var rep_id: String = ""
}