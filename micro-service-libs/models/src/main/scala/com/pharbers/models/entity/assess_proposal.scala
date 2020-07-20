package com.pharbers.models.entity
import com.pharbers.macros.api.commonEntity
import com.pharbers.macros.common.connecting.ToStringMacro

@ToStringMacro
class assess_proposal extends commonEntity  {
    var proposal_id: String = ""
    var timestamp: Int = 0
    var data: List[Map[String, String]] = Nil
}