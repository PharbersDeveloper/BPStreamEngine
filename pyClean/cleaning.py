#!/usr/bin/python
# -*- coding: UTF-8 -*-

import string
import mapping
from results import ResultModel
from results import ResultTag

import sys

reload(sys)
sys.setdefaultencoding('utf8')


def create_mapping(source):
    if source.upper() == "CPA":
        return mapping.cpa_gyc_mapping()
    elif source.upper() == "GYCX" or source.upper() == "GYC":
        return mapping.cpa_gyc_mapping()
    elif source.upper() == "CHC":
        return mapping.chc_mapping()
    else:
        return mapping.cpa_gyc_mapping()


def process(event):
    reval = {}  # 保存结果

    # 1. Simple Conversion
    old_data = event["data"]
    metadata = event["metadata"]
    providers = metadata.get("providers", [])

    source = providers[1:2]
    if not len(source):
        source = "DEFAULT_SOURCE"
    else:
        source = source[0]

    company = providers[0:1]
    if not len(company):
        company = "DEFAULT_COMPANY"
    else:
        company = company[0]

    reval["COMPANY"] = company
    reval["SOURCE"] = source

    # 2. Create an col name mapping table
    mapping = create_mapping(source)

    # 3. Change data col name
    for m in mapping:
        for old_key in old_data.keys():
            compare_key = string.split(old_key, "#")[1]
            if compare_key.upper().strip() in m["Candidate"]:
                reval[m["ColName"]] = old_data[old_key]
                break
            elif reval.get(m["ColName"], None) is not None:
                break
            else:
                reval[m["ColName"]] = None

    # 4. Check Reval Completeness
    def get_true_value(m, value):
        if m["Type"] is "String":
            if value is None:
                return ""
            else:
                return value
        elif m["Type"] is "Integer":
            if value is None:
                return 0
            else:
                return int(value)
        elif m["Type"] is "Double":
            if value is None:
                return 0.0
            else:
                return float(value)

    for m in mapping:
        value = reval[m["ColName"]]
        if m.get("NotNull", False) is False:
            reval[m["ColName"]] = get_true_value(m, value)
        elif m.get("NotNull", False) is True and value is not None:
            reval[m["ColName"]] = get_true_value(m, value)
        else:
            raise Exception(m["ColName"] + " is None，Please check the file or completion mapping rules")

    # 5. Change metadata schema
    schema = []
    for m in mapping:
        schema.append({"key": m["ColName"], "type": m["Type"]})
    metadata["schema"] = schema

    result = ResultModel(
        data=reval,
        metadata=metadata,
        tag=ResultTag.Success
    )

    return [result]
