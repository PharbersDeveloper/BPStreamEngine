#!/usr/bin/python
# -*- coding: UTF-8 -*-

import string
from mapping import create_mapping
from results import ResultModel
from results import ResultTag


def process(event):
    # 0. Create an col name mapping table
    mapping = create_mapping()

    old_data = event["data"]
    metadata = event["metadata"]

    file_name = string.split(metadata["fileName"], ".")[0]
    file_name = string.split(file_name, "_")
    file_company = file_name[0]
    file_source = file_name[len(file_name)-1]

    # 1. Change data col name
    reval = {}
    for m in mapping:
        value = None
        for old_key in old_data.keys():
            if old_key.upper() in m["Candidate"]:
                value = old_data[old_key]
                break

        if m["Type"] is "String":
            if value is None:
                reval[m["ColName"]] = ""
            else:
                reval[m["ColName"]] = value
        elif m["Type"] is "Integer":
            if value is None:
                reval[m["ColName"]] = 0
            else:
                reval[m["ColName"]] = int(value)
        elif m["Type"] is "Double":
            if value is None:
                reval[m["ColName"]] = 0.0
            else:
                reval[m["ColName"]] = float(value)
    reval["COMPANY"] = file_company
    reval["SOURCE"] = file_source

    # 2. Change metadata schema
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
