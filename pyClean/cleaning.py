#!/usr/bin/python
# -*- coding: UTF-8 -*-

from mapping import create_mapping
from results import ResultModel
from results import ResultTag


def process(event):
    # 0. Create an col name mapping table
    mapping = create_mapping()

    reval = {}

    # 1. Change data col name
    old_data = event
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
        else :
            if value is None:
                reval[m["ColName"]] = 0.0
            else:
                reval[m["ColName"]] = float(value)

    metadata = {}  # arg["_metadata"]
    result = ResultModel(
        data=reval,
        metadata=metadata,
        tag=ResultTag.Success
    )

    return [result]
