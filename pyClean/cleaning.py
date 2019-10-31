#!/usr/bin/python
# -*- coding: UTF-8 -*-

from mapping import create_mapping

def process(event):
    # 0. Create an col name mapping table
    mapping = create_mapping()

    reval = {}
    # 1. Change data col name
    for key in event.keys():
        for m in mapping:
            if key in m["Candidate"]:
                reval[m["ColName"]] = event[key]

    # 2. 返回打包, 只保留固定的Col
    # for v in mapping:
    #     if v not in reval.keys():
    #         if v["Type"] is "String":
    #             reval[v] = ""
    #         else:
    #             reval[v] = 0

    # event["data"] = reval
    event = reval

    # result = json.dumps(event, ensure_ascii=False)
    return event
