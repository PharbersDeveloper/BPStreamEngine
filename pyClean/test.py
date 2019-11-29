#!/usr/bin/python
# -*- coding: UTF-8 -*-

import os
import sys
import json

from results import ResultTag
from results import ResultModel
from cleaning import process


if __name__ == "__main__":
    try:
        message = sys.argv[1].decode("UTF-8", errors="ignore")
        event = json.loads(message)
        for item in process(event):
            print(item.toJson())
    except:
        import traceback
        exType, exValue, exTrace = sys.exc_info()
        print(exValue)
        # print(ResultModel(
        #     data=message,
        #     tag=ResultTag.Error,
        #     errMsg=str(repr(traceback.format_exception(exType, exValue, exTrace)))
        # ).toJson())
