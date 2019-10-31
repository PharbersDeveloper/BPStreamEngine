#!/usr/bin/python
# -*- coding: UTF-8 -*-

import sys
import json

reload(sys)
sys.setdefaultencoding('utf-8')

from results import ResultTag
from results import ResultModel
from cleaning import process

if __name__ == "__main__":
    # print("Pharbers Data Cleaning")

    # print("1. need check the context auth right")
    # pbs_auth_checking()

    # print("2. need continue with cleaning process ")
    try:
        input = sys.argv[1].decode("UTF-8", errors="ignore")
        event = json.loads(input)
        metadata = {}  # arg["_metadata"]
        result = process(event)
        result = ResultModel(
            data=result,
            metadata=metadata,
            tag=ResultTag.Success
        )
        print(result.toJson())
    except:
        import traceback

        exType, exValue, exTrace = sys.exc_info()
        print(ResultModel(
            tag=ResultTag.Error,
            errMsg=str(repr(traceback.format_exception(exType, exValue, exTrace)))
        ).toJson())

    # print("3. return data to data engine")
