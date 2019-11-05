#!/usr/bin/python
# -*- coding: UTF-8 -*-

import sys
import json

reload(sys)
sys.setdefaultencoding('utf-8')


class ResultTag(object):
    Error = -1
    Keep = 0
    Success = 1


class ResultModel(object):
    data = {}
    _metadata = {}
    tag = ResultTag.Error
    errMsg = {}

    def __init__(self, data={}, metadata={}, tag=ResultTag.Error, errMsg=""):
        self.data = data
        self._metadata = metadata
        self.tag = tag
        self.errMsg = errMsg

    def toJson(self):
        result = {
            "data": self.data,
            "metadata": self._metadata,
            "tag": self.tag.value,
            "errMsg": self.errMsg
        }
        return json.dumps(result, ensure_ascii=False)


def main(argv):
    metadata = argv["_metadata"]
    data = argv["data"]
    data["addTest"] = "哎呦"
    result = ResultModel(
        data=data,
        metadata=metadata,
        tag=ResultTag.Success
    )
    return [result]


if __name__ == '__main__':
    try:
        input = sys.argv[1].decode('UTF-8', errors="ignore")
        input = json.loads(input)
        for item in main(input):
            print(item.toJson())
    except:
        import traceback

        exType, exValue, exTrace = sys.exc_info()
        print(ResultModel(
            tag=ResultTag.Error,
            errMsg=str(repr(traceback.format_exception(exType, exValue, exTrace)))
        ).toJson())
