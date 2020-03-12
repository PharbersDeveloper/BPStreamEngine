#!/usr/bin/python
# -*- coding: UTF-8 -*-

import json
import sys
import os

base_path = os.path.dirname(os.path.dirname(
    os.path.abspath(__file__)))
sys.path.append(os.getcwd())

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
            "tag": self.tag,
            "errMsg": self.errMsg
        }
        return json.dumps(result, ensure_ascii=False)
