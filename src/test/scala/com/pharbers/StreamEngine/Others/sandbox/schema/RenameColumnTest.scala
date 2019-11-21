package com.pharbers.StreamEngine.Others.sandbox.schema

import com.pharbers.StreamEngine.Jobs.SandBoxJob.SchemaConverter
import org.scalatest.FunSuite

class RenameColumnTest extends FunSuite{
	test("rename column") {
		val jsonStr = """[{"key": "城市","type": "String"},{"key": "城市","type": "String"},{"key": "年月","type": "String"}]"""
		
		SchemaConverter.renameColumn(jsonStr)
	}
}
