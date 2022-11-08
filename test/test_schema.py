from blaster.schema import schema, Object, List, Str, Int, Optional
import json


class Test2(Object):
	a: List[int]
	b: Int
	c: Str(format="datetime")
	d: Optional[Str(format="datetimez")]
	e: Str(maxlen=9)

	def __init__(self):
		self.a = ["string"]
		self.b = 100
		self.c = "2021-10-08 14:39:56.621851"
		self.e = "somerandomstring"


test2_schema, test2_validator = schema(Test2)
print(json.dumps(test2_schema, indent=4))

obj = Test2()
print(obj.__dict__)
test2_validator(obj)
print(obj.__dict__)
