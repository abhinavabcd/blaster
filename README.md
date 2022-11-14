# High performance gevent based python3 server.
Very simplistic API, Best suited for API servers, websocket, realtime games, chat. 
Generates autoapi docs and packed with a bunch of useful utilities.

schema generator and validation in 200 lines.

#### Schema generator and validation (inplace) 

```python

class Test2(Object):
	a: Array(int)
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
print(obj.__dict)

```

Let's build few cross platform applications

Example 1: (PasteBin)
- Create a paste, with some text, restrict to users, password, file upload


```python

import blaster # should be first line, does the gevent monkey patch
from blaster.server import route
from blaster.schema import Object

class CreatePasteRequest(Object):
	title: Str(maxlen=100)
	content: Str
	user_ids: Optional[Array(str)]
	password: Optional[Str(maxlen=100)]
	upload: Optional[Str(format=binary)] #this will actually be a dict {"data": .., "headers": .., "attrs": ...}



class PasteResponse(Object):
	title: Str(maxlen=100)
	content: Str
	created_by: Str
	password: Optional[Str]
	


@route("/create", methods=POST)
def create(req: CreatePasteRequest) -> PasteResponse:   #give the argument a type and it's available to you, check all available types
	if(req.upload):
		pass




```



See examples in the examples folder


# Additional Utilitites:
## AWS - send email via SES
## AWS - Push tasks ( Defered processing function calls using sqs)
## AWS - DynamoDb orm - blaster/aws_utils/dynamix
## A 300 line mongoorm that support 
   - supports client level sharding using jumphash, but you might never need it.






# Apache Ab simple hello world HTML results:
- export PYTHONPATH=$PYTHONPATH:$PWD/blaster
- python examples/simple_examples.py 
- ab -k -c 100 -n 10000 http://localhost:8081/test
- ab -k -c 100 -n 10000 http://localhost:8081/index.html 

```
Concurrency Level:      100
Time taken for tests:   0.995 seconds
Complete requests:      10000
Failed requests:        0
Keep-Alive requests:    0
Total transferred:      1330000 bytes
HTML transferred:       370000 bytes
Requests per second:    10050.57 [#/sec] (mean)
Time per request:       9.950 [ms] (mean)
Time per request:       0.099 [ms] (mean, across all concurrent requests)
Transfer rate:          1305.40 [Kbytes/sec] received
```

