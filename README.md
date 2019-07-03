# High performance gevent based python3 server with minimal string copy operations.
Very simplistic API, Best suited for API servers, websocket handling. Fastest in the python town you could say ; ). 


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

