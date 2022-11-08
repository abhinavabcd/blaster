import timeit

import_module = '''from io import BytesIO'''
code = '''
a = bytearray()
i = 0
while(i < 100000):
	a.extend(b'abcd')
	i+=1
'''
code2 = '''
a = ''
i = 0
while(i < 100000):
	a += 'abcd'
	i+=1
'''
code3 = '''
a = b''
i = 0
while(i < 100000):
	a += b'abcd'
	i+=1
'''
code4 = '''
a = BytesIO()
i = 0
while(i < 100000):
	a.write(b'abcd')
	i+=1
a = a.getvalue()
'''

print(timeit.repeat(stmt=code, repeat=5, number=100))
print(timeit.repeat(stmt=code2, repeat=5, number=100))
#print(timeit.repeat(stmt=code3, repeat=5, number=100))
print(timeit.repeat(stmt=code4, setup=import_module, repeat=5, number=100))
