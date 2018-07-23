'''
Created on May 11, 2016

@author: abhinav
'''
import urllib2
import zlib
from StringIO import StringIO
import hashlib
import os
from Queue import Queue
import time
import ujson as json


url_loaders_queue = Queue()


def get_url_loader(actual_func):
    
    def ret_func(*args, **kwargs):
        url_loader = None
        try:
            url_loader = url_loaders_queue.get(block=False)
        except Exception as ex:
            http_logger = urllib2.HTTPHandler(debuglevel = 0)
            https_logger = urllib2.HTTPSHandler(debuglevel = 0)
            url_loader=urllib2.build_opener(http_logger,urllib2.HTTPCookieProcessor(),urllib2.ProxyHandler(),https_logger, urllib2.HTTPRedirectHandler())
            urllib2.install_opener(url_loader)
        
        if(kwargs==None):
            kwargs = {}
        kwargs["url_loader"] = url_loader
        ret = actual_func(*args, **kwargs)
        url_loaders_queue.put(url_loader)
        
        return ret
        
    return ret_func



@get_url_loader
def get_data(url,post=None,headers={}, method = None, url_loader=None):
    headers['Accept-encoding'] ='gzip'
    ret= None
    try:
        req=urllib2.Request(url,post,headers)
        if(method!=None):
            req.get_method = lambda : method
        ret = url_loader.open(req) 
        if ret.info().get('Content-Encoding') == 'gzip':
            ret2 = ret
            try:
                ret = StringIO(zlib.decompress(ret2.read(),16+zlib.MAX_WBITS))
            except:
                decompressor = zlib.decompressobj()
                ret = StringIO(decompressor.decompress(ret2.read()))
            ret2.close()
            
    except urllib2.HTTPError as e: 
        ret = None
        print(-4 ,  int(time.time()*1000), json.dumps(data=e.read()))
    return ret


if __name__ == "__main__":
    for i in range(10):
        print(get_data("http://WWW.google.com"))
        