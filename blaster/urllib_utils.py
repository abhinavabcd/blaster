'''
Created on May 11, 2016

@author: abhinav
'''
import urllib.request
import urllib.error
import urllib.parse
import zlib
from io import StringIO, BytesIO
import hashlib
import os
import re
from queue import Queue
import time
import ujson as json
from .config import IS_DEV
#enable any header format
import http.client
http.client._is_legal_header_name = re.compile(rb'[^\s][^:\r\n]*').fullmatch

url_loaders_queue = Queue()

class OpenerDirector2(urllib.request.OpenerDirector):
    def __init__(self):
        super().__init__()
        self.addheaders = [] # remove default python-urllib-header


urllib.request.OpenerDirector = OpenerDirector2 # override for everyone

def get_url_loader(actual_func):
    
    def ret_func(*args, **kwargs):
        url_loader = None
        try:
            url_loader = url_loaders_queue.get(block=False)
        except Exception as ex:
            http_logger = urllib.request.HTTPHandler(debuglevel=(1 if IS_DEV else 0))
            https_logger = urllib.request.HTTPSHandler(debuglevel=(1 if IS_DEV else 0))
            url_loader = urllib.request.build_opener(http_logger, urllib.request.HTTPCookieProcessor(), urllib.request.ProxyHandler(), https_logger, urllib.request.HTTPRedirectHandler())
            urllib.request.install_opener(url_loader)
        
        if(kwargs == None):
            kwargs = {}
        kwargs["url_loader"] = url_loader
        ret = actual_func(*args, **kwargs)
        url_loaders_queue.put(url_loader)
        
        return ret
        
    return ret_func


'''returns Io'''
@get_url_loader
def get_data(url, post=None, headers=None, method=None, url_loader=None, as_string_buffer=True):
    headers = headers or {}
    headers['Accept-encoding'] = 'gzip'
    ret = None
    try:
        if(post):
            if(isinstance(post, (list, dict))):
                post = json.dumps(post)
            if(isinstance(post, str)):
                post = post.encode()
        req = urllib.request.Request(url, data=post, headers=headers)
        if(method != None):
            req.get_method = lambda : method
        ret = url_loader.open(req)
        if ret.info().get('Content-Encoding') == 'gzip':
            ret2 = ret
            try:
                ret = BytesIO(zlib.decompress(ret2.read(), 16 + zlib.MAX_WBITS))
            except Exception as ex:
                decompressor = zlib.decompressobj()
                ret = BytesIO(decompressor.decompress(ret2.read()))
            ret2.close()
        #return as string
        if(as_string_buffer):
            return StringIO(ret.read().decode())
            
    except urllib.error.HTTPError as e:
        ret = None
        err_body = e.read()
        print("error_http_fetch", int(time.time() * 1000), json.dumps({"err": str(e), "url": url, "body": err_body.decode() if err_body else ""}))
    except Exception as e:
        ret = None
        print("error_http_fetch_error" , int(time.time() * 1000), json.dumps({"err": str(e), "url": url}))
    return ret


def get_data_cached(url, post=None, headers={}, method=None, cache=True, ignore_cache_read=False, cache_folder="/tmp/", as_string_buffer=True):
    cache_hash = url
    if(post):
        cache_hash += json.dumps(post)
    cache_hash = hashlib.md5(cache_hash.encode("utf-8")).hexdigest()
    if(cache and not ignore_cache_read):
        if(os.path.isfile(cache_folder + cache_hash)):
            #print "reading from cache"
            bytes_buffer = BytesIO(open(cache_folder + cache_hash, "rb").read())
            if(as_string_buffer):
                return StringIO(bytes_buffer.read().decode())
            else:
                return bytes_buffer

    _resp = get_data(url, post=post, headers=headers, as_string_buffer=False)
    if(_resp):
        _data = _resp.read()
        if(cache):
            open(cache_folder + cache_hash, "wb").write(_data)
        if(as_string_buffer):
            return StringIO(_data.decode())
        return BytesIO(_data)
    return None
        
            

if __name__ == "__main__":
    for i in range(10):
        print((get_data("http://WWW.google.com")))
