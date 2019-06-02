'''
Created on 04-Nov-2017

@author: abhinav
'''

import collections
import random
import string
import socket
import struct
from gevent.lock import BoundedSemaphore
from .connection_pool import use_connection_pool
import os
from collections import namedtuple
from datetime import datetime
import logging
import urllib.request, urllib.parse, urllib.error
from . import config 
import time
import hmac
import base64
import hashlib
import threading
from blaster.websocket._core import WebSocket
from blaster.urllib_utils import get_data
from gevent.threading import Lock
import re
from blaster.config import IS_DEBUG
from _datetime import timezone
import six


EPOCH = datetime(1970, 1, 1)



def cur_ms():
    return int(1000*time.time())
#random id

max_assumed_sent_buffer_time = 20*1000 #milli seconds # TCP_USER_TIMEOUT kernel setting

def set_socket_options(sock):    
    l_onoff = 1                                                                                                                                                           
    l_linger = 10 # seconds,                                                                                                                                                     
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER,                                                                                                                     
                 struct.pack('ii', l_onoff, l_linger))# close means a close understand ? 
    
    
    
    timeval = struct.pack('ll', 100, 0)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDTIMEO, timeval)

    TCP_USER_TIMEOUT = 18
    sock.setsockopt(socket.SOL_TCP, TCP_USER_TIMEOUT, max_assumed_sent_buffer_time)# close means a close understand ? 

    sock.setdefaulttimeout(60) #every 1 min timeout

class LRUCache:
    def __init__(self, capacity):
        self.capacity = capacity
        self.cache = collections.OrderedDict()

    def get(self, key, default=None):
        try:
            value = self.cache.pop(key)
            self.cache[key] = value
            return value
        except KeyError:
            return default

    def set(self, key, value):
        removed_entries = []
        try:
            self.cache.pop(key)
        except KeyError:
            while(len(self.cache) >= self.capacity):
                removed_entries.append(self.cache.popitem(last=False))
        self.cache[key] = value
        
        return removed_entries
        
    def delete(self, key):        
        return self.cache.pop(key, None)
    
    
    def to_son(self):
        ret = {}
        for key, val in self.cache:
            ret[key] =   val.to_son() if hasattr(val ,"to_son") else val
        return ret

#get SON of the fields from any generic python object
def to_son(obj):
    ret = obj.__dict__
    for k in ret.keys():
        if(ret[k]==None):
            del ret[k]            
    return ret


def from_kwargs(cls, **kwargs):
    ret = cls()
    for key in kwargs:
        setattr(ret, key, kwargs[key])
    return ret

def get_by_key_list(d , *keyList):
    for key in keyList:
        if(not d):
            return None
        d= d.get(key,None)
    return d

def date2string(date):
    return date.isoformat()

def date2timestamp(dt):
    # datetime to timestamp
    if not isinstance(dt, datetime):
        return dt
    if(six.PY34):
        return dt.replace(tzinfo=timezone.utc).timestamp()
    else:
        timestamp = time.mktime(dt.timetuple()) + dt.microsecond/1e6
        return timestamp

def timestamp2date(timestamp):
    if not isinstance(timestamp, (int, float)):
        return timestamp
    date = datetime.utcfromtimestamp(timestamp)
    return date

###############EPOCH
EPOCH = timestamp2date(0)




def find_index(a_list, value):
    try:
        return a_list.index(value)
    except ValueError:
        return -1



SOME_TIME_WHEN_WE_STARTED_THIS = 1471504855

__thread_data = LRUCache(10000)
def generate_64bit_key(shard_id):#shard id should be a 12 bit number
    # may be later use dattime
    millis_elapsed = int((time.time()-SOME_TIME_WHEN_WE_STARTED_THIS)*1000)
    
    _id = ( ((1 << 41) - 1)  & millis_elapsed) << 23 #41 bits , clear 22 places for random id and shard_id
    
    _id |=  (  ((1<<12)-1) & shard_id  ) << 11 # 12 bit shard id, on top
    

    #random number
    thread_id = threading.current_thread().ident
    thread_data = __thread_data.get(thread_id, None)
    if(not thread_data):
        thread_data = [0]
        __thread_data.set(thread_id , thread_data)
    thread_data[0] = (thread_data[0]+1)%(1<<11)
    
    _id |=  (((1<<11)-1) & thread_data[0]) #clear 12 places for random

    #shard_id|timestmap|random_number
    
    return _id


def get_shard_id(_id):
    return (int(_id) & ( ((1<<12)-1) << 11 )) >> 11
    
    
## move it to seperate module ?
## copied from internet sha1 token encode - decode module

if hasattr(hmac, 'compare_digest'):  # python 3.3
    _time_independent_equals = hmac.compare_digest
else:
    def _time_independent_equals(a, b):
        if len(a) != len(b):
            return False
        result = 0
        if isinstance(a[0], int):  # python3 byte strings
            for x, y in zip(a, b):
                result |= x ^ y
        else:  # python2
            for x, y in zip(a, b):
                result |= ord(x) ^ ord(y)
        return result == 0


def utf8(value)->bytes:
    if(isinstance(value, bytes)):
        return value
    return value.encode("utf-8")

def create_signed_value(name, value, secret_key_version="v0"):
    timestamp = utf8(str(int(time.time())))
    value = base64.b64encode(utf8(value))
    secret = config.secrets.get(secret_key_version)
    signature = _create_signature(secret, name, value, timestamp)
    value = b"|".join([value, timestamp, signature, utf8(secret_key_version)])
    return value

def decode_signed_value(name, value, max_age_days=-1):
    if not value:
        return None
    parts = utf8(value).split(b"|")
    secret_key_version = b"v0"
    if len(parts) == 4:
        secret_key_version = parts[3]
    if(len(parts)<3):
        return None
    secret = config.secrets.get(secret_key_version.decode())
    signature = _create_signature(secret, name, parts[0], parts[1])
    if not _time_independent_equals(parts[2], signature):
        return None
    if(max_age_days>0):
        timestamp = int(parts[1])
        if timestamp < time.time() - max_age_days * 86400:
            print (-1, cur_ms(), "Expired cookie %s"%value)
            return None
        if timestamp > time.time() + 31 * 86400:
            # _cookie_signature does not hash a delimiter between the
            # parts of the cookie, so an attacker could transfer trailing
            # digits from the payload to the timestamp without altering the
            # signature.  For backwards compatibility, sanity-check timestamp
            # here instead of modifying _cookie_signature.
            print (-1, cur_ms(), "Cookie timestamp in future; possible tampering %s"%value)
            return None
    if parts[1].startswith(b"0"):
        logging.warning("Tampered cookie %r", value)
        return None
    try:
        return base64.b64decode(parts[0])
    except Exception:
        return None


def _create_signature(secret, *parts):
    hash = hmac.new(utf8(secret), digestmod=hashlib.sha1)
    for part in parts:
        hash.update(utf8(part))
    return utf8(hash.hexdigest())



# Console or Cloud Console.
def get_random_id(length=10, include_timestamp=True):
    '''returns a 10 character random string containing numbers lowercase upper case'''
    '''http://stackoverflow.com/questions/2257441/random-string-generation-with-upper-case-letters-and-digits-in-python'''

    key_str = ''.join(random.SystemRandom().choice(string.ascii_uppercase + string.digits+string.ascii_lowercase) for _ in range(length)) 
    if(include_timestamp):
        key_str = key_str +  ("%d" % time.time())
    #key_str = hashlib.md5(key_str).hexdigest()
    return key_str


number_regex = re.compile(r"([0-9\.]+)" )
non_alpha_regex = re.compile("[^0-9a-zA-Z \.]", re.DOTALL)
def sanitize_string(text):
    return non_alpha_regex.sub("", text)


non_alpha_regex_2 = re.compile("[^0-9a-zA-Z]", re.DOTALL)
def sanitize_to_id(text):
    return non_alpha_regex_2.sub("", text.lower())

email_regex = re.compile('^[_a-z0-9-]+(\.[_a-z0-9-]+)*@[a-z0-9-]+(\.[a-z0-9-]+)*(\.[a-z]{2,4})$')
def is_valid_email(email):
    return email_regex.match(email)
    

def list_diff(first, second):
    if(not first or not second):
        return first
    second = set(second)
    return [item for item in first if item not in second]


@use_connection_pool(ses_client="ses")
def send_email(sender, to_list, subject, body_text=None, body_html=None, ses_client=None):
    """
    Send email.
    Note: The emails of sender and receiver should be verified.
    PARAMS
    @sender: sender's email, string
    @to: list of receipient emails eg ['a@b.com', 'c@d.com']
    @subject: subject of the email
    @body: body of the email
    """
    try:
        body_data = {}
        if(body_text):
            body_data['Text'] = {
                'Data': body_text,
                'Charset': 'UTF-8'
            }
        if(body_html):
            body_data['Html'] = {
                'Data': body_html,
                'Charset': 'UTF-8'
            }
        
        response = ses_client.send_email(
            Source=sender,
            Destination={
                'ToAddresses': to_list
            },
            Message={
                'Subject': {
                    'Data': subject,
                    'Charset': 'UTF-8'
                },
                'Body': body_data
            }
        )
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            return True
        else:
            return False
    except Exception as ex:
        return False

####################################websocket connection and send function , gevent compatible
#gevent _GetPrecompileRelatedFiles
class Connection(WebSocket):
    queue = None# to keep track of how many greenlets are waiting on semaphore to send 
    msg_assumed_sent = None# queue for older sent messages in case of reset we try to retransmit
    ws = None
    lock = BoundedSemaphore()
    is_stale = False
    last_msg_recv_timestamp = None
    last_msg_sent_timestamp = None
    
    
    user_id = None
    is_viewer_list = False
    
    def __init__(self, ws, user_id):
        self.ws = ws
        self.queue = collections.deque()# to keep track of how many greenlets are waiting on semaphore to send 
        self.msg_assumed_sent = collections.deque()# queue for older sent messages in case of reset we try to retransmit
        self.user_id = user_id
   
        self.last_msg_recv_timestamp = self.last_msg_sent_timestamp = time.time()*1000
        
        
    def send(self, msg, ref=None,add_to_assumend_sent=True): # msg is only string data , #ref is used , just in case an exception occurs , we pass that ref 
        if(self.is_stale):
            raise Exception("stale connection")
        
        self.queue.append((ref, msg))
        if(self.lock.locked()):
            return
        
        self.lock.acquire()
        data_ref = None
        data = None
        try:
            while(not self.is_stale and len(self.queue)>0):
                data_ref, data = self.queue.popleft() #peek
                self.ws.send(data) # msg objects only
                current_timestamp = time.time()*1000
                self.last_msg_sent_timestamp = current_timestamp
                if(add_to_assumend_sent):
                    while(len(self.msg_assumed_sent)>0 and self.msg_assumed_sent[0][0]<current_timestamp-max_assumed_sent_buffer_time):
                        #keep inly 100 seconds of previous data
                        self.msg_assumed_sent.popleft()
                    
                    self.msg_assumed_sent.append((current_timestamp , data_ref, data))
                
            
        except  Exception as ex:
            err_msg  = "Exception while sending message to %s might be closed "%(self.user_id)
            self.is_stale = True
            raise Exception(err_msg)
            
        finally:
            self.lock.release()
        return 
    
    
#dynamo db shit


######upload shit

TypeDescriptor = namedtuple('TypeDescriptor', ['mime_type'])
MIME_TYPE_MAP = dict(
    gif=TypeDescriptor(mime_type = 'image/gif'),
    webp=TypeDescriptor(mime_type = 'image/webp'),
    mp4=TypeDescriptor(mime_type = 'video/mp4'),

    jpeg=TypeDescriptor(mime_type = 'image/jpeg'),
    jpg=TypeDescriptor(mime_type = 'image/jpeg'),
    png=TypeDescriptor(mime_type = 'image/png'),
    json=TypeDescriptor(mime_type = 'application/json'),
    zip=TypeDescriptor(mime_type = 'application/zip'),
    mp3=TypeDescriptor(mime_type = 'audio/mpeg'),
    ogg=TypeDescriptor(mime_type = 'audio/ogg'),
    opus=TypeDescriptor(mime_type = 'audio/ogg'),
    wav=TypeDescriptor(mime_type = 'audio/wav'),
    m4a=TypeDescriptor(mime_type = 'audio/mp4'),
    aac=TypeDescriptor(mime_type = 'audio/aac'),
    docx = TypeDescriptor(mime_type = 'application/vnd.openxmlformats-officedocument.wordprocessingml.document'),
    pdf = TypeDescriptor(mime_type = 'application/pdf'),

    # variables should not start numerals
    gp=TypeDescriptor(mime_type = 'audio/3gp'),
    amr=TypeDescriptor(mime_type = 'audio/amr'),
    wma=TypeDescriptor(mime_type = 'audio/wma'),
    gpp=TypeDescriptor(mime_type = 'audio/3gpp'),
    flac=TypeDescriptor(mime_type = 'audio/flac'),
    
    css=TypeDescriptor(mime_type = 'text/css'),
    js=TypeDescriptor(mime_type = 'text/javascript')
    
)

MIME_TYPE_TO_EXTENSION = {v.mime_type: k for k, v in MIME_TYPE_MAP.items()}


def get_mime_type_from_filename(file_name):
    ext = os.path.splitext(file_name)[1][1:]
    return MIME_TYPE_MAP.get(ext, None)


@use_connection_pool(s3="s3")
def generate_upload_url(file_name, base_folder, s3=None , bucket=None, redirect_url=None, mime_type=None):
    if(not mime_type):
        #just in case
        extension = os.path.splitext(file_name)[1][1:]
        mime_type = MIME_TYPE_MAP.get(extension)

    fields = {"acl": "public-read", "Content-Type": mime_type, "success_action_status": "200"}

    conditions = [
        {"acl": "public-read"},
        {"Content-Type": mime_type},
        {"success_action_status": "200"}
    ]
    if(redirect_url):
        conditions.append({"redirect": redirect_url})


    # Generate the POST attributes
    s3_key = base_folder + "/" + file_name
    post = s3.meta.client.generate_presigned_post(
        Bucket=bucket,
        Key=s3_key,
        Fields=fields,
        Conditions=conditions,
        ExpiresIn=604800
    )
    return post












######################gevent handlers

####file handler
def static_file_handler(_base_folder_path_, default_file_path="index.html"):
    cached_file_data = {}
    gevent_lock = Lock()
    def file_handler(sock, path, query_params=None, headers=None, post_data=None):
        if(not path):
            path = default_file_path
        #from given base_path
        path = _base_folder_path_+str(path)
        file_data = cached_file_data.get(path, None)
        resp_headers = None

        if(not file_data or time.time()*1000 - file_data[2] > 1000 if IS_DEBUG else 2*60*1000): #1 millis
            gevent_lock.acquire()
            file_data = cached_file_data.get(path, None)
            if(not file_data or time.time()*1000 - file_data[2] > 1000): #1 millis
                #put a lock here
                try:
                    data = open(path,"rb").read()
                    mime_type_headers = get_mime_type_from_filename(path)
                    if(mime_type_headers):
                        resp_headers = ['Content-Type: %s'%(mime_type_headers.mime_type,)]
                    
                    file_data = (data, resp_headers, time.time()*1000)
                    cached_file_data[path] = file_data
                except Exception as ex:
                    file_data = ("--NO-FILE--", None, time.time()*1000)
                    
            gevent_lock.release()
                    
        data, resp_headers, last_updated = file_data
        return resp_headers, data
    
    return file_handler






