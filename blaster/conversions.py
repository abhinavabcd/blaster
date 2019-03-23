#coding=utf-8
import types
import datetime


#######protobuf based

def parse_list(values,message):
    '''parse list to protobuf message'''
    if not values:
        return
    if isinstance(values[0],dict):#value needs to be further parsed
        for v in values:
            cmd = message.add()
            dict_to_obj(v,cmd)
    else:#value can be set
        message.extend(values)


#just like move constructor ;) , move values much faster than copies
def dict_to_obj(values,obj,transformations=None, excludes=None, preserve_values=True):
    if(not preserve_values):
        if(transformations):
            for k,func in transformations.items():
                values[k] = func(values.get(k,None))
                
        if(excludes):
            for exclude, flag in excludes.items():
                if(hasattr(values, exclude)):
                    del values[exclude]
             
    for k,v in values.items():
 
        if(preserve_values):
            if(transformations and k in transformations):
                v = transformations[k](v)
                            
            if(excludes and k in excludes):
                continue

        if hasattr(obj, k):
            if isinstance(v,dict):#value needs to be further parsed
                dict_to_obj(v,getattr(obj,k))
            elif isinstance(v,list):
                parse_list(v,getattr(obj,k))
            else:#value can be set
                if v:#otherwise default
                    setattr(obj, k, v)



def dict_to_protobuf(value,message):
    dict_to_obj(value,message)

######general utils




# def get_mysql_rows_as_dict(res, as_batch=None):
#     
#     rows_as_dict = []
#     p = 0
#     for row in res.rows:
#         as_dict = {}
#         for field, val in zip(res.fields, row):
#             as_dict[field[0]] =  val
#         
#             
#             
#         
#         rows_as_dict.append(as_dict)
#         n = len(rows_as_dict)
#         if(as_batch!=None and n-p>=as_batch):
#             if(as_batch==1):
#                 yield rows_as_dict[0]
#             else:
#                 yield rows_as_dict
#                                 
#             p = n
#             rows_as_dict = []
#     
#     if(as_batch==1):
#         if(rows_as_dict):
#             yield rows_as_dict[0]
#         return
#     
#     yield rows_as_dict#LIKE RETURN ALL?
#     return

def get_mysql_rows_as_dict(res):
    
    rows_as_dict = []
    for row in res.rows:
        as_dict = {}
        for field, val in zip(res.fields, row):
            as_dict[field[0]] =  val
        
        
        rows_as_dict.append(as_dict)
    
    return rows_as_dict
