#! -*- coding: utf-8 -*-


import copy
from blaster.connection_pool import use_connection_pool

from .table import Table
from .fields import Fields
from .errors import FieldValidationException
import ujson as json


class Paginator(object):

    def __init__(self, model_object):
        self.model_object = model_object

    @use_connection_pool(db="dynamodb")
    def query(self, db=None, **kwargs):
        '''
        response_iterator = paginator.paginate(
            TableName='string',
            IndexName='string',
            Select='ALL_ATTRIBUTES'|'ALL_PROJECTED_ATTRIBUTES'|'SPECIFIC_ATTRIBUTES'|'COUNT',
            ConsistentRead=True|False,
            ScanIndexForward=True|False,
            ReturnConsumedCapacity='INDEXES'|'TOTAL'|'NONE',
            ProjectionExpression='string',
            FilterExpression='string',
            KeyConditionExpression='string',
            ExpressionAttributeNames={
                'string': 'string'
            },
            ExpressionAttributeValues={
                'string': {
                    'S': 'string',
                    'N': 'string',
                    'B': b'bytes',
                    'SS': [
                        'string',
                    ],
                    'NS': [
                        'string',
                    ],
                    'BS': [
                        b'bytes',
                    ],
                    'M': {
                        'string': {'... recursive ...'}
                    },
                    'L': [
                        {'... recursive ...'},
                    ],
                    'NULL': True|False,
                    'BOOL': True|False
                }
            },
            PaginationConfig={
                'MaxItems': 123,
                'PageSize': 123,
                'StartingToken': 'string'
            }
        )
        '''
        client = db.meta.client
        paginator = client.get_paginator('query')
        limit = kwargs.pop('Limit', None)
        kwargs['TableName'] = self.model_object.__table_name__
        if limit is not None:
            pagination_config = {
                'MaxItems': limit,
                'PageSize': limit,
                'StaringToken': '123232'
            }
            kwargs['PaginationConfig'] = pagination_config
        # response = paginator.paginate(**kwargs).build_full_result()
        response = paginator.paginate(**kwargs)
        for item in response:
            pass
        return item


class Query(object):

    def __init__(self, model_object, *args, **kwargs):
        self.Scan = False
        self.model_object = model_object
        self.model_class = self.model_object.__class__
        self.instance = self.model_class()
        self.ProjectionExpression = self._projection_expression(*args)
        self.ReturnConsumedCapacity = 'TOTAL'  # 'INDEXES'|'TOTAL'|'NONE'
        self.ConsistentRead = False
        self.FilterExpression = None
        self.ExclusiveStartKey = None  # 起始查询的key，就是上一页的最后一条数据
        self.KeyConditionExpression = None
        self.ExpressionAttributeNames = {}
        self.ExpressionAttributeValues = {}
        self.ScanIndexForward = False    # True|False
        self.ConditionalOperator = None  # 'AND'|'OR'
        self.IndexName = None
        self.Select = 'ALL_ATTRIBUTES'  # 'ALL_ATTRIBUTES'|'ALL_PROJECTED_ATTRIBUTES'|'SPECIFIC_ATTRIBUTES'|'COUNT'
        self.Offset = None
        self.Limit = None
        self.scaned_count = 0
        self.count = 0
        self.request_params = {}
        self.filter_args = []   # filter expression args
        self.filter_index_field = None  # index field name
        self.use_index_type = 0 #0=>primary, 1=>local secondary, 2=>global

    @property
    def consistent(self):
        self.ConsistentRead = True
        return self

    @property
    def scan(self):
        self.Scan = True
        return self

    def start_key_str(self, _str):
        if(_str):
            self.ExclusiveStartKey = json.loads(_str)
            self.request_params['ExclusiveStartKey'] = self.ExclusiveStartKey
        return self
    
    def start_key(self, cursor=None):
        if(cursor):
            self.ExclusiveStartKey = cursor
            self.request_params['ExclusiveStartKey'] = self.ExclusiveStartKey
        return self
    

    def _projection_expression(self, *args):
        instance = self.model_object
        projections = []
        for arg in args:
            if isinstance(arg, Fields):
                name = arg.name
                if arg not in instance.fields:
                    raise FieldValidationException('%s not found' % name)
                projections.append(name)
            else:
                raise FieldValidationException('Bad type must be Attribute type')
        ProjectionExpression = ",".join(projections)
        return ProjectionExpression

    def _get_primary_key(self):
        hash_key, range_key = self.instance._hash_key, self.instance._range_key
        key = {
            hash_key: getattr(self.instance, hash_key)
        }
        _range_key = getattr(self.instance, range_key, None)
        if range_key and not _range_key:
            raise FieldValidationException('Invalid range key value type')
        elif range_key:
            key[range_key] = _range_key
        return key

    def _filter_expression(self, *args):
        # get filter expression and key condition expression
        FilterExpression = None
        KeyConditionExpression = None
        params = {}
        for field_inst, exp, is_key in args:
            # field_inst = field_instance
            if field_inst.name == self.filter_index_field or \
                            (self.use_index_type==2  and  ( field_inst._is_secondary_hash_key or  field_inst._is_secondary_range_key )):
                _, exp, is_key = field_inst._expression_func(
                    field_inst.op, *field_inst.express_args, use_key=True)
            if is_key:
                if not KeyConditionExpression:
                    KeyConditionExpression = exp
                else:
                    KeyConditionExpression = KeyConditionExpression & exp
            else:
                if not FilterExpression:
                    FilterExpression = exp
                else:
                    FilterExpression = FilterExpression & exp
        if FilterExpression:
            params['FilterExpression'] = FilterExpression
        if KeyConditionExpression:
            params['KeyConditionExpression'] = KeyConditionExpression
        return params

    def _get_query_params(self):
        # get filter params
        params = {}
        # update filter expression
        filter_params = self._filter_expression(*self.filter_args)
        FilterExpression = filter_params.get('FilterExpression')
        if FilterExpression:
            params['FilterExpression'] = FilterExpression
        KeyConditionExpression = filter_params.get('KeyConditionExpression')
        if KeyConditionExpression:
            params['KeyConditionExpression'] = KeyConditionExpression
        if self.ProjectionExpression:
            params['ProjectionExpression'] = self.ProjectionExpression
        if self.ConsistentRead:
            params['ConsistentRead'] = self.ConsistentRead
        if self.ReturnConsumedCapacity:
            params['ReturnConsumedCapacity'] = self.ReturnConsumedCapacity
        self.request_params.update(params)
        return self.request_params

    def where(self, *args):
        # Find by any number of matching criteria... though presently only
        # "where" is supported.
        self.filter_args.extend(args)
        return self

    def limit(self, limit):
        self.Limit = limit
        self.request_params['Limit'] = limit
        return self

    def _get_item_params(self):
        params = {
            'Key': self._get_primary_key()
        }
        if self.ProjectionExpression:
            params['ProjectionExpression'] = self.ProjectionExpression
        if self.ConsistentRead:
            params['ConsistentRead'] = self.ConsistentRead
        return params

    @use_connection_pool(db="dynamodb")
    def get(self, db=None, **primary_key):
        # get directly by primary key
        self.instance = self.model_class(**primary_key)
        params = self._get_item_params()
        item = Table(self.instance, db).get_item(**params)
        if not item:
            return None
        value_for_read = self.instance._get_values_for_read(item)
        return value_for_read

    def first(self):
        response = self.limit(1).all()
        items = response['Items']
        return items[0] if items else None
    
    def use_index(self, index_field_or_name,asc=True):
        if isinstance(index_field_or_name, Fields):
            name = index_field_or_name.name
            index_name = self.instance._local_indexes.get(name)
            if not (index_name or index_field_or_name.range_key):
                raise FieldValidationException('index not found')
            self.filter_index_field = name
            if index_name:
                self.request_params['IndexName'] = index_name
            self.request_params['ScanIndexForward'] = asc
            self.use_index_type = 1
        elif isinstance(index_field_or_name, str):
            self.request_params['IndexName'] = index_field_or_name
            self.request_params['ScanIndexForward'] = asc
            index_name = self.model_class._global_indexes.get(index_field_or_name)
            if(index_name):
                self.use_index_type = 2
        elif(index_field_or_name==None):
            self.request_params['ScanIndexForward'] = asc
 
        return self

    def _yield_all(self, method, db):
        if method == 'scan':
            func = getattr(Table(self.instance, db), 'scan')
        elif method == 'query':
            func = getattr(Table(self.instance, db), 'query')
        else:
            return
        result_count = 0
        response = func(**self.request_params)
        while True:
            metadata = response.get('ResponseMetadata', {})
            for item in response['Items']:
                result_count += 1
                yield item
                if self.Limit > 0 and result_count >= self.Limit:
                    return
            LastEvaluatedKey = response.get('LastEvaluatedKey')
            if LastEvaluatedKey:
                self.request_params['ExclusiveStartKey'] = LastEvaluatedKey
                response = func(**self.request_params)
            else:
                break

    def _yield(self, db):
        if self.Scan:
            return self._yield_all('scan', db)
        else:
            return self._yield_all('query', db)

    @use_connection_pool(db="dynamodb")
    def all(self, db=None):
        self._get_query_params()
        if self.Scan:
            func = getattr(Table(self.instance, db), 'scan')
            #return self._yield_all('scan', db)
        else:
            func = getattr(Table(self.instance, db), 'query')
        response = func(**self.request_params)
        items = response['Items']
        results = []
        for item in items:
            _instance = self.model_class(**item)
            value_for_read = _instance._get_values_for_read(item)
            instance = self.model_class(**value_for_read)
            results.append(instance)
        response['Items'] = results
        return response
    
    def get_items(self, requery_for_all_projections=False):
        resp = self.all()
        items = resp["Items"]
        cursor = resp.get("LastEvaluatedKey", None)
        if(self.use_index_type != 0 and requery_for_all_projections and items):
            _items = self.model_class.batch_get(*map(lambda x : x.get_primary_key_dict() , items))
            #re-ordering back shit
            _items_map = {}
            for _item in _items:
                _items_map[_item.get_primary_key_tuple()] = _item
            
            items = [_items_map[item.get_primary_key_tuple()] for item in items]
        return items, cursor
