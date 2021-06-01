#! -*- coding: utf-8 -*-


import pprint

from decimal import Decimal
from botocore.exceptions import ClientError
from botocore.vendored.requests.exceptions import ConnectionError

from .helpers import get_attribute_type
from .errors import ClientException, ConnectionException, ParameterException

pp = pprint.PrettyPrinter(indent=4)
pprint = pp.pprint

__all__ = ['Table']


class Table(object):

    def __init__(self, instance, db):
        self.instance = instance
        self.db=db
        self.table_name = instance.__table_name__
        self.table = db.Table(self.table_name)

    def info(self):
        try:
            response = self.db.meta.client.describe_table(TableName=self.table_name)
        except ClientError as e:
            raise ClientException(e.response['Error']['Message'])
        else:
            table_info = response['Table']
        return table_info

    def _prepare_hash_key(self):
        hash_key = self.instance._hash_key
        param = {
            'AttributeName': hash_key,
            'KeyType': 'HASH'
        }
        return param

    def _prepare_range_key(self, range_key=None):
        if not range_key:
            range_key = self.instance._range_key
        if range_key:
            param = {
                'AttributeName': range_key,
                'KeyType': 'RANGE'
            }
            return param
        return {}

    def _prepare_key_schema(self):
        KeySchema = []
        hash_key_param = self._prepare_hash_key()
        KeySchema.append(hash_key_param)
        range_key_param = self._prepare_range_key()
        if range_key_param:
            KeySchema.append(range_key_param)
        return KeySchema

    def _prepare_attribute_definitions(self):
        AttributeDefinitions = []
        attributes = self.instance.attributes
        hash_key = self.instance._hash_key
        _fields_indexed = {}
        AttributeDefinitions.append({
            'AttributeName': hash_key,
            'AttributeType': get_attribute_type(attributes[hash_key]),
        })
        _fields_indexed[hash_key] = True
        range_key = self.instance._range_key
        if range_key:
            AttributeDefinitions.append({
                'AttributeName': range_key,
                'AttributeType': get_attribute_type(attributes[range_key]),
            })
            _fields_indexed[range_key] = True
            
        for field in self.instance._local_indexed_fields:
            AttributeDefinitions.append({
                'AttributeName': field,
                'AttributeType': get_attribute_type(attributes[field]),
            })
            _fields_indexed[field] = True


        #for global indexes
        for index_name, attribute_dict in self.instance._global_indexes.items():
            field = attribute_dict["hash_key"]
            if(not _fields_indexed.get(field, None)):
                AttributeDefinitions.append({
                    'AttributeName': field,
                    'AttributeType': get_attribute_type(attributes[field])
                })
            range_key = attribute_dict.get("range_key", None)
            if(range_key and not _fields_indexed.get(range_key, None)):
                AttributeDefinitions.append({
                    'AttributeName': range_key,
                    'AttributeType': get_attribute_type(attributes[range_key])
                })


                    
        return AttributeDefinitions

    def _prepare_primary_key(self, params):
        params['KeySchema'] = self._prepare_key_schema()
        params['AttributeDefinitions'] = self._prepare_attribute_definitions()
        return params

    def _prepare_local_indexes(self):
        indexes = []
        for field in self.instance._local_indexed_fields:
            index_name = '{table_name}_ix_{field}'.format(
                table_name=self.table_name, field=field)
            KeySchema = [self._prepare_hash_key()]
            range_key_param = self._prepare_range_key(field)
            if range_key_param:
                KeySchema.append(range_key_param)
            
            index_properties = {
                'IndexName': index_name,
                'KeySchema': KeySchema,
            }
            field_object = self.instance.attributes[field]
            if(field_object.projections):
                index_properties['Projection'] = {'ProjectionType': 'INCLUDE',
                                                  'NonKeyAttributes': field_object.projections
                                                }
            else:
                index_properties['Projection'] = {'ProjectionType': 'KEYS_ONLY'}

            indexes.append(index_properties)
        return indexes

    def _prepare_global_indexes(self):
        indexes = []
        for index_name, attribute_dict in self.instance._global_indexes.items():
            KeySchema = [{
                'AttributeName': attribute_dict["hash_key"], 
                'KeyType': 'HASH'
            }]
            if attribute_dict.get("range_key", None):
                KeySchema.append({
                        'AttributeName': attribute_dict.get("range_key"),
                        'KeyType': 'RANGE'
                })
            index_properties = {
                'IndexName': index_name,
                'KeySchema': KeySchema
            }

            read_capacity_units = attribute_dict.get("read_capacity")
            write_capacity_units = attribute_dict.get("write_capacity")

            if(read_capacity_units and write_capacity_units):
                index_properties['ProvisionedThroughput'] = {
                    'ReadCapacityUnits': read_capacity_units,
                    'WriteCapacityUnits': write_capacity_units
                }
                index_properties['BillingMode'] = "PROVISIONED"
            else:
                #this should not appear but due to a bug we are forced to user this
                # index_properties['ProvisionedThroughput'] = {
                #     'ReadCapacityUnits': 1,
                #     'WriteCapacityUnits': 1
                # }
                #assuming billing is pay per request
                index_properties['BillingMode'] = "PAY_PER_REQUEST"
                
            if("projections" in attribute_dict):
                index_properties['Projection'] = {'ProjectionType': 'INCLUDE',
                                                  'NonKeyAttributes': attribute_dict["projections"]
                                                 }
            else:
                index_properties['Projection'] = {'ProjectionType': 'KEYS_ONLY'}

            
            indexes.append(index_properties)
        return indexes

    def _prepare_create_table_params(self):
        # TableName
        table_params = {
            'TableName': self.table_name
        }
        # KeySchema && AttributeDefinitions
        table_params = self._prepare_primary_key(table_params)
        # LocalSecondaryIndexes
        local_indexes = self._prepare_local_indexes()
        if local_indexes:
            table_params['LocalSecondaryIndexes'] = local_indexes
        # GlobalSecondaryIndexes
        global_indexes = self._prepare_global_indexes()
        if global_indexes:
            table_params['GlobalSecondaryIndexes'] = global_indexes
        # ProvisionedThroughput

        read_capacity_units = getattr(self.instance, 'ReadCapacityUnits', None)
        write_capacity_units = getattr(self.instance, 'WriteCapacityUnits', None)
        if(read_capacity_units and write_capacity_units):
            table_params['ProvisionedThroughput'] = {
                'ReadCapacityUnits': read_capacity_units,
                'WriteCapacityUnits': write_capacity_units
            }
            table_params['BillingMode'] = "PROVISIONED"
        else:
            #assuming billing is pay per request
            table_params['BillingMode'] = "PAY_PER_REQUEST"

        return table_params

    def create(self):
        '''
        # create table
        create_table Request Syntax
        # http://boto3.readthedocs.io/en/sinstance/reference/services/dynamodb.html#DynamoDB.Client.create_instance
        response = client.create_table(
            AttributeDefinitions=[
                {
                    'AttributeName': 'string',
                    'AttributeType': 'S'|'N'|'B'
                },
            ],
            TableName='string',
            KeySchema=[
                {
                    'AttributeName': 'string',
                    'KeyType': 'HASH'|'RANGE'
                },
            ],
            LocalSecondaryIndexes=[
                {
                    'IndexName': 'string',
                    'KeySchema': [
                        {
                            'AttributeName': 'string',
                            'KeyType': 'HASH'|'RANGE'
                        },
                    ],
                    'Projection': {
                        'ProjectionType': 'ALL'|'KEYS_ONLY'|'INCLUDE',
                        'NonKeyAttributes': [
                            'string',
                        ]
                    }
                },
            ],
            GlobalSecondaryIndexes=[
                {
                    'IndexName': 'string',
                    'KeySchema': [
                        {
                            'AttributeName': 'string',
                            'KeyType': 'HASH'|'RANGE'
                        },
                    ],
                    'Projection': {
                        'ProjectionType': 'ALL'|'KEYS_ONLY'|'INCLUDE',
                        'NonKeyAttributes': [
                            'string',
                        ]
                    },
                    'ProvisionedThroughput': {
                        'ReadCapacityUnits': 123,
                        'WriteCapacityUnits': 123
                    }
                },
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 123,
                'WriteCapacityUnits': 123
            },
            StreamSpecification={
                'StreamEnabled': True|False,
                'StreamViewType': 'NEW_IMAGE'|'OLD_IMAGE'|'NEW_AND_OLD_IMAGES'|'KEYS_ONLY'
            }
        )

        AttributeType (string) -- [REQUIRED]
            The data type for the attribute, where:

            * S - the attribute is of type String
            * N - the attribute is of type Number
            * B - the attribute is of type Binary
        KeySchema (list) -- [REQUIRED]
        KeyType - The role that the key attribute will assume:
            * HASH - partition key
            * RANGE - sort key
        '''
        try:
            params = self._prepare_create_table_params()
            return self.db.create_table(**params)
        except ClientError as e:
            raise ClientException(e.response['Error']['Message'])
        except ConnectionError:
            raise ConnectionException('Connection refused')

    def _update_throughput(self, ProvisionedThroughput):
        ReadCapacityUnits = ProvisionedThroughput.get('ReadCapacityUnits', None)
        WriteCapacityUnits = ProvisionedThroughput.get('WriteCapacityUnits', None)

        read_capacity_units = getattr(self.instance, 'ReadCapacityUnits', None)
        write_capacity_units = getattr(self.instance, 'WriteCapacityUnits', None)

        if (read_capacity_units != ReadCapacityUnits or
                write_capacity_units != WriteCapacityUnits):
            self.table.update(ProvisionedThroughput={
                'ReadCapacityUnits': read_capacity_units,
                'WriteCapacityUnits': write_capacity_units
            })

    def _update_streams(self):
        # TODO
        pass

    def _update_global_indexes(self):
        # TODO
        pass

    def _update_billing_mode(self):
        pass

    def update(self):
        '''
        # update table
        http://boto3.readthedocs.io/en/stable/reference/services/dynamodb.html#DynamoDB.Table.update
        You can only perform one of the following operations at once:

        * Modify the provisioned throughput settings of the table.
        * Enable or disable Streams on the table.
        * Remove a global secondary index from the table.
        * Create a new global secondary index on the table.
          Once the index begins backfilling, you can use UpdateTable to perform
          other operations.

        UpdateTable is an asynchronous operation; while it is executing,
        the table status changes from ACTIVE to UPDATING. While it is UPDATING,
        you cannot issue another UpdateTable request.
        When the table returns to the ACTIVE state, the UpdateTable operation is
        complete.

        # Request Syntax

        AttributeDefinitions=[
            {
                'AttributeName': 'string',
                'AttributeType': 'S'|'N'|'B'
            },
        ],
        BillingMode='PROVISIONED'|'PAY_PER_REQUEST',
        ProvisionedThroughput={
            'ReadCapacityUnits': 123,
            'WriteCapacityUnits': 123
        },
        GlobalSecondaryIndexUpdates=[
            {
                'Update': {
                    'IndexName': 'string',
                    'ProvisionedThroughput': {
                        'ReadCapacityUnits': 123,
                        'WriteCapacityUnits': 123
                    }
                },
                'Create': {
                    'IndexName': 'string',
                    'KeySchema': [
                        {
                            'AttributeName': 'string',
                            'KeyType': 'HASH'|'RANGE'
                        },
                    ],
                    'Projection': {
                        'ProjectionType': 'ALL'|'KEYS_ONLY'|'INCLUDE',
                        'NonKeyAttributes': [
                            'string',
                        ]
                    },
                    'ProvisionedThroughput': {
                        'ReadCapacityUnits': 123,
                        'WriteCapacityUnits': 123
                    }
                },
                'Delete': {
                    'IndexName': 'string'
                }
            },
        ],
        StreamSpecification={
            'StreamEnabled': True|False,
            'StreamViewType': 'NEW_IMAGE'|'OLD_IMAGE'|'NEW_AND_OLD_IMAGES'|'KEYS_ONLY'
        },
        SSESpecification={
            'Enabled': True|False,
            'SSEType': 'AES256'|'KMS',
            'KMSMasterKeyId': 'string'
        },
        ReplicaUpdates=[
            {
                'Create': {
                    'RegionName': 'string',
                    'KMSMasterKeyId': 'string',
                    'ProvisionedThroughputOverride': {
                        'ReadCapacityUnits': 123
                    },
                    'GlobalSecondaryIndexes': [
                        {
                            'IndexName': 'string',
                            'ProvisionedThroughputOverride': {
                                'ReadCapacityUnits': 123
                            }
                        },
                    ]
                },
                'Update': {
                    'RegionName': 'string',
                    'KMSMasterKeyId': 'string',
                    'ProvisionedThroughputOverride': {
                        'ReadCapacityUnits': 123
                    },
                    'GlobalSecondaryIndexes': [
                        {
                            'IndexName': 'string',
                            'ProvisionedThroughputOverride': {
                                'ReadCapacityUnits': 123
                            }
                        },
                    ]
                },
                'Delete': {
                    'RegionName': 'string'
                }
            },
        ]
        '''
        table_info = self.info()
        ProvisionedThroughput = table_info.get('ProvisionedThroughput')
        self._update_throughput(ProvisionedThroughput)

        self._update_billing_mode(table_info.get('BillingMode'))


    def delete(self):
        # delete table
        try:
            return self.table.delete()
        except ClientError:
            raise ClientException('Cannot do operations on a non-existent table')
        except ConnectionError:
            raise ConnectionException('Connection refused')

    def _get_primary_key(self, **kwargs):
        hash_key, range_key = self.instance._hash_key, self.instance._range_key
        hash_value = kwargs.get(hash_key) or getattr(self.instance, hash_key)
        if isinstance(hash_value, (int, float)):
            hash_value = Decimal(hash_value)
        key = {
            hash_key: hash_value
        }
        if(range_key):
            range_value = kwargs.get(range_key) or getattr(self.instance, range_key, None)
            if not range_value:
                raise ParameterException('Invalid range key value type')
            if isinstance(range_value, (int, float)):
                range_value = Decimal(range_value)
            key[range_key] = range_value
        return key

    def get_item(self, **kwargs):
        """
        primary_key: params: primary_key dict
        """
        kwargs['Key'] = kwargs.get('Key') or self._get_primary_key()
        try:
            response = self.table.get_item(**kwargs)
        except ClientError as e:
            if e.response['Error']['Code'] == 'ValidationException':
                return None
            raise ClientException(e.response['Error']['Message'])
        else:
            item = response.get('Item')
        return item

    def batch_get_item(self, *primary_keys, **kwargs):
        """
        primary_key: params: primary_keys list
        """
        _primary_keys = []
        for primary_key in primary_keys:
            key = self._get_primary_key(**primary_key)
            _primary_keys.append(key)
        params = {
            'RequestItems': {
                self.table_name: {
                    'Keys': _primary_keys
                }
            },
            'ReturnConsumedCapacity': 'TOTAL'
        }
        projections = kwargs.get("projections", None)
        if(projections):
            params["RequestItems"]["ProjectionExpression"] = ",".join(projections)
        
        try:
            response = self.db.batch_get_item(**params)
        except ClientError as e:
            raise ClientException(e.response['Error']['Message'])
        else:
            items = response['Responses'][self.table_name]
        return items

    def put_item(self, item, overwrite=True):
        args = dict(Item=item)
        if(not overwrite):
            expr = 'attribute_not_exists(%s)'%(self.instance.__class__._hash_key,)
            if(self.instance.__class__._range_key):
                expr += (" AND attribute_not_exists(%s)"%(self.instance.__class__._range_key,))
            args["ConditionExpression"] = expr
            
            
        self.table.put_item(**args)
        return True

    def batch_write(self, items, overwrite=False):
        pkeys = []
        if overwrite:
            instance = self.instance
            pkeys = [instance._hash_key, instance._range_key]
        try:
            with self.table.batch_writer(overwrite_by_pkeys=pkeys) as batch:
                for item in items:
                    batch.put_item(Item=item)
        except ClientError as e:
            raise ClientException(e.response['Error']['Message'])
        
    def batch_delete(self, items=None, keys=None):
        try:
            with self.table.batch_writer() as batch:
                if(items):
                    for item in items:
                        batch.delete_item(Key=Table(item)._get_primary_key())
                if(keys):
                    for key in keys:
                        batch.delete_item(Key=key)
                    
        except ClientError as e:
            raise ClientException(e.response['Error']['Message'])


    def query(self, **kwargs):
        """
        response = table.query(
            IndexName='string',
            Select='ALL_ATTRIBUTES'|'ALL_PROJECTED_ATTRIBUTES'|'SPECIFIC_ATTRIBUTES'|'COUNT',
            Limit=123,
            ConsistentRead=True|False,
            ScanIndexForward=True|False,
            ExclusiveStartKey={
                'string': 'string'|123|Binary(b'bytes')|True|None|set(['string'])|set([123])|set([Binary(b'bytes')])|[]|{}
            },
            ReturnConsumedCapacity='INDEXES'|'TOTAL'|'NONE',
            ProjectionExpression='string',
            FilterExpression=Attr('myattribute').eq('myvalue'),
            KeyConditionExpression=Key('mykey').eq('myvalue'),
            ExpressionAttributeNames={
                'string': 'string'
            },
            ExpressionAttributeValues={
                'string': 'string'|123|Binary(b'bytes')|True|None|set(['string'])|set([123])|set([Binary(b'bytes')])|[]|{}
            }
        )
        """
        try:
            #print(kwargs)
            response = self.table.query(**kwargs)
        except ClientError as e:
            raise ClientException(e.response['Error']['Message'])
        return response

    def scan(self, **kwargs):
        try:
            #print(kwargs)
            response = self.table.scan(**kwargs)
        except ClientError as e:
            raise ClientException(e.response['Error']['Message'])
        return response

    def _prepare_update_item_params(self, update_fields=None, *args, **kwargs):
        params = {
            'Key': self._get_primary_key()
        }
        _condition_expression = kwargs.pop('ConditionExpression', None)
        if _condition_expression:
            params['ConditionExpression'] = _condition_expression
        _expression_attribute_values = kwargs.pop('ExpressionAttributeValues', {})
        _expression_attribute_names = kwargs.pop('ExpressionAttributeNames', {})
        action_exp_dict = {}
        if update_fields:
            set_expression_str = ''
            for k, v in update_fields.items():
                label = ':{k}'.format(k=k)
                path = '#{k}'.format(k=k)
                if set_expression_str:
                    set_expression_str += ', {k} = {v}'.format(k=path, v=label)
                else:
                    set_expression_str += '{k} = {v}'.format(k=path, v=label)
                _expression_attribute_values[label] = v
                _expression_attribute_names[path] = k
            action_exp_dict['SET'] = set_expression_str
        for arg in args:
            exp, exp_attr, action = arg
            eav = exp_attr.get('value', {})
            ean = exp_attr.get('name', {})
            action_exp = action_exp_dict.get(action)
            if action_exp:
                action_exp = '{action_exp}, {exp}'.format(action_exp=action_exp,
                                                          exp=exp)
            else:
                action_exp = exp
            action_exp_dict[action] = action_exp
            _expression_attribute_values.update(eav)
            _expression_attribute_names.update(ean)
        for action, _exp in action_exp_dict.items():
            action_exp_dict[action] = '{action} {exp}'.format(action=action,
                                                              exp=_exp)
        if _expression_attribute_values:
            params['ExpressionAttributeValues'] = _expression_attribute_values
        if _expression_attribute_names:
            params['ExpressionAttributeNames'] = _expression_attribute_names
        params['UpdateExpression'] = " ".join(action_exp_dict.values())
        params.update(kwargs)
        return params

    def update_item(self, update_fields, *args, **kwargs):
        '''
        update_fields: update_fields (dict)
        http://boto3.readthedocs.io/en/stable/reference/services/dynamodb.html#DynamoDB.Table.update_item
        response = table.update_item(
            Key={
                'string': 'string'|123|Binary(b'bytes')|True|None|set(['string'])|set([123])|set([Binary(b'bytes')])|[]|{}
            },
            ReturnValues='NONE'|'ALL_OLD'|'UPDATED_OLD'|'ALL_NEW'|'UPDATED_NEW',
            ReturnConsumedCapacity='INDEXES'|'TOTAL'|'NONE',
            ReturnItemCollectionMetrics='SIZE'|'NONE',
            UpdateExpression='string',
            ConditionExpression=Attr('myattribute').eq('myvalue'),
            ExpressionAttributeNames={
                'string': 'string'
            },
            ExpressionAttributeValues={
                'string': 'string'|123|Binary(b'bytes')|True|None|set(['string'])|set([123])|set([Binary(b'bytes')])|[]|{}
            }
        )
        ## example
        item.update_item(a=12, b=12, c=12)
        '''
        params = self._prepare_update_item_params(update_fields, *args, **kwargs)
        try:
            item = self.table.update_item(**params)
            attributes = item.get('Attributes')
            return attributes
        except ClientError as e:
            if e.response['Error']['Code'] == "ConditionalCheckFailedException":
                print(e.response['Error']['Message'])
            raise ClientException(e.response['Error']['Message'])

    def _prepare_delete_item_params(self, **kwargs):
        params = {
            'Key': self._get_primary_key()
        }
        _condition_expression = kwargs.pop('ConditionExpression', None)
        if _condition_expression:
            params['ConditionExpression'] = _condition_expression
        _expression_attribute_values = kwargs.pop('ExpressionAttributeValues', {})
        if _expression_attribute_values:
            params['ExpressionAttributeValues'] = _expression_attribute_values
        _expression_attribute_names = kwargs.pop('ExpressionAttributeNames', {})
        if _expression_attribute_names:
            params['ExpressionAttributeNames'] = _expression_attribute_names
        return params

    def delete_item(self, key=None, **kwargs):
        '''
        http://boto3.readthedocs.io/en/stable/reference/services/dynamodb.html#DynamoDB.Table.delete_item
        Deletes a single item in a table by primary key. You can perform a
        conditional delete operation that deletes the item if it exists,
        or if it has an expected attribute value.

        In addition to deleting an item, you can also return the item's
        attribute values in the same operation, using the ReturnValues parameter.

        Unless you specify conditions, the DeleteItem is an idempotent operation;
        running it multiple times on the same item or attribute does not result
        in an error response.

        Conditional deletes are useful for deleting items only if specific
        conditions are met. If those conditions are met, DynamoDB performs the
        delete. Otherwise, the item is not deleted.

        Request Syntax

        response = table.delete_item(
            Key={
                'string': 'string'|123|Binary(b'bytes')|True|None|set(['string'])|set([123])|set([Binary(b'bytes')])|[]|{}
            },
            ReturnValues='NONE'|'ALL_OLD'|'UPDATED_OLD'|'ALL_NEW'|'UPDATED_NEW',
            ReturnConsumedCapacity='INDEXES'|'TOTAL'|'NONE',
            ReturnItemCollectionMetrics='SIZE'|'NONE',
            ConditionExpression=Attr('myattribute').eq('myvalue'),
            ExpressionAttributeNames={
                'string': 'string'
            },
            ExpressionAttributeValues={
                'string': 'string'|123|Binary(b'bytes')|True|None|set(['string'])|set([123])|set([Binary(b'bytes')])|[]|{}
            }
        )
        Parameters:
            Key (dict) -- [REQUIRED]
        '''
        key = key or self._get_primary_key()
        try:
            self.table.delete_item(Key=key, ReturnValues='ALL_OLD')
        except ClientError as e:
            if e.response['Error']['Code'] == "ConditionalCheckFailedException":
                raise ClientException(e.response['Error']['Message'])
        return True

    def item_count(self):
        return self.table.item_count
