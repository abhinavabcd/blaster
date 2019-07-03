#! -*- coding: utf-8 -*-
import copy

from botocore.exceptions import ClientError
from blaster.connection_pool import use_connection_pool

from .table import Table
from .query import Query
from .fields import Attribute
from .errors import FieldValidationException, ValidationException, ClientException
from .helpers import get_items_for_storage
from .fields import ListField, SetField, DictField,\
    CharField


def _initialize_attributes(model_class, name, bases, attrs):
    """
    Initialize the attributes of the model.
    """
    model_class._attributes = {}

    # In case of inheritance, we also add the parent's
    # attributes in the list of our attributes
    for parent in bases:
        if not isinstance(parent, ModelMetaclass):
            continue
        for k, v in parent._attributes.items():
            model_class._attributes[k] = v

    for k, v in attrs.items():
        if isinstance(v, Attribute):
            model_class._attributes[k] = v
            v.name = v.name or k


def _initialize_indexes(model_class, name, bases, attrs):
    """
    Stores the list of indexed attributes.
    """
    model_class._local_indexed_fields = []
    model_class._local_indexes = {}
    model_class._global_indexes = {}
    model_class._hash_key = None
    model_class._range_key = None
    for parent in bases:
        if not isinstance(parent, ModelMetaclass):
            continue
        for k, v in parent._attributes.items():
            if v.indexed:
                model_class._local_indexed_fields.append(k)

    for k, v in attrs.items():
        if isinstance(v, Attribute):
            if v.indexed:
                model_class._local_indexed_fields.append(k)
                model_class._local_indexes[k] = '{table_name}_ix_{field}'.format(
                    table_name=name, field=k)
            elif v.range_key:
                model_class._range_key = k
            elif v.hash_key:
                model_class._hash_key = k
    if name not in ('ModelBase', 'Model') and not model_class._hash_key:
        raise ValidationException('hash_key is required')
    
    #global indexes
    #TODO: copy from all base classes
    if(model_class.__secondary_indexes__):
        for global_index_name, index_attrs in model_class.__secondary_indexes__.items():
            hash_key = index_attrs.get("hash_key")
            range_key = index_attrs.get("range_key",None)
            
            model_class._attributes[hash_key]._is_secondary_hash_key = global_index_name
            if(range_key):
                model_class._attributes[range_key]._is_secondary_range_key = global_index_name
                
            model_class._global_indexes[global_index_name] = index_attrs
    
    

class ModelMetaclass(type):

    """
    Metaclass of the Model.
    """

    __table_name__ = None
    __local_index__ = {}
    
    __secondary_indexes__ = None

    def __init__(cls, name, bases, attrs):
        super(ModelMetaclass, cls).__init__(name, bases, attrs)
        name = cls.__table_name__ or name
        _initialize_attributes(cls, name, bases, attrs)
        _initialize_indexes(cls, name, bases, attrs)


class ModelBase(object, metaclass=ModelMetaclass):

    _condition_expression = None
    _expression_attribute_values = None

    pending_updates = None
    
    def __init__(self):
        self.pending_updates = []

    @classmethod
    @use_connection_pool(db="dynamodb")
    def create(cls, db=None, overwrite=True, **kwargs):
        instance = cls(**kwargs)
        if not instance.is_valid():
            raise ValidationException(instance.errors)
        try:
            Table(instance, db).put_item(instance.item, overwrite=overwrite)
            item = cls(**instance.item)
            return item
        except ClientError as e:
            raise ClientException(e.response['Error']['Message'])

    def _prepare_update_item_params(self, **kwargs):
        params = kwargs
        if self._condition_expression:
            params['ConditionExpression'] = self._condition_expression
        if self._expression_attribute_values:
            params['ExpressionAttributeValues'] = self._expression_attribute_values
        return params

  
    @use_connection_pool(db="dynamodb")
    def update(self, *args, **kwargs):
        update_fields = {}
        db = kwargs.pop("db")
        ReturnValues = kwargs.pop('ReturnValues', 'ALL_NEW')
        ReturnConsumedCapacity = kwargs.pop('ReturnConsumedCapacity', 'NONE')
        params = self._prepare_update_item_params(
            ReturnValues=ReturnValues,
            ReturnConsumedCapacity=ReturnConsumedCapacity)
        if not self.validate_attrs(**kwargs):
            raise FieldValidationException(self._errors)
        for k, v in kwargs.items():
            field = self.attributes[k]
            update_fields[k] = field.typecast_for_storage(v)
        # use storage value
        item = Table(self, db).update_item(
            update_fields, *args, **params)
        value_for_read = self._get_values_for_read(item)
        for k, v in value_for_read.items():
            setattr(self, k, v)#set new values
        return self

    @classmethod
    @use_connection_pool(db="dynamodb")
    def get(cls, db=None, **primary_key):
        instance = cls(**primary_key)
        item = Table(instance, db).get_item()
        if not item:
            return None
        value_for_read = instance._get_values_for_read(item)
        return cls(**value_for_read)

    @classmethod
    @use_connection_pool(db="dynamodb")
    def batch_get(cls, *primary_keys, **kwargs):
        if(not primary_keys):
            return [] #no results
        instance = cls()
        projections = kwargs.get("projections", None)
        if(projections):
            kwargs["projections"] = [x.name for x in projections]
            
        items = Table(instance,kwargs.get("db")).batch_get_item(*primary_keys)
        results = []
        for item in items:
            value_for_read = instance._get_values_for_read(item)
            results.append(cls(**value_for_read))
        return results

    @classmethod
    @use_connection_pool(db="dynamodb")
    def batch_write(cls, kwargs, overwrite=False, db=None):
        '''
        kwargs: items list
        '''
        items_validated, instances = get_items_for_storage(cls, kwargs)
        instance = cls()
        Table(instance,db).batch_write(items_validated, overwrite=overwrite)
        return instances

    @classmethod
    @use_connection_pool(db="dynamodb")
    def batch_delete(cls, items=None, keys=None,db=None):
        instance = cls()
        return Table(instance,db).batch_delete(items, keys)

    #utility function
    def get_primary_key_dict(self):
        ret = {}
        ret[self.__class__._hash_key] = getattr(self, self.__class__._hash_key)
        if(self.__class__._range_key):
            ret[self.__class__._range_key] =  getattr(self, self.__class__._range_key)
        return ret
    
    def get_primary_key_tuple(self):
        ret = {}
        if(self.__class__._range_key):
            return (getattr(self, self.__class__._hash_key), getattr(self, self.__class__._range_key))
        return tuple(getattr(self, self.__class__._hash_key))
    
    @use_connection_pool(db="dynamodb")
    def delete(self , db=None):
        # delete an item
        return Table(self, db).delete_item()

    @classmethod
    @use_connection_pool(db="dynamodb")
    def delete_by_key(cls ,  db=None, **kwargs):
        # delete an item
        return Table(cls(), db).delete_item(key=kwargs)

    @classmethod
    def query(cls, *args):
        instance = cls()
        return Query(instance, *args)

    @classmethod
    @use_connection_pool(db="dynamodb")
    def scan(cls, db=None, **kwargs):
        instance = cls()
        return Table(instance, db).scan(**kwargs)

    @classmethod
    @use_connection_pool(db="dynamodb")
    def item_count(cls, db=None):
        instance = cls()
        return Table(instance, db).item_count()

    @use_connection_pool(db="dynamodb")
    def write(self,db=None):
        item = Table(self, db).put_item(self.item)
        return item

    def save(self, overwrite=False):
        if not self.is_valid():
            raise ValidationException(self.errors)
        self.write()
        return self

    


class Model(ModelBase):

    pending_updates = None
    
    def __init__(self, **kwargs):
        self.update_attributes(**kwargs)
        self.projections = []
        self.pending_updates = []

    def is_valid(self):
        """
        Returns True if all the fields are valid, otherwise
        errors are in the 'errors' attribute
        It first validates the fields (required, unique, etc.)
        and then calls the validate method.
        >>> from dynamodb import Model
        >>> def validate_me(field, value):
        ...     if value == "Invalid":
        ...         return (field, "Invalid value")
        ...
        >>> class Foo(Model):
        ...     bar = Attribute(validator=validate_me)
        ...
        >>> f = Foo()
        >>> f.bar = "Invalid"
        >>> f.save()
        False
        >>> f.errors
        ['bar', 'Invalid value']
        .. WARNING::
        You may want to use ``validate`` described below to validate your model
        """
        self.errors = []
        for field in self.fields:
            try:
                field.validate(self)
            except FieldValidationException as e:
                self.errors.extend(e.errors)
        self.validate()
        return not bool(self.errors)

    def validate_attrs(self, **kwargs):
        self._errors = []
        instance = copy.deepcopy(self)#copy all values to another object #discarded anyway
        for attr, value in kwargs.items():
            field = self.attributes.get(attr)
            if not field:
                raise ValidationException('Field not found: %s' % attr)
            setattr(instance, attr, value)
            try:
                field.validate(instance)
            except FieldValidationException as e:
                self._errors.extend(e.errors)
        return not bool(self._errors)

    def validate(self):
        """
        Overriden in the model class.
        The function is here to help you validate your model.
        The validation should add errors to self._errors.
        Example:
        >>> from zaih_core.redis_index import Model
        >>> class Foo(Model):
        ...     name = Attribute(required=True)
        ...     def validate(self):
        ...         if self.name == "Invalid":
        ...             self._errors.append(('name', 'cannot be Invalid'))
        ...
        >>> f = Foo(name="Invalid")
        >>> f.save()
        False
        >>> f.errors
        [('name', 'cannot be Invalid')]
        """
        pass


    
    #transaction.set_modify_conditions(Transaction.transaction_status.ne(TRANSACTION_STATUS_COMPLETED))
    def set_modify_conditions(self, *args):
        instance = self
        _condition_expression = None
        _expression_attribute_values = {}
        for field_inst, exp, is_key in args:
            if is_key:
                pass
            else:
                if not _condition_expression:
                    _condition_expression = exp
                else:
                    _condition_expression = _condition_expression & exp
        instance._condition_expression = _condition_expression
        instance._expression_attribute_values = _expression_attribute_values
        return instance


    def set_field(self, field_name, value, update_now=False):
        if(isinstance(field_name, Attribute)):
            field = field_name
        else:
            field = self.attributes[field_name]
        if(isinstance(field, (CharField)) and not value):
            value = None
        if(value==None and isinstance(field, (ListField, SetField, DictField))): 
            #cannot set list or set field to NOne
            return self
        self.pending_updates.append(field.set(value))
        setattr(self, field.name, value)
        return self
    
    def add_to_field(self, field_name, value, update_now=False):
        if(not value): return self
        if(isinstance(field_name, Attribute)):
            field = field_name
        else:
            field = self.attributes[field_name]
        self.pending_updates.append(field.add(value))
        cur_value = getattr(self, field.name)
        if(not cur_value):
            setattr(self, field.name, value)
        else:
            setattr(self, field.name, cur_value + value)
        return self
    
    def update_to_dict_field(self, field_name, _dict):
        if(isinstance(field_name, Attribute)):
            field = field_name
        else:
            field = self.attributes[field_name]
            
        cur_value = getattr(self, field.name)
        if(cur_value==None):
            cur_val = _dict
            setattr(self, field.name, cur_val)
            self.pending_updates.append(field.set(cur_val))
        else:
            cur_value.update(_dict)
            self.pending_updates.append(field.update_dict(_dict))
            
        return self
    
    def remove_from_dict_field(self, field_name, keys):
        if(isinstance(field_name, Attribute)):
            field = field_name
        else:
            field = self.attributes[field_name]
            
        cur_value = getattr(self, field.name)
        if(not cur_value):
            return self
        else:
            for i in keys:
                cur_value.pop(i, None)
            self.pending_updates.append(field.remove_from_dict(keys))
            
        return self
    
    def append_to_listfield(self, field_name, value, path=None, update_now=False):
        if(not value): return self
        if(not isinstance(value, list)): #should be a list
            value = [value]
        if(isinstance(field_name, Attribute)):
            field = field_name
        else:
            field = self.attributes[field_name]
        cur_value = getattr(self, field.name)
        if(cur_value==None):#empty list field
            setattr(self, field.name, value)
            return self.set_field(field_name, value, update_now)#reset field
        
        self.pending_updates.append(field.list_append(value, path=path))
        cur_value.append(value)
        setattr(self, field.name, cur_value)
        return self
   
    def replace_by_index_in_listfield(self, field_name, index, value, path=None, update_now=False):
        if(isinstance(field_name, Attribute)):
            field = field_name
        else:
            field = self.attributes[field_name]
        self.pending_updates.append(field.list_append(value, path=path, index=index))
        cur_value = getattr(self, field.name, [])
        if(cur_value):
            if(index>=len(cur_value)):
                cur_value.append(value)
            else:
                cur_value[index] = value
            setattr(self, field.name, cur_value)
        return self
    
    def remove_by_index_from_listfield(self, field_name, index, path=None, remove_from_list=False, update_now =False):
        if(isinstance(field_name, Attribute)):
            field = field_name
        else:
            field = self.attributes[field_name]
        self.pending_updates.append(field.remove(path=path, indexes=[index]))
        cur_value = getattr(self, field.name)
        if(cur_value and remove_from_list):
            cur_value.pop(index)
        return self
            
    def add_to_setfield(self, field_name, values, update_now=False):
        if(not values or not isinstance(values, set)): return self
        
        if(isinstance(field_name, Attribute)):
            field = field_name
        else:
            field = self.attributes[field_name]
        cur_value = getattr(self, field.name)
        if(not cur_value):
            setattr(self, field.name, values)
            return self.set_field(field_name, values, update_now)#reset field
            
        else:
            self.pending_updates.append(field.add(values))
            cur_value.update(values)
        return self

    def delete_from_setfield(self, field_name, values, path=None, update_now=False):
        if(not values or not isinstance(values, set)): return self
        
        if(isinstance(field_name, Attribute)):
            field = field_name
        else:
            field = self.attributes[field_name]
        self.pending_updates.append(field.delete(values))
        cur_value = getattr(self, field.name)
        if(cur_value):
            for i in values:
                cur_value.discard(i)            
            
        return self
    
    def commit_changes(self):
        #parse through all changes in fields
        ret = self
        if(self.pending_updates):
            ret = self.update(*self.pending_updates)#as list of updates
            del self.pending_updates[:]
            self._condition_expression = None
            self._expression_attribute_values = None
        return ret
        #TODO: iterate through all fields and compare and update accordingly, all at once



    def update_attributes(self, **kwargs):
        """
        Updates the attributes of the model.
        >>> class Foo(Model):
        ...    name = Attribute()
        ...    title = Attribute()
        ...
        >>> f = Foo(name="Einstein", title="Mr.")
        >>> f.update_attributes(name="Tesla")
        >>> f.name
        'Tesla'
        """
        params = {}
        attrs = self.attributes.values()
        for att in attrs:
            if att.name in kwargs:
                att.__set__(self, kwargs[att.name])
                params[att.name] = kwargs[att.name]
        return params

    @property
    def attributes(self):
        """Return the attributes of the model.
        Returns a dict with models attribute name as keys
        and attribute descriptors as values.
        """
        # print self._attributes
        return dict(self._attributes)

    @property
    def fields(self):
        """Returns the list of field names of the model."""
        return list(self.attributes.values())

    def _get_values_for_read(self, values):
        read_values = {}
        for att, value in values.items():
            if att not in self.attributes:
                continue
            descriptor = self.attributes[att]
            _value = descriptor.typecast_for_read(value)
            read_values[att] = _value
        return read_values

    def _get_values_for_storage(self):
        data = {}
        if not self.is_valid():
            raise FieldValidationException(self.errors)
        for attr, field in self.attributes.items():
            value = getattr(self, attr)
            if value is not None:
                data[attr] = field.typecast_for_storage(value)
        return data

    @property
    def item(self):
        return self._get_values_for_storage()
    
    def to_son(self):
        data = {}
        for attr, field in self.attributes.items():
            value = getattr(self, attr)
            if value is not None:
                data[attr] = field.typecast_for_read(value)
        return data
    
        
    
