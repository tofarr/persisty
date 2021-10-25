import itertools
import logging
from functools import lru_cache
from typing import Dict, List, Union, Optional, Iterable, Iterator

import boto3
from boto3.dynamodb.conditions import ConditionBase, Key, Not
from botocore.exceptions import ClientError
from marshy.types import ExternalItemType

from persisty.util import filter_none

logger = logging.getLogger(__name__)


def search(table_name: str,
           key: Union[ConditionBase, Dict, None] = None,
           index_name: Optional[str] = None,
           filter_expression: Optional[ConditionBase] = None,
           exclusive_start_key: Optional[Dict] = None,
           projected_attributes: Optional[List[str]] = None
           ) -> Iterator[ExternalItemType]:
    logger.debug(f'search:table:{table_name}')
    table = dynamodb_table(table_name)
    kwargs = dict(KeyConditionExpression=_build_key_condition(key),
                  IndexName=index_name,
                  FilterExpression=filter_expression,
                  ExclusiveStartKey=exclusive_start_key)
    kwargs = filter_none(kwargs)
    if projected_attributes:
        kwargs['ProjectionExpression'] = ','.join(projected_attributes)
        kwargs['Select'] = 'SPECIFIC_ATTRIBUTES'
    logger.debug(f'search:kwargs:{kwargs}')
    while True:
        if key:
            response = table.query(**kwargs)
        else:
            response = table.scan(**kwargs)
        _check_response(response)
        for item in response['Items']:
            yield item
        if 'LastEvaluatedKey' in response:
            kwargs['ExclusiveStartKey'] = response['LastEvaluatedKey']
        else:
            return


def count(table_name: str,
          key: Union[ConditionBase, Dict, None] = None,
          index_name: Optional[str] = None,
          filter_expression: Optional[ConditionBase] = None
          ) -> int:
    logger.debug(f'count:table:{table_name}')
    table = dynamodb_table(table_name)
    kwargs = dict(KeyConditionExpression=_build_key_condition(key),
                  IndexName=index_name,
                  FilterExpression=filter_expression)
    kwargs = filter_none(kwargs)
    logger.debug(f'count:kwargs:{kwargs}')
    count_ = 0
    while True:
        if key:
            response = table.query(**kwargs)
        else:
            response = table.scan(**kwargs)
        count_ += response['Count']
        if 'LastEvaluatedKey' in response:
            kwargs['ExclusiveStartKey'] = response['LastEvaluatedKey']
        else:
            logger.debug(f'count:return:{count_}')
            return count_


def create(table_name: str, key: ExternalItemType, values: ExternalItemType) -> ExternalItemType:
    logger.debug(f'create:{table_name}:{key}:{values}')
    table = dynamodb_table(table_name)
    data = dict(values, **key)
    response = table.put_item(
        Item=data,
        ConditionExpression=Not(_build_key_condition(key)),
    )
    _check_response(response)
    return data


def read(table_name: str, key: ExternalItemType, projected_attributes: Optional[List[str]] = None) -> ExternalItemType:
    logger.debug(f'read:{table_name}:{key}')
    table = dynamodb_table(table_name)
    kwargs = dict(Key=key)
    if projected_attributes:
        kwargs['ProjectionExpression'] = ','.join(projected_attributes)
    response = table.get_item(**kwargs)
    return response.get('Item')


def read_all(table_name: str,
             keys: Iterable[ExternalItemType],
             projected_attributes: List[str] = None
             ) -> Iterator[ExternalItemType]:
    """ Read all the batch keys given. Note: Does not enforce ordering """
    iterator = iter(keys)
    while True:
        batch_keys = list(itertools.islice(iterator, 100))
        logger.debug(f'read_all:{table_name}:{batch_keys}')
        if not batch_keys:
            return
        items = _read_batch(table_name, batch_keys, projected_attributes)
        for item in items:
            yield item


def _read_batch(table_name: str,
                batch_keys: List[ExternalItemType],
                projected_attributes: List[str]
                ) -> List[ExternalItemType]:
    table_request_items = dict(Keys=batch_keys)
    if projected_attributes:
        table_request_items['ProjectionExpression'] = ','.join(projected_attributes)
    kwargs = dict(RequestItems={table_name: table_request_items})
    table_responses = []
    while True:
        response = _dynamodb().batch_get_item(**kwargs)
        responses = response['Responses']
        table_responses.extend(responses[table_name])
        unprocessed_keys = response.get('UnprocessedKeys')  # Batch size would have been greater than 16 Mb
        if not unprocessed_keys:
            if len(table_responses) != len(batch_keys):  # missing key
                logger.info(f'_read_batch:missing_keys')
                table_responses = _nones_for_missing_keys(batch_keys, table_responses)
            return table_responses
        kwargs['RequestItems'] = unprocessed_keys


def _nones_for_missing_keys(keys: List[ExternalItemType],
                            items: List[ExternalItemType]
                            ) -> List[ExternalItemType]:
    including_nones = []
    item_iter = iter(items)
    item = next(item_iter, None)
    for key in keys:
        if item and key.items() <= item.items():
            including_nones.append(item)
            item = next(item_iter, None)
        else:
            including_nones.append(None)
    return including_nones


def update(table_name: str, key: ExternalItemType, values: ExternalItemType) -> ExternalItemType:
    logger.debug(f'update:{table_name}:{key}:{values}')
    for a in key:
        if a in values and values[a] != key[a]:
            raise ClientError(dict(
                Error=dict(Code='key_update',
                           Message=f'Cannot change the id during update: {key} : {values}')
            ), 'update')
    table = dynamodb_table(table_name)
    updates = _build_update(values)
    response = table.update_item(
        Key=key,
        ConditionExpression=_build_key_condition(key),
        UpdateExpression=updates['str'],
        ExpressionAttributeValues=updates['values'],
        ReturnValues='ALL_NEW'
    )
    _check_response(response)
    return response.get('Attributes')


def destroy(table_name: str, key: ExternalItemType) -> Optional[ExternalItemType]:
    logger.debug(f'destroy:{table_name}:{key}')
    table = dynamodb_table(table_name)
    response = table.delete_item(
        Key=key,
        ReturnValues='ALL_OLD'
    )
    attributes = response.get('Attributes')
    return attributes if attributes else None


@lru_cache()  # TODO: swap out for @cache when we switch to python 3.9
def _dynamodb():
    return boto3.resource('dynamodb')


# Get a dynamodb table object
# Since paginated calls call this expensive operation, it's cached
@lru_cache(maxsize=200)
def dynamodb_table(table_name: str):
    return _dynamodb().Table(table_name)


def _build_key_condition(key: Union[ConditionBase, Dict, None]) -> Optional[ConditionBase]:
    if not key:
        return None
    if isinstance(key, ConditionBase):
        return key
    condition = None
    for k, v in key.items():
        sub_condition = Key(k).eq(v)
        condition = condition & sub_condition if condition else sub_condition
    return condition


def _build_update(updates: ExternalItemType) -> Dict:
    update_str = 'set '
    update_list = []
    remove_list = []
    update_values = {}
    for k, v in updates.items():
        if v is None:
            remove_list.append(k)
        else:
            update_list.append('%s = :%s' % (k, k))
            update_values[':%s' % k] = v
    update_str += ', '.join(update_list)
    if remove_list:
        update_str += ' REMOVE ' + ', '.join(remove_list)
    return {
        'str': update_str,
        'values': update_values
    }


def _check_response(response: Dict):
    http_status = response['ResponseMetadata']['HTTPStatusCode']
    if http_status != 200:
        raise ClientError(f'Put failed with response: {response}', 'response')
