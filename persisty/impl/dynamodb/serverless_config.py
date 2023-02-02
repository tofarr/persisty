import os
from typing import Iterator

from marshy.types import ExternalItemType
from servey.servey_aws.serverless.yml_config.yml_config_abc import YmlConfigABC, ensure_ref_in_file, create_yml_file

from persisty.attr.attr_type import AttrType
from persisty.finder.store_finder_abc import find_store_factories
from persisty.impl.default_store import DefaultStore
from persisty.impl.dynamodb.dynamodb_store_factory import DynamodbStoreFactory
from persisty.impl.dynamodb.dynamodb_table_store import DynamodbTableStore


class ServerlessConfig(YmlConfigABC):
    dynamodb_tables_yml_file: str = "serverless_servey/dynamodb_tables.yml"
    dynamodb_role_statements_yml_file: str = "serverless_servey/dynamodb_role_statements.yml"

    def configure(self, main_serverless_yml_file: str):
        if os.environ['SERVEY_SLS_SKIP_DYNAMO'] == '1':
            return
        ensure_ref_in_file(
            main_serverless_yml_file,
            ["resources"],
            self.dynamodb_tables_yml_file,
        )
        dynamodb_tables_yml = _build_dynamodb_tables_yml()
        create_yml_file(self.dynamodb_tables_yml_file, dynamodb_tables_yml)

        dynamodb_role_statements_yml = _build_dynamodb_role_statements_yml()
        create_yml_file(self.dynamodb_role_statements_yml_file, dynamodb_role_statements_yml)
        for i in range(len(dynamodb_role_statements_yml["iamRoleStatements"])):
            ensure_ref_in_file(
                main_serverless_yml_file,
                ["provider", "iamRoleStatements"],
                self.dynamodb_role_statements_yml_file,
                f"iamRoleStatements.{i}",
            )


def _build_dynamodb_tables_yml() -> ExternalItemType:
    resources = {}
    for store_factory in _find_dynamodb_store_factories():
        attrs_by_name = {a.name: a for a in store_factory.meta.attrs}
        attribute_definitions = []
        for attr_name in store_factory.meta.key_config.get_key_attrs():
            attr = attrs_by_name.pop(attr_name)
            attr_type = 'N' if attr.attr_type in (AttrType.INT, AttrType.FLOAT) else 'S'
            attribute_definitions.append({"AttributeName": attr.name, "AttributeType": attr_type})
        for index in store_factory.meta.indexes:
            for attr_name in index.attr_names:
                attr = attrs_by_name.pop(attr_name, None)
                if attr:
                    attr_type = 'N' if attr.attr_type in (AttrType.INT, AttrType.FLOAT) else 'S'
                    attribute_definitions.append({"AttributeName": attr.name, "AttributeType": attr_type})
        resources[store_factory.table_name.title().replace("_", "")] = {
            "Type": "AWS::DynamoDB::Table",
            "Properties": {
                "TableName": store_factory.table_name,
                "BillingMode": "PAY_PER_REQUEST",
                "AttributeDefinitions": attribute_definitions,
                "KeySchema": store_factory.index.to_schema(),
                "GlobalSecondaryIndexes": [
                    {
                        "IndexName": f"gsi__{'__'.join(index.attr_names)}",
                        "Projection": {"ProjectionType": "ALL"},
                        "KeySchema": [
                            {
                                "AttributeName": "subscription_name",
                                "KeyType": "RANGE" if i else "HASH",
                            }
                            for i, attr_name in enumerate(index.attr_names)
                        ],
                    }
                    for index in store_factory.meta.indexes
                ]
            }
        }
    resource_definitions = {"Resources": resources}
    return resource_definitions


def _build_dynamodb_role_statements_yml() -> ExternalItemType:
    resources = []
    for store_factory in _find_dynamodb_store_factories():
        resource_name = store_factory.table_name.replace('_', '')
        resources.append({
            "Fn::GetAtt": [resource_name, "Arn"]
        })
        for key in store_factory.global_secondary_indexes:
            resources.append({
                "Fn::Join": [
                    "/",
                    [
                        {
                            "Fn::GetAtt": [resource_name, "Arn"]
                        },
                        "index",
                        key,
                    ],
                ]
            })

    role_statements = [
        {
            "Effect": "Allow",
            "Action": [
                "dynamodb:BatchWriteItem",
                "dynamodb:DeleteItem",
                "dynamodb:GetItem",
                "dynamodb:PutItem",
                "dynamodb:Query",
            ],
            "Resource": resources,
        },
    ]
    subscription_policy = {"iamRoleStatements": role_statements}
    return subscription_policy


def _find_dynamodb_stores() -> Iterator[DynamodbStoreFactory]:
    for store in find_stores():
        if isinstance(store, DynamodbTableStore):
            yield store
        elif isinstance(store, DefaultStore):
            factory = DynamodbStoreFactory(store.store_meta)
            yield factory.create()
