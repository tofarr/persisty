from marshy.types import ExternalItemType
from servey.servey_aws.serverless.yml_config.yml_config_abc import (
    YmlConfigABC,
    ensure_ref_in_file,
    create_yml_file,
)

from persisty.factory.store_factory import StoreFactory
from persisty.finder.store_meta_finder_abc import find_store_meta
from persisty.impl.dynamodb.dynamodb_store_factory import DynamodbStoreFactory


class DynamodbYmlConfig(YmlConfigABC):
    """
    Set up some aspect of the serverless environment yml files. (For example, functions, resources, etc...)
    """

    dynamodb_resource_yml_file: str = "serverless_servey/dynamodb_resource.yml"
    dynamodb_role_statement_yml_file: str = (
        "serverless_servey/dynamodb_role_statement.yml"
    )

    def configure(self, main_serverless_yml_file: str):
        ensure_ref_in_file(
            main_serverless_yml_file,
            ["resources"],
            self.dynamodb_resource_yml_file,
        )
        ensure_ref_in_file(
            main_serverless_yml_file,
            ["provider", "iamRoleStatements"],
            self.dynamodb_role_statement_yml_file,
            "iamRoleStatements.0",
        )
        dynamodb_resource_yml = self.build_dynamodb_resource_yml()
        create_yml_file(self.dynamodb_resource_yml_file, dynamodb_resource_yml)
        dynamodb_role_statement_yml = self.build_dynamodb_role_statement_yml()
        create_yml_file(
            self.dynamodb_role_statement_yml_file, dynamodb_role_statement_yml
        )

    @staticmethod
    def get_dynamodb_store_meta():
        for store_meta in find_store_meta():
            if isinstance(
                store_meta.store_factory, (DynamodbStoreFactory, StoreFactory)
            ):
                yield store_meta

    def build_dynamodb_resource_yml(self) -> ExternalItemType:
        resources = {}
        for store_meta in self.get_dynamodb_store_meta():
            factory = DynamodbStoreFactory()
            factory.derive_from_meta(store_meta)
            resources[factory.table_name.title().replace("_", "")] = {
                "Type": "AWS::DynamoDB::Table",
                "Properties": {
                    "TableName": factory.table_name,
                    "AttributeDefinitions": factory.get_attribute_definitions(
                        store_meta
                    ),
                    "KeySchema": factory.index.to_schema(),
                    "GlobalSecondaryIndexes": factory.get_global_secondary_indexes(),
                    "BillingMode": "PAY_PER_REQUEST",
                },
            }
        return {"Resources": resources}

    def build_dynamodb_role_statement_yml(self) -> ExternalItemType:
        resources = []
        for store_meta in self.get_dynamodb_store_meta():
            factory = DynamodbStoreFactory()
            factory.derive_from_meta(store_meta)
            resource_name = factory.table_name.title().replace("_", "")
            resources.append({"Fn::GetAtt": [resource_name, "Arn"]})
            for index_name in factory.global_secondary_indexes.keys():
                resources.append(
                    {
                        "Fn::Join": [
                            "/",
                            [
                                {"Fn::GetAtt": [resource_name, "Arn"]},
                                "index",
                                index_name,
                            ],
                        ]
                    }
                )
        return {"iamRoleStatements": [self._iam_role_statement(resources)]}

    @staticmethod
    def _iam_role_statement(resource):
        result = {
            "Effect": "Allow",
            "Action": [
                "dynamodb:DescribeTable",
                "dynamodb:Query",
                "dynamodb:Scan",
                "dynamodb:BatchGetItem",
                "dynamodb:GetItem",
                "dynamodb:PutItem",
                "dynamodb:UpdateItem",
                "dynamodb:DeleteItem",
            ],
            "Resource": resource,
        }
        return result
