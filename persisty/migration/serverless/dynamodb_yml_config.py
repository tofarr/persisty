from marshy.types import ExternalItemType
from servey.servey_aws.serverless.yml_config.yml_config_abc import YmlConfigABC, ensure_ref_in_file, create_yml_file

from persisty.finder.store_finder_abc import find_stores
from persisty.impl.default_store import DefaultStore
from persisty.impl.dynamodb.dynamodb_store_factory import DynamodbStoreFactory
from persisty.impl.dynamodb.dynamodb_table_store import DynamodbTableStore


class DynamodbYmlConfig(YmlConfigABC):
    """
    Set up some aspect of the serverless environment yml files. (For example, functions, resources, etc...)
    """

    dynamodb_resource_yml_file: str = "serverless_servey/dynamodb_resource.yml"
    dynamodb_role_statement_yml_file: str = "serverless_servey/dynamodb_role_statement.yml"

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
        create_yml_file(self.dynamodb_role_statement_yml_file, dynamodb_role_statement_yml)

    @staticmethod
    def get_dynamodb_store_factories():
        stores = find_stores()
        for store in stores:
            if isinstance(store, DynamodbTableStore) or isinstance(store, DefaultStore):
                factory = DynamodbStoreFactory(store.meta)
                factory.derive_from_meta()
                yield factory

    def build_dynamodb_resource_yml(self) -> ExternalItemType:
        resources = {}
        for factory in self.get_dynamodb_store_factories():
            resources[factory.table_name.title().replace('_', '')] = {
                "Type": "AWS::DynamoDB::Table",
                "Properties": {
                    "TableName": factory.table_name,
                    "AttributeDefinitions": factory.get_attribute_definitions(),
                    "KeySchema": factory.index.to_schema(),
                    "GlobalSecondaryIndexes": factory.get_global_secondary_indexes(),
                    "BillingMode": "PAY_PER_REQUEST"
                }
            }
        return {
            "Resources": resources
        }

    def build_dynamodb_role_statement_yml(self) -> ExternalItemType:
        iam_role_statements = []
        for factory in self.get_dynamodb_store_factories():
            resource_name = factory.table_name.title().replace('_', '')
            iam_role_statements.append(self._iam_role_statement({"Fn::GetAtt": [resource_name, "Arn"]}))
            for index_name, index in factory.global_secondary_indexes.items():
                iam_role_statements.append(self._iam_role_statement({
                    "Fn::Join": ["/", [{"Fn::GetAtt": [resource_name, "Arn"]}, "index", index_name]]
                }))
        return {
            "iamRoleStatements": iam_role_statements
        }

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
            "Resource": resource
        }
        return result
