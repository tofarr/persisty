import os
from dataclasses import dataclass
from typing import Optional

from servey.security.authorization import Authorization

from persisty.data_storage.data_storage_abc import DataStorageABC
from persisty.data_storage.data_storage_factory_abc import DataStorageFactoryABC


@dataclass
class EnvDataStorageFactory(DataStorageFactoryABC):
    """
    Data storage factory which allows different storage mechanisms to be used depending on the environment
    """
    name: str
    description: Optional[str] = None

    def get_name(self) -> str:
        return self.name

    def get_description(self) -> Optional[str]:
        return self.description

    def create(self, authorization: Authorization) -> DataStorageABC:
        # Data Storage Factory:
        # If we are in lambda prod mode, then it must be S3 based.
        # If we are in hosted prod mode, then it can be S3 or Directory based.
        # If we are in develop mode, then it is Directory Based.
        # If we are in serverless mode, then we are generating the S3 bucket.
        # Would we ever need it to be specifiable?

        # Object Storage Factory:
        # If we are in prod mode - it can be either Dynamodb or Sql based - developer picks what they want for each
        #   instance and a global default
        # If we are in develop mode, then it is Memory Based
        # If we are in serverless mode, then we are generating Couldformation resources. THIS DOES NOT HANDLE SQL.
        #   we would need something like ALEMBIC for that.

        impl = os.environ.get('DATA_STORAGE_IMPL')
        if impl == 'S3':

        elif impl == 'MEM':

        else: # Impl == 'FILE'
            pass


def env_data_storage_factory():
