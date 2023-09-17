import dataclasses

from servey.security.authorizer.authorizer_abc import AuthorizerABC
from servey.servey_aws.authorizer.kms_authorizer import KmsAuthorizer
from servey.servey_aws.authorizer.kms_authorizer_factory import KmsAuthorizerFactory

from persisty.security.jwt_permission_authorizer_abc import JwtPermissionAuthorizerABC


class KmsPermissionAuthorizer(JwtPermissionAuthorizerABC, KmsAuthorizer):
    """Authorizer that also includes permissions"""


class KmsPermissionAuthorizerFactory(KmsAuthorizerFactory):
    priority = KmsAuthorizerFactory.priority + 5

    def create_authorizer(self) -> AuthorizerABC:
        authorizer = super().create_authorizer()
        if authorizer:
            # noinspection PyDataclass
            authorizer = KmsPermissionAuthorizer(**dataclasses.asdict(authorizer))
        return authorizer
