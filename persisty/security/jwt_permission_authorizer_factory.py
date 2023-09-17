import dataclasses

from servey.security.authorizer.authorizer_abc import AuthorizerABC
from servey.security.authorizer.jwt_authorizer import JwtAuthorizer
from servey.security.authorizer.jwt_authorizer_factory import JwtAuthorizerFactory

from persisty.security.jwt_permission_authorizer_abc import JwtPermissionAuthorizerABC


class JwtPermissionAuthorizer(JwtPermissionAuthorizerABC, JwtAuthorizer):
    """Authorizer that also includes permissions"""


class JwtPermissionAuthorizerFactory(JwtAuthorizerFactory):
    priority = JwtAuthorizerFactory.priority + 5

    def create_authorizer(self) -> AuthorizerABC:
        authorizer = super().create_authorizer()
        if authorizer:
            # noinspection PyDataclass
            authorizer = JwtPermissionAuthorizer(**dataclasses.asdict(authorizer))
        return authorizer
