from abc import ABC
from typing import Optional, Tuple

import marshy
from marshy.types import ExternalItemType
from servey.security.authorization import Authorization
from servey.security.authorizer.jwt_authorizer_abc import (
    JwtAuthorizerABC,
    date_from_jwt,
)

from persisty.security.named_permission import NamedPermission
from persisty.security.permission_authorization import PermissionAuthorization


class JwtPermissionAuthorizerABC(JwtAuthorizerABC, ABC):
    @staticmethod
    def authorization_from_decoded(decoded: ExternalItemType):
        scope = decoded.get("scope")
        scopes = tuple()
        if scope:
            scopes = scope.split(" ")
        authorization = PermissionAuthorization(
            subject_id=decoded.get("sub"),
            not_before=date_from_jwt(decoded, "nbf"),
            expire_at=date_from_jwt(decoded, "exp"),
            scopes=frozenset(scopes),
            stores_permissions=marshy.load(
                Optional[Tuple[NamedPermission, ...]], decoded.get("permissions")
            ),
        )
        return authorization

    @staticmethod
    def payload_from_authorization(authorization: Authorization, iss: str, aud: str):
        payload = JwtAuthorizerABC.payload_from_authorization(authorization, iss, aud)
        permissions = getattr(authorization, "permissions", None)
        if permissions:
            payload["permissions"] = marshy.dump(permissions)
        return payload
