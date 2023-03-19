import base64
from datetime import datetime
from typing import Optional

import bcrypt

from servey.security.authenticator.password_authenticator_abc import (
    PasswordAuthenticatorABC,
)
from servey.security.authorization import Authorization

from persisty.attr.attr_filter import AttrFilter, AttrFilterOp


class UserAuthenticator(PasswordAuthenticatorABC):
    priority: int = 200

    def authenticate(self, username: str, password: str) -> Optional[Authorization]:
        from servey_main.store import user_store

        for item in user_store.search_all(
            AttrFilter("username", AttrFilterOp.eq, username)
        ):
            password_digest_base64_str = item.password_digest
            password_digest_base64 = password_digest_base64_str.encode("UTF-8")
            password_digest = base64.b64decode(password_digest_base64)
            new_password_digest = bcrypt.hashpw(
                password.encode("UTF-8"), password_digest
            )
            if new_password_digest == password_digest:
                scopes = []
                if item.admin:
                    scopes.append("admin")
                # We should add some oauth for the exp
                authorization = Authorization(
                    str(item.id), frozenset(scopes), datetime.now(), None
                )
                return authorization
