import base64
from datetime import datetime
from typing import Optional

import bcrypt

from servey.security.authenticator.password_authenticator_abc import PasswordAuthenticatorABC
from servey.security.authorization import Authorization

from persisty.field.field_filter import FieldFilter, FieldFilterOp


class UserAuthenticator(PasswordAuthenticatorABC):

    def authenticate(self, username: str, password: str) -> Optional[Authorization]:
        import servey_main.actions  # This means that the generated output exists...
        from servey_main.storage import user_storage_factory
        storage = user_storage_factory.create()
        for item in storage.search_all(FieldFilter('username', FieldFilterOp.eq, username)):
            password_digest_base64_str = item['password_digest']
            password_digest_base64 = password_digest_base64_str.encode('UTF-8')
            password_digest = base64.b64decode(password_digest_base64)
            new_password_digest = bcrypt.hashpw(password.encode("UTF-8"), password_digest)
            if new_password_digest == password_digest:
                scopes = []
                if item['admin']:
                    scopes.append('admin')
                # We should add some oauth for the exp
                authorization = Authorization(item['id'], frozenset(scopes), datetime.now(), None)
                return authorization
