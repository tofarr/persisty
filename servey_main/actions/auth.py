import base64
from datetime import datetime
from typing import Optional

import bcrypt
from servey.action.action import action
from servey.security.authenticator.password_authenticator_abc import get_default_password_authenticator
from servey.security.authorization import AuthorizationError, Authorization
from servey.security.authorizer.authorizer_factory_abc import get_default_authorizer
from servey.trigger.web_trigger import WEB_POST

from persisty.field.field_filter import FieldFilter, FieldFilterOp
import servey_main.actions  # This means that the generated output exists...
from servey_main.storage import user_storage_factory


@action(triggers=(WEB_POST,))
def sign_up(
        username: str,
        password: str,
        full_name: Optional[str] = None,
        email_address: Optional[str] = None
) -> str:
    password_digest = bcrypt.hashpw(password.encode('UTF-8'), bcrypt.gensalt())
    password_digest_base64 = base64.b64encode(password_digest)
    password_digest_base64_str = password_digest_base64.decode("UTF-8")
    storage = user_storage_factory.create()
    existing_user = next(storage.search_all(FieldFilter('username', FieldFilterOp.eq, username)), None)
    if existing_user:
        raise AuthorizationError()
    result = storage.create(dict(
        username=username,
        password_digest=password_digest_base64_str,
        full_name=full_name,
        email_address=email_address
    ))
    authorization = Authorization(result['id'], frozenset(), datetime.now(), None)
    authorizer = get_default_authorizer()
    token = authorizer.encode(authorization)
    return token


@action(triggers=(WEB_POST,))
def login(username: str, password: str) -> Optional[str]:
    authenticator = get_default_password_authenticator()
    authorization = authenticator.authenticate(username, password)
    if authorization:
        authorizer = get_default_authorizer()
        token = authorizer.encode(authorization)
        return token
