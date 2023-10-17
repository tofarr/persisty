import base64
from datetime import datetime
from typing import Optional

import bcrypt
from persisty.store.store_abc import get_store
from servey.action.action import action
from servey.security.authenticator.password_authenticator_abc import (
    get_default_password_authenticator,
)
from servey.security.authorization import AuthorizationError, Authorization
from servey.security.authorizer.authorizer_factory_abc import get_default_authorizer
from servey.trigger.web_trigger import WEB_POST

from persisty.search_filter.filter_factory import filter_factory
from messenger.store.user import User


@action(triggers=WEB_POST)
def sign_up(
    username: str,
    password: str,
    full_name: Optional[str] = None,
    email_address: Optional[str] = None,
) -> str:
    user_store = get_store(User)
    password_digest = bcrypt.hashpw(password.encode("UTF-8"), bcrypt.gensalt())
    password_digest_base64 = base64.b64encode(password_digest)
    password_digest_base64_str = password_digest_base64.decode("UTF-8")
    search_filter = filter_factory(User).username.eq(username)
    existing_user = next(user_store.search_all(search_filter), None)
    if existing_user:
        raise AuthorizationError()
    result = user_store.create(
        User(
            username=username,
            password_digest=password_digest_base64_str,
            full_name=full_name,
            email_address=email_address,
        )
    )
    authorization = Authorization(str(result.id), frozenset(), datetime.now(), None)
    authorizer = get_default_authorizer()
    token = authorizer.encode(authorization)
    return token


@action(triggers=WEB_POST)
def login(username: str, password: str) -> Optional[str]:
    authenticator = get_default_password_authenticator()
    authorization = authenticator.authenticate(username, password)
    if authorization:
        authorizer = get_default_authorizer()
        token = authorizer.encode(authorization)
        return token
