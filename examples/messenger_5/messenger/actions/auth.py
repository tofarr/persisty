import base64
import os
from datetime import datetime
from typing import Optional

import bcrypt
import boto3
from persisty.store.store_abc import get_store
from servey.action.action import action
from servey.cache_control.ttl_cache_control import TtlCacheControl
from servey.security.authenticator.password_authenticator_abc import (
    get_default_password_authenticator,
)
from servey.security.authorization import AuthorizationError, Authorization
from servey.security.authorizer.authorizer_factory_abc import get_default_authorizer
from servey.servey_aws import is_lambda_env
from servey.trigger.web_trigger import WEB_POST, WEB_GET

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


@action(triggers=WEB_GET, cache_control=TtlCacheControl(ttl=86400))
def get_appsync_api_key() -> Optional[str]:
    """Appsync requires the use of an API key, so we add the ability to get it"""
    if not is_lambda_env():
        return
    appsync_client = boto3.client("appsync")
    api_id = _get_api_id(appsync_client)
    if not api_id:
        return
    api_key = _get_api_key(appsync_client, api_id)
    return api_key


def _get_api_id(appsync_client) -> Optional[str]:
    kwargs = {}
    while True:
        response = appsync_client.list_graphql_apis(**kwargs)
        for graphql_api in response.get("graphqlApis") or []:
            if graphql_api["name"].lower() == os.environ["SERVEY_MAIN"]:
                return graphql_api["apiId"]
        if response["nextToken"]:
            kwargs["nextToken"] = response["nextToken"]
        else:
            return


def _get_api_key(appsync_client, api_id: str) -> Optional[str]:
    kwargs = {"apiId": api_id}
    while True:
        response = appsync_client.list_api_keys(**kwargs)
        for api_key in response.get("apiKeys") or []:
            if api_key.get("expires") > datetime.now().timestamp():
                return api_key["id"]
        if response["nextToken"]:
            kwargs["nextToken"] = response["nextToken"]
        else:
            return
