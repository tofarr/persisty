import logging

from marshy.factory.impl_marshaller_factory import register_impl
from marshy.marshaller_context import MarshallerContext

priority = 100
LOGGER = logging.getLogger(__name__)


def configure(context: MarshallerContext):
    from servey.security.authenticator.password_authenticator_abc import (
        PasswordAuthenticatorABC,
    )
    from messenger.user_authenticator import UserAuthenticator

    register_impl(PasswordAuthenticatorABC, UserAuthenticator, context)
