from servey.action.action import action

from persisty.trigger.after_create_trigger import AfterCreateTrigger

from messager.models.message import Message


# noinspection PyUnresolvedReferences
@action(triggers=AfterCreateTrigger("message"))
def on_create_message(message: Message):
    """This action simply passes created messages to a subscription"""
    from messager.subscriptions import (
        on_create_message as on_create_message_subscription,
    )

    on_create_message_subscription.publish(message)
