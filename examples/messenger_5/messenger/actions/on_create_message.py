from servey.action.action import action

from persisty.trigger.after_create_trigger import AfterCreateTrigger

from messenger.store.message import Message


# noinspection PyUnresolvedReferences
@action(triggers=AfterCreateTrigger("message"))
def on_create_message(message: Message):
    """This action simply passes created messages to a subscription"""
    from messenger.event_channels import (
        on_create_message as on_create_message_channel,
    )

    on_create_message_channel.publish(message)
