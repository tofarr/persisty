from servey.subscription.subscription import subscription

from messager.store.message import Message


on_create_message = subscription(
    Message, "on_create_message"
)  # Inform users of new messages
