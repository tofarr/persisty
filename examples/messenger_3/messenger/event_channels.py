from servey.event_channel.websocket.websocket_event_channel import websocket_event_channel

from messenger.store.message import Message


on_create_message = websocket_event_channel(
    "on_create_message", Message
)  # Inform users of new messages
