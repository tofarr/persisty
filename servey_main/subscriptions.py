from servey.subscription.subscription import subscription

import servey_main.actions  # This means that the generated output exists...
from persisty.servey import output

# noinspection PyUnresolvedReferences
on_create_message = subscription(output.Message, "on_create_message")  # Inform users of new messages
