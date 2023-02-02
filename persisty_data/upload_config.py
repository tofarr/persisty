from dataclasses import dataclass
from datetime import datetime
from typing import Optional, List

from servey.trigger.web_trigger import WebTriggerMethod

from persisty_data.form_field import FormField


@dataclass
class UploadConfig:
    """ Configuration for a presigned upload """
    url: str
    method: WebTriggerMethod = WebTriggerMethod.POST
    pre_populated_fields: Optional[List[FormField]] = None
    file_param: str = "file"
    expire_at: Optional[datetime] = None
