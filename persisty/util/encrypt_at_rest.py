"""
Use a key from an environment variable to do encryption at rest...
"""

import base64
import json
import os
import logging
from typing import Any

import pyaes


KEY_PARAM = "PERSISTY_SECRET_KEY"
logger = logging.getLogger(__name__)
_KEY = os.environ.get(KEY_PARAM)
if not _KEY:
    logging.getLogger().warning(
        f"NO SECRET KEY FOUND IN ENVIRONMENT SO USING DEFAULT. PLEASE SET {KEY_PARAM}! "
        f"THIS IS NOT SECURE!!!"
    )
    _KEY = "NOT_A_SECURE_KEY"
_KEY = _KEY.encode("utf-8")


def encrypt(item: Any) -> str:
    json_bytes = json.dumps(item).encode("utf-8")
    aes = pyaes.AESModeOfOperationCTR(_KEY)
    encrypted_bytes = aes.encrypt(json_bytes)
    b64_bytes = base64.b64encode(encrypted_bytes)
    b64_str = b64_bytes.decode("utf-8")
    return b64_str


def decrypt(encrypted: str) -> Any:
    b64_bytes = encrypted.encode("utf-8")
    encrypted_bytes = base64.b64decode(b64_bytes)
    aes = pyaes.AESModeOfOperationCTR(_KEY)
    json_bytes = aes.decrypt(encrypted_bytes)
    json_str = json_bytes.decode("utf-8")
    item = json.loads(json_str)
    return item
