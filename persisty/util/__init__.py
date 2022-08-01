import base64
import hashlib
import json
import logging
import os
import re
import sys

from dataclasses import fields
from marshy.types import ExternalType, ExternalItemType

from persisty.util.undefined import UNDEFINED

_PATTERN = re.compile(r"(?<!^)(?=[A-Z])")


def to_base64(item: ExternalType) -> str:
    json_str = json.dumps(item)
    json_bytes = json_str.encode("utf-8")
    base64_bytes = base64.b64encode(json_bytes)
    base64_str = base64_bytes.decode("utf-8")
    return base64_str


def from_base64(base64_str: str) -> ExternalType:
    base64_bytes = base64_str.encode("utf-8")
    json_bytes = base64.b64decode(base64_bytes)
    json_str = json_bytes.decode("utf-8")
    item = json.loads(json_str)
    return item


def secure_hash(item) -> str:
    item_json = json.dumps(item)
    item_bytes = item_json.encode("utf-8")
    sha = hashlib.sha256()
    sha.update(item_bytes)
    hash_bytes = sha.digest()
    b64_bytes = base64.b64encode(hash_bytes)
    b64_str = b64_bytes.decode("utf-8")
    return b64_str


def filter_none(item: ExternalItemType) -> ExternalItemType:
    return {k: v for k, v in item.items() if v is not None}


LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()


def get_logger(name: str):
    """
    Get a logger with it's level set from the default environment variable for logging
    :param name: the name for the logger
    :return: a logger
    """
    logger = logging.getLogger(name)
    logger.addHandler(logging.StreamHandler(sys.stdout))
    logger.setLevel(LEVEL)
    return logger


def dataclass_to_params(dataclass):
    """Convert a dataclass to a dict, skipping any values which are UNDEFINED"""
    items = {
        field.name: getattr(dataclass, field.name)
        for field in fields(dataclass)
        if getattr(dataclass, field.name) is not UNDEFINED
    }
    return items


def to_snake_case(name: str) -> str:
    return _PATTERN.sub("_", name).lower()


def to_camel_case(name: str) -> str:
    return "".join(n.title() for n in name.split("_"))
