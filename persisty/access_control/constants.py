from dataclasses import fields

from persisty.access_control.access_control import AccessControl

ALL_ACCESS = AccessControl(**{f.name: True for f in fields(AccessControl)})
READ_ONLY = AccessControl(readable=True, searchable=True)
NO_ACCESS = AccessControl()
