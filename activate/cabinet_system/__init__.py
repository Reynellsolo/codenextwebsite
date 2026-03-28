"""Isolated cabinet system package.

This package is intentionally decoupled from app.py so it can be uploaded and
reviewed independently before integration.
"""

from .router import router as cabinet_router, set_database
from .schema import init_cabinet_schema

__all__ = ["cabinet_router", "set_database", "init_cabinet_schema"]
