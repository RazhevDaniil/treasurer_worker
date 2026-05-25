"""Entry point. Supports two import contexts:

  1. Production (CWD=mail_app/, no package): `from app.main import app`.
  2. Tests / project root (mail_app importable as a package via
     `mail_app/__init__.py`): `from .app.main import app`.

The try/except keeps both work without per-environment branching.
"""

try:
    from .app.main import app  # type: ignore[import-not-found]
except ImportError:
    from app.main import app  # noqa: F401

__all__ = ["app"]
