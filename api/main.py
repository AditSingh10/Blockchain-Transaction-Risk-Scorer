"""Backward-compatible import path for the stateless API gateway."""

from services.api_gateway.main import app

__all__ = ["app"]
