"""heartbeat-hermes: thin adapter over one heartbeat-core child for Hermes."""

from .plugin import register

__all__ = ["register"]
