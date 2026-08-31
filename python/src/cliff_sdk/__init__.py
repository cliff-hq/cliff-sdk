"""Python SDK for Cliff's ingest protocol. See PROTOCOL.md at the repo root."""

from .client import (
    PROTOCOL_VERSION,
    SDK_VERSION,
    WIRE_MAJOR,
    Client,
    IngestError,
    Signal,
    connect,
)

__version__ = SDK_VERSION
__all__ = [
    "connect",
    "Client",
    "Signal",
    "IngestError",
    "SDK_VERSION",
    "PROTOCOL_VERSION",
    "WIRE_MAJOR",
]
