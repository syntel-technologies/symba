"""Symba — a standalone, Postgres-backed job execution engine."""

__version__ = "0.1.0"

# Wire protocol version = engine MAJOR.MINOR. The engine advertises this in
# handshakes and rejects SDK versions outside its supported range.
PROTOCOL_VERSION = "0.1"
