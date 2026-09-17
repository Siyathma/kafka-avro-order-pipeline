"""Failure taxonomy for the consumer.

Separating transient from permanent failures lets the processing loop
decide whether to retry (transient) or route straight to the DLQ (permanent).
"""


class TransientError(Exception):
    """A temporary failure worth retrying (e.g. a downstream timeout)."""


class PermanentError(Exception):
    """A failure that will never succeed on retry (e.g. invalid data).
    Should be routed to the DLQ immediately."""