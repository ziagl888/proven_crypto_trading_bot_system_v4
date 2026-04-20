# core/shutdown.py
# Graceful shutdown coordination for all V4 processes.
#
# Usage in any script:
#
#   from core.shutdown import ShutdownHandler
#   shutdown = ShutdownHandler("INGESTION")
#
#   # In main loop:
#   while not shutdown.is_set():
#       do_work()
#
#   # In cleanup:
#   shutdown.wait()   # blocks until shutdown requested
#
# The handler registers SIGINT (Ctrl+C) and SIGTERM.
# On signal: sets the shutdown event, logs cleanly, gives threads time to finish.

from __future__ import annotations

import logging
import signal
import threading

logger = logging.getLogger(__name__)


class ShutdownHandler:
    """
    Thread-safe shutdown coordinator.
    Registers SIGINT + SIGTERM handlers on creation.
    """

    def __init__(self, name: str, timeout: int = 10) -> None:
        """
        name:    process name for logging
        timeout: seconds to wait for threads to finish on shutdown
        """
        self.name    = name
        self.timeout = timeout
        self._event  = threading.Event()

        signal.signal(signal.SIGINT,  self._handle)
        signal.signal(signal.SIGTERM, self._handle)

    def _handle(self, signum, frame) -> None:
        sig_name = "SIGINT (Ctrl+C)" if signum == signal.SIGINT else "SIGTERM"
        if not self._event.is_set():
            logger.info(
                f"[{self.name}] {sig_name} received — "
                f"shutting down gracefully (max {self.timeout}s)..."
            )
            self._event.set()

    def is_set(self) -> bool:
        """Returns True if shutdown has been requested."""
        return self._event.is_set()

    def wait(self, timeout: float | None = None) -> bool:
        """
        Blocks until shutdown is requested or timeout expires.
        Returns True if shutdown was requested, False on timeout.
        """
        return self._event.wait(timeout=timeout)

    def sleep(self, seconds: float) -> bool:
        """
        Interruptible sleep — wakes up early if shutdown is requested.
        Returns True if shutdown was requested during sleep.
        """
        return self._event.wait(timeout=seconds)
