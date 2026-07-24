from __future__ import annotations

import asyncio
import signal

from prometheus_client import start_http_server

from shared.config.settings import Settings
from shared.observability import configure_logging, configure_tracing


def initialize_service(settings: Settings) -> asyncio.Event:
    configure_logging(settings)
    configure_tracing(settings)
    start_http_server(settings.metrics_port)
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop_event.set)
    return stop_event
