"""Local-only metrics collector implementations.

This module previously implemented telemetry features that sent trade volume
metrics to an external cloud service. To comply with strict on-premise
requirements, all remote reporting functionality has been removed.  Only a
``DummyMetricsCollector`` placeholder remains so that existing calls do not
break, but it performs no network activity.
"""

import logging
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.connector_base import ConnectorBase

class MetricsCollector(ABC):

    @abstractmethod
    def start(self):
        raise NotImplementedError

    @abstractmethod
    def stop(self):
        raise NotImplementedError

    @abstractmethod
    def process_tick(self, timestamp: float):
        raise NotImplementedError


class DummyMetricsCollector(MetricsCollector):

    def start(self):
        # Nothing is required
        pass

    def stop(self):
        # Nothing is required
        pass

    def process_tick(self, timestamp: float):
        # Nothing is required
        pass
