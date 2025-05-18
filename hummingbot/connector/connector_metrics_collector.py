"""Local metrics collectors for on-premise deployments.

This module provides a simple Prometheus-based trade volume collector
that replaces the previous cloud reporting functionality. Trade volume
is aggregated locally and exposed via a Prometheus metrics endpoint so
that users can build their own Grafana dashboards without sharing data
with external services.
"""

import logging
import platform
from abc import ABC, abstractmethod
from decimal import Decimal
from typing import TYPE_CHECKING, List, Tuple

from prometheus_client import Counter, start_http_server

from hummingbot.connector.utils import combine_to_hb_trading_pair, split_hb_trading_pair
from hummingbot.core.event.event_forwarder import EventForwarder
from hummingbot.core.event.events import MarketEvent, OrderFilledEvent
from hummingbot.core.rate_oracle.rate_oracle import RateOracle
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.connector_base import ConnectorBase


PROM_SERVER_STARTED = False


class MetricsCollector(ABC):
    """Abstract interface for connector metrics collectors."""

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
    """No-op collector used when metrics are disabled."""

    def start(self):
        pass

    def stop(self):
        pass

    def process_tick(self, timestamp: float):
        pass


class PrometheusTradeVolumeCollector(MetricsCollector):
    """Collects filled trade volume and exposes it locally via Prometheus."""

    _logger = None
    METRIC_NAME = "filled_usdt_volume"

    def __init__(
        self,
        connector: "ConnectorBase",
        activation_interval: Decimal,
        rate_provider: RateOracle,
        instance_id: str,
        valuation_token: str = "USDT",
        metrics_port: int = 9100,
    ):
        super().__init__()
        self._connector = connector
        self._activation_interval = activation_interval
        self._rate_provider = rate_provider
        self._instance_id = instance_id
        self._valuation_token = valuation_token
        self._last_process_tick_timestamp = 0
        self._last_executed_collection_process = None
        self._collected_events: List[OrderFilledEvent] = []
        self._fill_event_forwarder = EventForwarder(self._register_fill_event)
        self._event_pairs: List[Tuple[MarketEvent, EventForwarder]] = [
            (MarketEvent.OrderFilled, self._fill_event_forwarder),
        ]

        global PROM_SERVER_STARTED
        if not PROM_SERVER_STARTED:
            start_http_server(metrics_port)
            PROM_SERVER_STARTED = True

        self._volume_counter = Counter(
            self.METRIC_NAME,
            "Total filled volume converted to USDT",
            ["instance_id", "exchange", "client_version", "system"],
        )
        with open("VERSION") as vf:
            self._client_version = vf.read().strip()
        self._system = f"{platform.system()} {platform.release()}({platform.platform()})"

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    def start(self):
        for event_pair in self._event_pairs:
            self._connector.add_listener(event_pair[0], event_pair[1])

    def stop(self):
        self.trigger_metrics_collection_process()
        for event_pair in self._event_pairs:
            self._connector.remove_listener(event_pair[0], event_pair[1])

    def process_tick(self, timestamp: float):
        inactivity_time = timestamp - self._last_process_tick_timestamp
        if inactivity_time >= self._activation_interval:
            self._last_process_tick_timestamp = timestamp
            self.trigger_metrics_collection_process()

    def trigger_metrics_collection_process(self):
        events_to_process = self._collected_events
        self._collected_events = []
        self._last_executed_collection_process = safe_ensure_future(
            self.collect_metrics(events=events_to_process)
        )

    async def collect_metrics(self, events: List[OrderFilledEvent]):
        total_volume = Decimal("0")
        for fill_event in events:
            trade_base, trade_quote = split_hb_trading_pair(fill_event.trading_pair)
            from_quote_conversion_pair = combine_to_hb_trading_pair(base=trade_quote, quote=self._valuation_token)
            rate = await self._rate_provider.stored_or_live_rate(from_quote_conversion_pair)
            if rate is not None:
                total_volume += fill_event.amount * fill_event.price * rate
            else:
                from_base_conversion_pair = combine_to_hb_trading_pair(base=trade_base, quote=self._valuation_token)
                rate = await self._rate_provider.stored_or_live_rate(from_base_conversion_pair)
                if rate is not None:
                    total_volume += fill_event.amount * rate
                else:
                    self.logger().debug(
                        "Could not find a conversion rate using Rate Oracle for any of "
                        f"the pairs {from_quote_conversion_pair} or {from_base_conversion_pair}"
                    )

        if total_volume > Decimal("0"):
            self._dispatch_trade_volume(total_volume)

    def _dispatch_trade_volume(self, volume: Decimal):
        self._volume_counter.labels(
            instance_id=self._instance_id,
            exchange=self._connector.name,
            client_version=self._client_version,
            system=self._system,
        ).inc(float(volume))

    def _register_fill_event(self, event: OrderFilledEvent):
        self._collected_events.append(event)
