from __future__ import annotations

import json
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from .openf1 import OpenF1Connector


def _mqtt() -> Any:
    try:
        import paho.mqtt.client as mqtt
    except ModuleNotFoundError as exc:
        msg = "paho-mqtt is required for OpenF1LiveConnector – pip install paho-mqtt"
        raise ImportError(msg) from exc
    return mqtt


@dataclass(frozen=True, slots=True)
class OpenF1LiveMessage:
    topic: str
    payload: Any
    observed_at: datetime


class OpenF1LiveConnector:
    mqtt_host = "mqtt.openf1.org"
    mqtt_port = 8883

    def __init__(self, *, username: str, password: str) -> None:
        self.username = username
        self.password = password
        self.auth_client = OpenF1Connector()

    def stream(
        self,
        *,
        topics: Sequence[str],
        on_message: Callable[[OpenF1LiveMessage], None],
        stop_after_seconds: float,
        message_limit: int | None = None,
    ) -> int:
        access_token = self.auth_client.fetch_access_token(
            username=self.username,
            password=self.password,
        )
        mqtt = _mqtt()
        client = mqtt.Client()
        client.tls_set()
        client.username_pw_set(self.username or "openf1-live", access_token)

        message_count = 0

        def _on_connect(
            mqtt_client: Any,
            _userdata: Any,
            _flags: Any,
            _reason_code: Any,
            _properties: Any = None,
        ) -> None:
            for topic in topics:
                mqtt_client.subscribe(topic)

        def _on_message(_client: Any, _userdata: Any, msg: Any) -> None:
            nonlocal message_count
            payload_text = msg.payload.decode("utf-8")
            try:
                payload: Any = json.loads(payload_text)
            except json.JSONDecodeError:
                payload = payload_text
            on_message(
                OpenF1LiveMessage(
                    topic=str(msg.topic),
                    payload=payload,
                    observed_at=datetime.now(tz=timezone.utc),
                )
            )
            message_count += 1
            if message_limit is not None and message_count >= message_limit:
                _client.disconnect()

        client.on_connect = _on_connect
        client.on_message = _on_message
        client.connect(self.mqtt_host, self.mqtt_port, keepalive=30)
        client.loop_start()

        deadline = time.monotonic() + max(0.0, stop_after_seconds)
        try:
            while client.is_connected() and time.monotonic() < deadline:
                time.sleep(0.25)
        finally:
            if client.is_connected():
                client.disconnect()
            client.loop_stop()
        return message_count
