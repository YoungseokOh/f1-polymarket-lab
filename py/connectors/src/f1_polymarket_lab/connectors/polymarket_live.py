from __future__ import annotations

import asyncio
import json
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any


def _ws_connect():  # noqa: ANN202
    try:
        from websockets.asyncio.client import connect  # type: ignore[import-untyped]
    except ModuleNotFoundError as exc:
        msg = "websockets is required for PolymarketLiveConnector – pip install websockets"
        raise ImportError(msg) from exc
    return connect


@dataclass(frozen=True, slots=True)
class PolymarketLiveMessage:
    payload: Any
    observed_at: datetime


class PolymarketLiveConnector:
    market_ws_url = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

    async def _stream_async(
        self,
        *,
        asset_ids: Sequence[str],
        on_message: Callable[[PolymarketLiveMessage], None],
        stop_after_seconds: float,
        message_limit: int | None = None,
    ) -> int:
        message_count = 0
        connect = _ws_connect()
        async with connect(self.market_ws_url) as websocket:
            await websocket.send(json.dumps({"assets_ids": list(asset_ids), "type": "market"}))
            deadline = asyncio.get_running_loop().time() + max(0.0, stop_after_seconds)
            while asyncio.get_running_loop().time() < deadline:
                remaining = deadline - asyncio.get_running_loop().time()
                if remaining <= 0:
                    break
                try:
                    raw_message = await asyncio.wait_for(websocket.recv(), timeout=remaining)
                except TimeoutError:
                    break
                payload = json.loads(raw_message)
                payloads = payload if isinstance(payload, list) else [payload]
                for item in payloads:
                    on_message(
                        PolymarketLiveMessage(
                            payload=item,
                            observed_at=datetime.now(tz=timezone.utc),
                        )
                    )
                    message_count += 1
                    if message_limit is not None and message_count >= message_limit:
                        return message_count
        return message_count

    def stream_market_messages(
        self,
        *,
        asset_ids: Sequence[str],
        on_message: Callable[[PolymarketLiveMessage], None],
        stop_after_seconds: float,
        message_limit: int | None = None,
    ) -> int:
        return asyncio.run(
            self._stream_async(
                asset_ids=asset_ids,
                on_message=on_message,
                stop_after_seconds=stop_after_seconds,
                message_limit=message_limit,
            )
        )
