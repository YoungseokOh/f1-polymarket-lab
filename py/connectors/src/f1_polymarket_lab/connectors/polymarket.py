from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential


class PolymarketConnector:
    gamma_base_url = "https://gamma-api.polymarket.com"
    clob_base_url = "https://clob.polymarket.com"
    data_base_url = "https://data-api.polymarket.com"
    market_ws_url = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

    def __init__(self) -> None:
        self.client = httpx.Client(timeout=20.0, headers={"User-Agent": "f1-polymarket-lab/0.1.0"})

    @retry(wait=wait_exponential(min=1, max=8), stop=stop_after_attempt(3), reraise=True)
    def _get_json(
        self,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        allow_404: bool = False,
    ) -> Any:
        response = self.client.get(url, params=params)
        if allow_404 and response.status_code == 404:
            return None
        response.raise_for_status()
        return response.json()

    def list_markets(
        self,
        *,
        limit: int = 100,
        offset: int = 0,
        active: bool | None = None,
        closed: bool | None = None,
        archived: bool | None = None,
        order: str = "updatedAt",
        ascending: bool = False,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {
            "limit": limit,
            "offset": offset,
            "order": order,
            "ascending": ascending,
        }
        if active is not None:
            params["active"] = str(active).lower()
        if closed is not None:
            params["closed"] = str(closed).lower()
        if archived is not None:
            params["archived"] = str(archived).lower()
        payload = self._get_json(f"{self.gamma_base_url}/markets", params=params)
        return list(payload)

    def iterate_markets(
        self,
        *,
        batch_size: int = 100,
        max_pages: int | None = None,
        active: bool | None = None,
        closed: bool | None = None,
        archived: bool | None = None,
        order: str = "updatedAt",
        ascending: bool = False,
    ) -> Iterator[tuple[int, list[dict[str, Any]]]]:
        offset = 0
        page_index = 0
        while True:
            batch = self.list_markets(
                limit=batch_size,
                offset=offset,
                active=active,
                closed=closed,
                archived=archived,
                order=order,
                ascending=ascending,
            )
            if not batch:
                return
            yield offset, batch
            if len(batch) < batch_size:
                return
            offset += batch_size
            page_index += 1
            if max_pages is not None and page_index >= max_pages:
                return

    def list_events(self, *, limit: int = 100, offset: int = 0) -> list[dict[str, Any]]:
        payload = self._get_json(
            f"{self.gamma_base_url}/events",
            params={"limit": limit, "offset": offset},
        )
        return list(payload)

    def iterate_events(
        self,
        *,
        batch_size: int = 100,
        max_pages: int | None = None,
    ) -> Iterator[tuple[int, list[dict[str, Any]]]]:
        offset = 0
        page_index = 0
        while True:
            batch = self.list_events(limit=batch_size, offset=offset)
            if not batch:
                return
            yield offset, batch
            if len(batch) < batch_size:
                return
            offset += batch_size
            page_index += 1
            if max_pages is not None and page_index >= max_pages:
                return

    def get_order_book(self, token_id: str) -> dict[str, Any] | None:
        payload = self._get_json(
            f"{self.clob_base_url}/book",
            params={"token_id": token_id},
            allow_404=True,
        )
        return None if payload is None else dict(payload)

    def get_midpoint(self, token_id: str) -> float | None:
        payload = self._get_json(
            f"{self.clob_base_url}/midpoint",
            params={"token_id": token_id},
            allow_404=True,
        )
        return None if payload is None else float(payload.get("mid"))

    def get_spread(self, token_id: str) -> float | None:
        payload = self._get_json(
            f"{self.clob_base_url}/spread",
            params={"token_id": token_id},
            allow_404=True,
        )
        return None if payload is None else float(payload.get("spread"))

    def get_last_trade_price(self, token_id: str) -> float | None:
        payload = self._get_json(
            f"{self.clob_base_url}/last-trade-price",
            params={"token_id": token_id},
            allow_404=True,
        )
        return None if payload is None else float(payload.get("price"))

    def get_price_history(self, token_id: str, *, fidelity: int = 60) -> list[dict[str, Any]]:
        payload = self._get_json(
            f"{self.clob_base_url}/prices-history",
            params={"market": token_id, "interval": "max", "fidelity": fidelity},
            allow_404=True,
        )
        if payload is None:
            return []
        return list(payload.get("history", []))

    def get_trades(self, condition_id: str, *, limit: int = 200) -> list[dict[str, Any]]:
        payload = self._get_json(
            f"{self.data_base_url}/trades",
            params={"market": condition_id, "limit": limit},
            allow_404=True,
        )
        return [] if payload is None else list(payload)

    def get_open_interest(self, condition_id: str) -> float | None:
        payload = self._get_json(
            f"{self.data_base_url}/open-interest",
            params={"market": condition_id},
            allow_404=True,
        )
        if payload is None:
            return None
        value = payload.get("openInterest") or payload.get("open_interest")
        return None if value is None else float(value)
