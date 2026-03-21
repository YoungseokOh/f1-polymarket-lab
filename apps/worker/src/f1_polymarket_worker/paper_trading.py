"""Paper trading engine for live simulation.

Takes model predictions and executes simulated trades against
live Polymarket prices, tracking positions, PnL, and risk limits.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


@dataclass(slots=True)
class PaperTradeConfig:
    """Configuration for paper trading."""

    max_position_size: float = 50.0
    max_daily_loss: float = 100.0
    min_edge: float = 0.05
    bet_size: float = 10.0
    fee_rate: float = 0.02
    max_open_positions: int = 20


@dataclass(slots=True)
class PaperPosition:
    """A single simulated position."""

    market_id: str
    token_id: str | None
    side: str
    quantity: float
    entry_price: float
    entry_time: datetime
    model_prob: float
    market_prob: float
    edge: float
    status: str = "open"
    exit_price: float | None = None
    exit_time: datetime | None = None
    realized_pnl: float | None = None


@dataclass(slots=True)
class PaperTradingEngine:
    """Simulates trading based on model predictions.

    Tracks positions, enforces risk limits, and logs all signals for
    post-session analysis.
    """

    config: PaperTradeConfig = field(default_factory=PaperTradeConfig)
    positions: list[PaperPosition] = field(default_factory=list)
    signals: list[dict[str, Any]] = field(default_factory=list)
    daily_pnl: float = 0.0
    total_pnl: float = 0.0

    @property
    def open_positions(self) -> list[PaperPosition]:
        return [p for p in self.positions if p.status == "open"]

    def _check_risk_limits(self) -> tuple[bool, str]:
        """Check if we can take a new position."""
        if len(self.open_positions) >= self.config.max_open_positions:
            return False, "max_open_positions_reached"
        if self.daily_pnl <= -self.config.max_daily_loss:
            return False, "daily_loss_limit_reached"
        return True, "ok"

    def evaluate_signal(
        self,
        *,
        market_id: str,
        token_id: str | None,
        model_prob: float,
        market_price: float,
        timestamp: datetime | None = None,
    ) -> dict[str, Any]:
        """Evaluate a model prediction as a trading signal.

        Returns a signal dict describing the action taken (or reason for skip).
        """
        ts = timestamp or datetime.now(tz=timezone.utc)
        edge = model_prob - market_price

        signal: dict[str, Any] = {
            "timestamp": ts.isoformat(),
            "market_id": market_id,
            "token_id": token_id,
            "model_prob": model_prob,
            "market_price": market_price,
            "edge": edge,
            "action": "skip",
            "reason": "",
        }

        if edge < self.config.min_edge:
            signal["reason"] = f"edge {edge:.4f} < min_edge {self.config.min_edge}"
            self.signals.append(signal)
            return signal

        allowed, reason = self._check_risk_limits()
        if not allowed:
            signal["reason"] = reason
            self.signals.append(signal)
            return signal

        # Check for existing position in same market
        for pos in self.open_positions:
            if pos.market_id == market_id:
                signal["reason"] = "duplicate_market"
                self.signals.append(signal)
                return signal

        # Execute paper trade
        entry_price = market_price
        fees = self.config.bet_size * self.config.fee_rate
        position = PaperPosition(
            market_id=market_id,
            token_id=token_id,
            side="buy_yes",
            quantity=self.config.bet_size,
            entry_price=entry_price,
            entry_time=ts,
            model_prob=model_prob,
            market_prob=market_price,
            edge=edge,
        )
        self.positions.append(position)

        signal["action"] = "buy_yes"
        signal["quantity"] = self.config.bet_size
        signal["entry_price"] = entry_price
        signal["fees"] = fees
        signal["reason"] = "signal_accepted"
        self.signals.append(signal)
        return signal

    def settle_position(
        self,
        market_id: str,
        outcome_yes: bool,
        *,
        timestamp: datetime | None = None,
    ) -> PaperPosition | None:
        """Settle an open position against the actual outcome."""
        ts = timestamp or datetime.now(tz=timezone.utc)

        for pos in self.open_positions:
            if pos.market_id != market_id:
                continue

            if outcome_yes:
                pnl = pos.quantity * (1.0 - pos.entry_price)
            else:
                pnl = -pos.quantity * pos.entry_price

            fees = pos.quantity * self.config.fee_rate
            pnl -= fees

            pos.status = "settled"
            pos.exit_price = 1.0 if outcome_yes else 0.0
            pos.exit_time = ts
            pos.realized_pnl = pnl
            self.daily_pnl += pnl
            self.total_pnl += pnl
            return pos

        return None

    def reset_daily(self) -> None:
        """Reset daily PnL counter (call at start of each trading day)."""
        self.daily_pnl = 0.0

    def summary(self) -> dict[str, Any]:
        """Return a summary of the paper trading session."""
        settled = [p for p in self.positions if p.status == "settled"]
        wins = [p for p in settled if (p.realized_pnl or 0) > 0]

        return {
            "total_signals": len(self.signals),
            "trades_executed": len([s for s in self.signals if s["action"] != "skip"]),
            "open_positions": len(self.open_positions),
            "settled_positions": len(settled),
            "win_count": len(wins),
            "loss_count": len(settled) - len(wins),
            "win_rate": len(wins) / len(settled) if settled else None,
            "total_pnl": self.total_pnl,
            "daily_pnl": self.daily_pnl,
        }

    def save_log(self, path: Path) -> Path:
        """Save signal log and position history to JSON."""
        path.parent.mkdir(parents=True, exist_ok=True)

        log = {
            "summary": self.summary(),
            "signals": self.signals,
            "positions": [
                {
                    "market_id": p.market_id,
                    "token_id": p.token_id,
                    "side": p.side,
                    "quantity": p.quantity,
                    "entry_price": p.entry_price,
                    "entry_time": p.entry_time.isoformat(),
                    "model_prob": p.model_prob,
                    "market_prob": p.market_prob,
                    "edge": p.edge,
                    "status": p.status,
                    "exit_price": p.exit_price,
                    "exit_time": p.exit_time.isoformat() if p.exit_time else None,
                    "realized_pnl": p.realized_pnl,
                }
                for p in self.positions
            ],
        }

        path.write_text(json.dumps(log, indent=2, default=str), encoding="utf-8")
        return path
