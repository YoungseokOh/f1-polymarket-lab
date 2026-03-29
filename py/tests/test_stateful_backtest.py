from __future__ import annotations

from f1_polymarket_worker.backtest import CheckpointPolicyConfig, _decide_checkpoint_action


def test_decide_checkpoint_action_opens_when_edge_is_strong() -> None:
    config = CheckpointPolicyConfig()

    action = _decide_checkpoint_action(
        current_edge=0.12,
        current_position=0.0,
        config=config,
    )

    assert action == "open"


def test_decide_checkpoint_action_adds_to_existing_position() -> None:
    config = CheckpointPolicyConfig()

    action = _decide_checkpoint_action(
        current_edge=0.11,
        current_position=10.0,
        config=config,
    )

    assert action == "add"


def test_decide_checkpoint_action_closes_when_edge_collapses() -> None:
    config = CheckpointPolicyConfig()

    action = _decide_checkpoint_action(
        current_edge=-0.01,
        current_position=10.0,
        config=config,
    )

    assert action == "close"
