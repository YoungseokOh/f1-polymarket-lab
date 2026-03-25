from __future__ import annotations

from typing import TYPE_CHECKING, Any

from .stages import MODELING_ORDER
from .xgb_trainer import (
    ALL_FEATURES,
    TrainResult,
    WalkForwardSplit,
    XGBTrainerConfig,
    build_walk_forward_splits,
    train_one_split,
)

if TYPE_CHECKING:
    from .lgbm_trainer import LGBMTrainerConfig
    from .tuner import tune_xgb
else:
    try:
        from .lgbm_trainer import LGBMTrainerConfig, train_one_split_lgbm
    except ModuleNotFoundError as exc:
        _LGBM_IMPORT_ERROR = exc

        class LGBMTrainerConfig:  # type: ignore[no-redef]
            def __init__(self, *args: Any, **kwargs: Any) -> None:
                msg = (
                    "LightGBM support requires the optional 'lightgbm' dependency. "
                    "Install it with `uv sync --group modeling`."
                )
                raise ImportError(msg) from _LGBM_IMPORT_ERROR

        def train_one_split_lgbm(*args: Any, **kwargs: Any) -> Any:
            msg = (
                "LightGBM support requires the optional 'lightgbm' dependency. "
                "Install it with `uv sync --group modeling`."
            )
            raise ImportError(msg) from _LGBM_IMPORT_ERROR

    try:
        from .tuner import tune_xgb
    except ModuleNotFoundError as exc:
        _TUNER_IMPORT_ERROR = exc

        def tune_xgb(*args: Any, **kwargs: Any) -> Any:
            msg = (
                "Optuna tuning requires the optional 'optuna' dependency. "
                "Install it with `uv sync --group modeling`."
            )
            raise ImportError(msg) from _TUNER_IMPORT_ERROR

__all__ = [
    "ALL_FEATURES",
    "LGBMTrainerConfig",
    "MODELING_ORDER",
    "TrainResult",
    "WalkForwardSplit",
    "XGBTrainerConfig",
    "build_walk_forward_splits",
    "train_one_split",
    "train_one_split_lgbm",
    "tune_xgb",
]
