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
    from .multitask_model import MultitaskModelConfig, MultitaskTabularModel
    from .multitask_trainer import (
        MultitaskTrainerConfig,
        load_multitask_artifacts,
        save_multitask_artifacts,
        score_multitask_frame,
        train_multitask_split,
    )
    from .tuner import tune_xgb
else:
    try:
        from .lgbm_trainer import LGBMTrainerConfig, train_one_split_lgbm
    except (ImportError, OSError) as exc:
        _LGBM_IMPORT_ERROR = exc

        class LGBMTrainerConfig:  # type: ignore[no-redef]
            def __init__(self, *args: Any, **kwargs: Any) -> None:
                msg = (
                    "LightGBM support requires the optional 'lightgbm' dependency. "
                    "Install it with `make bootstrap` or "
                    "`uv sync --all-packages --group modeling`. "
                    "On macOS, ensure `libomp` is installed."
                )
                raise ImportError(msg) from _LGBM_IMPORT_ERROR

        def train_one_split_lgbm(*args: Any, **kwargs: Any) -> Any:
            msg = (
                "LightGBM support requires the optional 'lightgbm' dependency. "
                "Install it with `make bootstrap` or "
                "`uv sync --all-packages --group modeling`. "
                "On macOS, ensure `libomp` is installed."
            )
            raise ImportError(msg) from _LGBM_IMPORT_ERROR

    try:
        from .tuner import tune_xgb
    except (ImportError, OSError) as exc:
        _TUNER_IMPORT_ERROR = exc

        def tune_xgb(*args: Any, **kwargs: Any) -> Any:
            msg = (
                "Optuna tuning requires the optional 'optuna' dependency. "
                "Install it with `make bootstrap` or "
                "`uv sync --all-packages --group modeling`."
            )
            raise ImportError(msg) from _TUNER_IMPORT_ERROR

    try:
        from .multitask_model import MultitaskModelConfig, MultitaskTabularModel
    except (ImportError, OSError) as exc:
        _MULTITASK_IMPORT_ERROR = exc

        class MultitaskModelConfig:  # type: ignore[no-redef]
            def __init__(self, *args: Any, **kwargs: Any) -> None:
                msg = (
                    "Multitask modeling requires the optional 'torch' dependency. "
                    "Install it with `make bootstrap` or "
                    "`uv sync --all-packages --group modeling`."
                )
                raise ImportError(msg) from _MULTITASK_IMPORT_ERROR

        class MultitaskTabularModel:  # type: ignore[no-redef]
            def __init__(self, *args: Any, **kwargs: Any) -> None:
                msg = (
                    "Multitask modeling requires the optional 'torch' dependency. "
                    "Install it with `make bootstrap` or "
                    "`uv sync --all-packages --group modeling`."
                )
                raise ImportError(msg) from _MULTITASK_IMPORT_ERROR

    try:
        from .multitask_trainer import (
            MultitaskTrainerConfig,
            load_multitask_artifacts,
            save_multitask_artifacts,
            score_multitask_frame,
            train_multitask_split,
        )
    except (ImportError, OSError) as exc:
        _MULTITASK_TRAINER_IMPORT_ERROR = exc

        class MultitaskTrainerConfig:  # type: ignore[no-redef]
            def __init__(self, *args: Any, **kwargs: Any) -> None:
                msg = (
                    "Multitask training requires the optional 'torch' dependency. "
                    "Install it with `make bootstrap` or "
                    "`uv sync --all-packages --group modeling`."
                )
                raise ImportError(msg) from _MULTITASK_TRAINER_IMPORT_ERROR

        def train_multitask_split(*args: Any, **kwargs: Any) -> Any:
            msg = (
                "Multitask training requires the optional 'torch' dependency. "
                "Install it with `make bootstrap` or "
                "`uv sync --all-packages --group modeling`."
            )
            raise ImportError(msg) from _MULTITASK_TRAINER_IMPORT_ERROR

        def save_multitask_artifacts(*args: Any, **kwargs: Any) -> Any:
            msg = (
                "Multitask training requires the optional 'torch' dependency. "
                "Install it with `make bootstrap` or "
                "`uv sync --all-packages --group modeling`."
            )
            raise ImportError(msg) from _MULTITASK_TRAINER_IMPORT_ERROR

        def load_multitask_artifacts(*args: Any, **kwargs: Any) -> Any:
            msg = (
                "Multitask training requires the optional 'torch' dependency. "
                "Install it with `make bootstrap` or "
                "`uv sync --all-packages --group modeling`."
            )
            raise ImportError(msg) from _MULTITASK_TRAINER_IMPORT_ERROR

        def score_multitask_frame(*args: Any, **kwargs: Any) -> Any:
            msg = (
                "Multitask training requires the optional 'torch' dependency. "
                "Install it with `make bootstrap` or "
                "`uv sync --all-packages --group modeling`."
            )
            raise ImportError(msg) from _MULTITASK_TRAINER_IMPORT_ERROR

__all__ = [
    "ALL_FEATURES",
    "LGBMTrainerConfig",
    "MODELING_ORDER",
    "MultitaskModelConfig",
    "MultitaskTabularModel",
    "MultitaskTrainerConfig",
    "load_multitask_artifacts",
    "save_multitask_artifacts",
    "score_multitask_frame",
    "TrainResult",
    "WalkForwardSplit",
    "XGBTrainerConfig",
    "build_walk_forward_splits",
    "train_one_split",
    "train_one_split_lgbm",
    "train_multitask_split",
    "tune_xgb",
]
