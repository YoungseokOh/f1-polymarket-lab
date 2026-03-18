from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ExperimentSpec:
    stage: str
    model_family: str
    config_path: str
