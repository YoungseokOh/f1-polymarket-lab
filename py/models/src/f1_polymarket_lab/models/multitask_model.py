from __future__ import annotations

from dataclasses import dataclass

import torch
from torch import nn

HEADS = ("pole", "constructor_pole", "winner", "h2h")


@dataclass(frozen=True, slots=True)
class MultitaskModelConfig:
    input_dim: int
    hidden_dim: int = 128
    depth: int = 2
    dropout: float = 0.1


class ResidualBlock(nn.Module):
    def __init__(self, hidden_dim: int, dropout: float) -> None:
        super().__init__()
        self.net = nn.Sequential(
            nn.Linear(hidden_dim, hidden_dim),
            nn.ReLU(),
            nn.Dropout(dropout),
            nn.Linear(hidden_dim, hidden_dim),
        )
        self.norm = nn.LayerNorm(hidden_dim)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        return self.norm(x + self.net(x))


class MultitaskTabularModel(nn.Module):
    def __init__(self, config: MultitaskModelConfig) -> None:
        super().__init__()
        self.input_layer = nn.Sequential(
            nn.Linear(config.input_dim, config.hidden_dim),
            nn.ReLU(),
            nn.Dropout(config.dropout),
        )
        self.blocks = nn.Sequential(
            *(ResidualBlock(config.hidden_dim, config.dropout) for _ in range(config.depth))
        )
        self.heads = nn.ModuleDict(
            {
                head: nn.Sequential(
                    nn.Linear(config.hidden_dim, config.hidden_dim // 2),
                    nn.ReLU(),
                    nn.Linear(config.hidden_dim // 2, 1),
                )
                for head in HEADS
            }
        )

    def forward(self, x: torch.Tensor) -> dict[str, torch.Tensor]:
        hidden = self.blocks(self.input_layer(x))
        return {head: module(hidden).squeeze(-1) for head, module in self.heads.items()}
