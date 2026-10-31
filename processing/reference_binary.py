"""Trusted binary response envelope; streams own their read-pin/budget lease."""
from dataclasses import dataclass, field


@dataclass
class BinaryResponse:
    stream: object = field(repr=False)
    status: int = 200
    headers: dict = field(default_factory=dict)
