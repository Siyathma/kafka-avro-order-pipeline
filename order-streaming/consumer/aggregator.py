"""Running aggregation over processed order prices."""
from __future__ import annotations


class RunningAverage:
    """Maintains a running mean incrementally, without storing every value.

    Uses the update rule:  mean_n = mean_{n-1} + (x_n - mean_{n-1}) / n
    which is numerically stabler than summing then dividing.
    """

    def __init__(self) -> None:
        self._count = 0
        self._mean = 0.0

    def add(self, value: float) -> float:
        self._count += 1
        self._mean += (value - self._mean) / self._count
        return self._mean

    @property
    def count(self) -> int:
        return self._count

    @property
    def mean(self) -> float:
        return self._mean