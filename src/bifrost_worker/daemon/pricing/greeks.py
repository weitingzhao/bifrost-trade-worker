"""Portfolio greeks: delta, gamma, valid flag for state space."""

from typing import List

from bifrost_core.portfolio.positions.portfolio import OptionLeg, portfolio_delta, portfolio_gamma


class Greeks:
    """Computes portfolio delta/gamma from legs; exposes valid if computation succeeded."""

    def __init__(
        self,
        option_legs: List[OptionLeg],
        stock_shares: int,
        spot: float,
        risk_free_rate: float,
        volatility: float,
    ):
        self._legs = option_legs
        self._stock_shares = stock_shares
        self._spot = spot
        self._r = risk_free_rate
        self._vol = volatility
        self._delta: float = 0.0
        self._gamma: float = 0.0
        self._valid = False
        self._recompute()

    def _recompute(self) -> None:
        if self._spot <= 0:
            self._valid = False
            self._delta = 0.0
            self._gamma = 0.0
            return
        try:
            self._delta = portfolio_delta(
                self._legs, self._stock_shares, self._spot, self._r, self._vol
            )
            self._gamma = portfolio_gamma(self._legs, self._spot, self._r, self._vol)
            # Consider invalid if delta/gamma are NaN
            self._valid = (
                self._delta == self._delta and self._gamma == self._gamma
            )  # not NaN
        except Exception:
            self._valid = False
            self._delta = 0.0
            self._gamma = 0.0

    @property
    def delta(self) -> float:
        return self._delta

    @property
    def gamma(self) -> float:
        return self._gamma

    @property
    def valid(self) -> bool:
        return self._valid
