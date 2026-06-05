"""Gamma scalper: target-position-based hedge (target delta 0)."""

from dataclasses import dataclass
from typing import Any, Optional


@dataclass
class HedgeOrder:
    """Proposed hedge: side and quantity in shares."""

    side: str  # 'BUY' or 'SELL'
    quantity: int


@dataclass
class TargetPosition:
    """Target stock position in shares (delta-neutral target). Emitted by strategy only; execution subscribes."""

    target_shares: int
    reason: str = ""
    ts: float = 0.0
    trace_id: Optional[str] = None


@dataclass
class HedgeIntent:
    """Hedge intent: target, side, quantity, and whether to force (e.g. D3)."""

    target_shares: int
    side: str  # 'BUY' or 'SELL'
    quantity: int
    force_hedge: bool = False


def compute_target_position(portfolio_delta: float, stock_shares: int) -> int:
    """Target stock position for delta 0: target = -option_delta_shares."""
    opt_delta_shares = portfolio_delta - stock_shares
    return int(round(-opt_delta_shares))


def compute_target_and_need(
    portfolio_delta: float,
    stock_shares: int,
) -> tuple[float, float]:
    """
    Target-position framing: target delta 0.
    target_shares = -opt_delta_shares (opt_delta = port_delta - stock_shares)
    need = target_shares - stock_shares = -port_delta

    Returns (target_shares, need).
    """
    opt_delta_shares = portfolio_delta - stock_shares
    target_shares = -opt_delta_shares
    need = target_shares - stock_shares
    return target_shares, need


def gamma_scalper_hedge(
    portfolio_delta: float,
    stock_shares: int,
    threshold_hedge_shares: float = 25.0,
    max_hedge_shares_per_order: int = 500,
) -> Optional[HedgeOrder]:
    """
    Target-position-based hedge: need = target_shares - pos_shares.
    Only hedge when |need| >= threshold.
    Returns HedgeOrder(side, quantity) or None.
    """
    _, need = compute_target_and_need(portfolio_delta, stock_shares)
    if need > threshold_hedge_shares:
        qty = min(int(round(need)), max_hedge_shares_per_order)
        if qty <= 0:
            return None
        return HedgeOrder(side="BUY", quantity=qty)
    if need < -threshold_hedge_shares:
        qty = min(int(round(-need)), max_hedge_shares_per_order)
        if qty <= 0:
            return None
        return HedgeOrder(side="SELL", quantity=qty)
    return None


def gamma_scalper_intent(
    portfolio_delta: float,
    stock_shares: int,
    threshold_hedge_shares: float = 25.0,
    max_hedge_shares_per_order: int = 500,
    config: Optional[Any] = None,
) -> Optional[HedgeIntent]:
    """
    Intent-only: returns HedgeIntent when |need| >= threshold; no direct order.
    config can provide threshold_hedge_shares / max_hedge_shares_per_order overrides.
    """
    cfg = config or {}
    threshold = cfg.get("threshold_hedge_shares", threshold_hedge_shares)
    max_qty = cfg.get("max_hedge_shares_per_order", max_hedge_shares_per_order)
    _, need = compute_target_and_need(portfolio_delta, stock_shares)
    target = compute_target_position(portfolio_delta, stock_shares)
    if need > threshold:
        qty = min(int(round(need)), max_qty)
        if qty <= 0:
            return None
        return HedgeIntent(
            target_shares=target, side="BUY", quantity=qty, force_hedge=False
        )
    if need < -threshold:
        qty = min(int(round(-need)), max_qty)
        if qty <= 0:
            return None
        return HedgeIntent(
            target_shares=target, side="SELL", quantity=qty, force_hedge=False
        )
    return None
