"""SEPA fundamentals Celery feed helpers (Massive REST upserts + job runners).

Migrated from engine src/research/sepa/financials_data.py — executed by Celery workers only.
Gap detection / readiness queries remain in bifrost_api.research.sepa.financials_data.
"""

from __future__ import annotations

import logging
import time
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple

from psycopg2.extras import Json

logger = logging.getLogger(__name__)

SOURCE_DEFAULT = "massive"

def _parse_date(val: Any) -> Optional[date]:
    if val is None:
        return None
    if isinstance(val, date) and not isinstance(val, datetime):
        return val
    s = str(val).strip()[:10]
    if len(s) < 10:
        return None
    try:
        return date.fromisoformat(s)
    except ValueError:
        return None


def _f(row: Dict[str, Any], key: str) -> Optional[float]:
    v = row.get(key)
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _f_any(row: Dict[str, Any], *keys: str) -> Optional[float]:
    """First usable numeric among alternative JSON keys (vendors rename fields; includes negatives and zero).

    Do not chain with ``or`` — legitimate ``0`` must not fall through to a fallback key.
    """
    for k in keys:
        out = _f(row, k)
        if out is not None:
            return out
    return None


def _i(row: Dict[str, Any], key: str) -> Optional[int]:
    v = row.get(key)
    if v is None:
        return None
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return None


def _sym_from_row(row: Dict[str, Any], fallback: Optional[str] = None) -> str:
    tn = row.get("ticker")
    if tn is not None:
        sym = str(tn).strip().upper()
        if sym:
            return sym
    sn = row.get("symbol")
    if sn is not None:
        sym = str(sn).strip().upper()
        if sym:
            return sym
    t = row.get("tickers")
    if isinstance(t, list) and t:
        return str(t[0]).strip().upper()
    if fallback:
        return fallback.strip().upper()
    return ""


def _normalize_massive_statement_timeframe(raw: Any) -> str:
    """Canonical timeframe for financial statement PKs (matches Massive ``results[].timeframe`` wording).

    Request params may use ``ttm`` while bodies use ``trailing_twelve_months``; store one canonical value.
    """
    s = str(raw or "").strip().lower()
    if not s:
        return "quarterly"
    if s in ("ttm", "trailing_twelve_months", "trailing-12-months"):
        return "trailing_twelve_months"
    return s


def _normalize_sec_cik(raw: Any) -> Optional[str]:
    """SEC CIK as zero-padded 10-digit text when numeric (JSON sometimes emits integers and drops leading zeros)."""
    if raw is None or raw == "":
        return None
    s = str(raw).strip()
    if not s:
        return None
    if s.isdigit() and len(s) <= 10:
        return s.zfill(10)
    return s


# Bound columns for ``upsert_cash_flow_rows`` (order MUST match DDL ``stock_cash_flows`` and INSERT list).
_STOCK_CASH_FLOW_UPSERT_BIND_COLUMNS: Tuple[str, ...] = (
    "symbol",
    "timeframe",
    "period_end",
    "filing_date",
    "fiscal_year",
    "fiscal_quarter",
    "cik",
    "cash_from_operating_activities_continuing_operations",
    "change_in_cash_and_equivalents",
    "change_in_other_operating_assets_and_liabilities_net",
    "depreciation_depletion_and_amortization",
    "dividends",
    "effect_of_currency_exchange_rate",
    "income_loss_from_discontinued_operations",
    "long_term_debt_issuances_repayments",
    "net_cash_from_financing_activities",
    "net_cash_from_financing_activities_continuing_operations",
    "net_cash_from_financing_activities_discontinued_operations",
    "net_cash_from_investing_activities",
    "net_cash_from_investing_activities_continuing_operations",
    "net_cash_from_investing_activities_discontinued_operations",
    "net_cash_from_operating_activities",
    "net_cash_from_operating_activities_discontinued_operations",
    "net_income",
    "noncontrolling_interests",
    "other_cash_adjustments",
    "other_financing_activities",
    "other_investing_activities",
    "other_operating_activities",
    "purchase_of_property_plant_and_equipment",
    "sale_of_property_plant_and_equipment",
    "short_term_debt_issuances_repayments",
    "source",
)


def _cash_flow_bind_tuple(
    row: Dict[str, Any],
    *,
    sym: str,
    tf: str,
    pe: Any,
    fd: Optional[Any],
    fy: int,
    fq: int,
    cik_v: Optional[str],
    source: str,
) -> Tuple[Any, ...]:
    """Build execute() params in :data:`_STOCK_CASH_FLOW_UPSERT_BIND_COLUMNS` order."""
    cf_cont = _f_any(row, "cash_from_operating_activities_continuing_operations")
    chg = _f_any(
        row,
        "change_in_cash_and_equivalents",
        "net_change_in_cash_and_equivalents",
        "net_change_in_cash",
    )
    dep = _f_any(row, "depreciation_depletion_and_amortization", "depreciation_and_amortization")
    fin_tot = _f_any(
        row,
        "net_cash_from_financing_activities",
        "net_cash_flow_from_financing_activities",
        "net_cash_flow_from_financingactivities",
        "net_cash_from_financing_activities_continuing_operations",
    )
    inv_tot = _f_any(
        row,
        "net_cash_from_investing_activities",
        "net_cash_flow_from_investing_activities",
        "net_cash_flow_from_investingactivities",
        "net_cash_from_investing_activities_continuing_operations",
    )
    op_tot = _f_any(
        row,
        "net_cash_from_operating_activities",
        "net_cash_flow_from_operating_activities",
        "net_cash_flow_from_operatingactivities",
        "cash_from_operating_activities_continuing_operations",
    )
    ppe = _f_any(
        row,
        "purchase_of_property_plant_and_equipment",
        "capital_expenditure",
        "capital_expenditures",
    )
    return (
        sym,
        tf,
        pe,
        fd,
        fy,
        fq,
        cik_v,
        cf_cont,
        chg,
        _f_any(row, "change_in_other_operating_assets_and_liabilities_net"),
        dep,
        _f_any(row, "dividends"),
        _f_any(row, "effect_of_currency_exchange_rate"),
        _f_any(row, "income_loss_from_discontinued_operations"),
        _f_any(row, "long_term_debt_issuances_repayments"),
        fin_tot,
        _f_any(row, "net_cash_from_financing_activities_continuing_operations"),
        _f_any(row, "net_cash_from_financing_activities_discontinued_operations"),
        inv_tot,
        _f_any(row, "net_cash_from_investing_activities_continuing_operations"),
        _f_any(row, "net_cash_from_investing_activities_discontinued_operations"),
        op_tot,
        _f_any(row, "net_cash_from_operating_activities_discontinued_operations"),
        _f_any(row, "net_income"),
        _f_any(row, "noncontrolling_interests"),
        _f_any(row, "other_cash_adjustments"),
        _f_any(row, "other_financing_activities"),
        _f_any(row, "other_investing_activities"),
        _f_any(row, "other_operating_activities"),
        ppe,
        _f_any(row, "sale_of_property_plant_and_equipment"),
        _f_any(row, "short_term_debt_issuances_repayments"),
        source,
    )


_STOCK_INCOME_UPSERT_BIND_COLUMNS: Tuple[str, ...] = (
    "symbol",
    "timeframe",
    "period_end",
    "filing_date",
    "fiscal_year",
    "fiscal_quarter",
    "basic_earnings_per_share",
    "diluted_earnings_per_share",
    "revenue",
    "basic_shares_outstanding",
    "diluted_shares_outstanding",
    "consolidated_net_income_loss",
    "cost_of_revenue",
    "gross_profit",
    "operating_income",
    "total_operating_expenses",
    "selling_general_administrative",
    "research_development",
    "depreciation_depletion_amortization",
    "ebitda",
    "interest_income",
    "interest_expense",
    "other_income_expense",
    "total_other_income_expense",
    "income_before_income_taxes",
    "income_taxes",
    "net_income_loss_attributable_common_shareholders",
    "noncontrolling_interest",
    "discontinued_operations",
    "extraordinary_items",
    "equity_in_affiliates",
    "preferred_stock_dividends_declared",
    "other_operating_expenses",
    "tickers",
    "cik",
    "source",
)


def _income_statement_bind_tuple(
    row: Dict[str, Any],
    *,
    sym: str,
    tf: str,
    pe: Any,
    fd: Optional[Any],
    fy: int,
    fq: int,
    cik_v: Optional[str],
    source: str,
) -> Tuple[Any, ...]:
    """Build execute() params in :data:`_STOCK_INCOME_UPSERT_BIND_COLUMNS` order."""
    return (
        sym,
        tf,
        pe,
        fd,
        fy,
        fq,
        _f(row, "basic_earnings_per_share"),
        _f(row, "diluted_earnings_per_share"),
        _f(row, "revenue"),
        _f(row, "basic_shares_outstanding"),
        _f(row, "diluted_shares_outstanding"),
        _f(row, "consolidated_net_income_loss"),
        _f(row, "cost_of_revenue"),
        _f(row, "gross_profit"),
        _f(row, "operating_income"),
        _f(row, "total_operating_expenses"),
        _f(row, "selling_general_administrative"),
        _f(row, "research_development"),
        _f(row, "depreciation_depletion_amortization"),
        _f(row, "ebitda"),
        _f(row, "interest_income"),
        _f(row, "interest_expense"),
        _f(row, "other_income_expense"),
        _f(row, "total_other_income_expense"),
        _f(row, "income_before_income_taxes"),
        _f(row, "income_taxes"),
        _f(row, "net_income_loss_attributable_common_shareholders"),
        _f(row, "noncontrolling_interest"),
        _f(row, "discontinued_operations"),
        _f(row, "extraordinary_items"),
        _f(row, "equity_in_affiliates"),
        _f(row, "preferred_stock_dividends_declared"),
        _f(row, "other_operating_expenses"),
        Json(tk) if (tk := row.get("tickers")) is not None else None,
        cik_v,
        source,
    )


def upsert_income_statement_rows(
    cur: Any,
    rows: List[Dict[str, Any]],
    *,
    fallback_symbol: Optional[str] = None,
    source: str = SOURCE_DEFAULT,
) -> int:
    """UPSERT Massive GET /stocks/financials/v1/income-statements ``results[]`` (column names match API)."""
    if not rows:
        return 0
    sql = """
    INSERT INTO public.stock_income_statements (
        symbol, timeframe, period_end, filing_date, fiscal_year, fiscal_quarter,
        basic_earnings_per_share, diluted_earnings_per_share, revenue,
        basic_shares_outstanding, diluted_shares_outstanding,
        consolidated_net_income_loss, cost_of_revenue, gross_profit, operating_income,
        total_operating_expenses, selling_general_administrative, research_development,
        depreciation_depletion_amortization, ebitda, interest_income, interest_expense,
        other_income_expense, total_other_income_expense, income_before_income_taxes,
        income_taxes, net_income_loss_attributable_common_shareholders,
        noncontrolling_interest, discontinued_operations, extraordinary_items,
        equity_in_affiliates, preferred_stock_dividends_declared, other_operating_expenses,
        tickers, cik, source, fetched_at
    ) VALUES (
        %s,%s,%s,%s,%s,%s,
        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,now()
    )
    ON CONFLICT (symbol, timeframe, period_end, source) DO UPDATE SET
        filing_date = EXCLUDED.filing_date,
        fiscal_year = EXCLUDED.fiscal_year,
        fiscal_quarter = EXCLUDED.fiscal_quarter,
        basic_earnings_per_share = EXCLUDED.basic_earnings_per_share,
        diluted_earnings_per_share = EXCLUDED.diluted_earnings_per_share,
        revenue = EXCLUDED.revenue,
        basic_shares_outstanding = EXCLUDED.basic_shares_outstanding,
        diluted_shares_outstanding = EXCLUDED.diluted_shares_outstanding,
        consolidated_net_income_loss = EXCLUDED.consolidated_net_income_loss,
        cost_of_revenue = EXCLUDED.cost_of_revenue,
        gross_profit = EXCLUDED.gross_profit,
        operating_income = EXCLUDED.operating_income,
        total_operating_expenses = EXCLUDED.total_operating_expenses,
        selling_general_administrative = EXCLUDED.selling_general_administrative,
        research_development = EXCLUDED.research_development,
        depreciation_depletion_amortization = EXCLUDED.depreciation_depletion_amortization,
        ebitda = EXCLUDED.ebitda,
        interest_income = EXCLUDED.interest_income,
        interest_expense = EXCLUDED.interest_expense,
        other_income_expense = EXCLUDED.other_income_expense,
        total_other_income_expense = EXCLUDED.total_other_income_expense,
        income_before_income_taxes = EXCLUDED.income_before_income_taxes,
        income_taxes = EXCLUDED.income_taxes,
        net_income_loss_attributable_common_shareholders = EXCLUDED.net_income_loss_attributable_common_shareholders,
        noncontrolling_interest = EXCLUDED.noncontrolling_interest,
        discontinued_operations = EXCLUDED.discontinued_operations,
        extraordinary_items = EXCLUDED.extraordinary_items,
        equity_in_affiliates = EXCLUDED.equity_in_affiliates,
        preferred_stock_dividends_declared = EXCLUDED.preferred_stock_dividends_declared,
        other_operating_expenses = EXCLUDED.other_operating_expenses,
        tickers = EXCLUDED.tickers,
        cik = EXCLUDED.cik,
        fetched_at = now()
    """
    _n_ph = sql.count("%s")
    if _n_ph != len(_STOCK_INCOME_UPSERT_BIND_COLUMNS):
        raise RuntimeError(
            "stock_income_statements INSERT placeholder mismatch: "
            f"{_n_ph} vs {len(_STOCK_INCOME_UPSERT_BIND_COLUMNS)}"
        )
    n = 0
    for row in rows:
        if not isinstance(row, dict):
            continue
        sym = _sym_from_row(row, fallback_symbol)
        if not sym:
            continue
        pe = _parse_date(row.get("period_end"))
        if not pe:
            continue
        tf = _normalize_massive_statement_timeframe(row.get("timeframe"))
        fy = int(row.get("fiscal_year") or 0)
        fq = int(row.get("fiscal_quarter") or 0)
        fd = _parse_date(row.get("filing_date"))
        cik_v = _normalize_sec_cik(row.get("cik"))
        bind = _income_statement_bind_tuple(
            row,
            sym=sym,
            tf=tf,
            pe=pe,
            fd=fd,
            fy=fy,
            fq=fq,
            cik_v=cik_v,
            source=source,
        )
        if len(bind) != len(_STOCK_INCOME_UPSERT_BIND_COLUMNS):
            raise RuntimeError("stock_income_statements bind tuple length drift")
        cur.execute(sql, bind)
        n += 1
    return n


_STOCK_BALANCE_UPSERT_BIND_COLUMNS: Tuple[str, ...] = (
    "symbol",
    "timeframe",
    "period_end",
    "filing_date",
    "fiscal_year",
    "fiscal_quarter",
    "accounts_payable",
    "accrued_and_other_current_liabilities",
    "accumulated_other_comprehensive_income",
    "additional_paid_in_capital",
    "cash_and_equivalents",
    "cik",
    "commitments_and_contingencies",
    "common_stock",
    "debt_current",
    "deferred_revenue_current",
    "goodwill",
    "intangible_assets_net",
    "inventories",
    "long_term_debt_and_capital_lease_obligations",
    "noncontrolling_interest",
    "other_assets",
    "other_current_assets",
    "other_equity",
    "other_noncurrent_liabilities",
    "preferred_stock",
    "property_plant_equipment_net",
    "receivables",
    "retained_earnings_deficit",
    "short_term_investments",
    "total_assets",
    "total_current_assets",
    "total_current_liabilities",
    "total_equity",
    "total_equity_attributable_to_parent",
    "total_liabilities",
    "total_liabilities_and_equity",
    "treasury_stock",
    "source",
)


def _balance_sheet_bind_tuple(
    row: Dict[str, Any],
    *,
    sym: str,
    tf: str,
    pe: Any,
    fd: Optional[Any],
    fy: int,
    fq: int,
    cik_v: Optional[str],
    source: str,
) -> Tuple[Any, ...]:
    """Build execute() params in :data:`_STOCK_BALANCE_UPSERT_BIND_COLUMNS` order."""
    return (
        sym,
        tf,
        pe,
        fd,
        fy,
        fq,
        _f(row, "accounts_payable"),
        _f(row, "accrued_and_other_current_liabilities"),
        _f(row, "accumulated_other_comprehensive_income"),
        _f(row, "additional_paid_in_capital"),
        _f(row, "cash_and_equivalents"),
        cik_v,
        _f(row, "commitments_and_contingencies"),
        _f(row, "common_stock"),
        _f(row, "debt_current"),
        _f(row, "deferred_revenue_current"),
        _f(row, "goodwill"),
        _f(row, "intangible_assets_net"),
        _f(row, "inventories"),
        _f(row, "long_term_debt_and_capital_lease_obligations"),
        _f(row, "noncontrolling_interest"),
        _f(row, "other_assets"),
        _f(row, "other_current_assets"),
        _f(row, "other_equity"),
        _f(row, "other_noncurrent_liabilities"),
        _f(row, "preferred_stock"),
        _f(row, "property_plant_equipment_net"),
        _f(row, "receivables"),
        _f(row, "retained_earnings_deficit"),
        _f(row, "short_term_investments"),
        _f(row, "total_assets"),
        _f(row, "total_current_assets"),
        _f(row, "total_current_liabilities"),
        _f(row, "total_equity"),
        _f(row, "total_equity_attributable_to_parent"),
        _f(row, "total_liabilities"),
        _f(row, "total_liabilities_and_equity"),
        _f(row, "treasury_stock"),
        source,
    )


def upsert_balance_sheet_rows(
    cur: Any,
    rows: List[Dict[str, Any]],
    *,
    fallback_symbol: Optional[str] = None,
    source: str = SOURCE_DEFAULT,
) -> int:
    """UPSERT Massive GET /stocks/financials/v1/balance-sheets ``results[]`` (column names match API)."""
    if not rows:
        return 0
    sql = """
    INSERT INTO public.stock_balance_sheets (
        symbol, timeframe, period_end, filing_date, fiscal_year, fiscal_quarter,
        accounts_payable, accrued_and_other_current_liabilities,
        accumulated_other_comprehensive_income, additional_paid_in_capital,
        cash_and_equivalents, cik, commitments_and_contingencies, common_stock,
        debt_current, deferred_revenue_current, goodwill, intangible_assets_net,
        inventories, long_term_debt_and_capital_lease_obligations, noncontrolling_interest,
        other_assets, other_current_assets, other_equity, other_noncurrent_liabilities,
        preferred_stock, property_plant_equipment_net, receivables, retained_earnings_deficit,
        short_term_investments, total_assets, total_current_assets, total_current_liabilities,
        total_equity, total_equity_attributable_to_parent, total_liabilities,
        total_liabilities_and_equity, treasury_stock, source, fetched_at
    ) VALUES (
        %s,%s,%s,%s,%s,%s,
        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,now()
    )
    ON CONFLICT (symbol, timeframe, period_end, source) DO UPDATE SET
        filing_date = EXCLUDED.filing_date,
        fiscal_year = EXCLUDED.fiscal_year,
        fiscal_quarter = EXCLUDED.fiscal_quarter,
        accounts_payable = EXCLUDED.accounts_payable,
        accrued_and_other_current_liabilities = EXCLUDED.accrued_and_other_current_liabilities,
        accumulated_other_comprehensive_income = EXCLUDED.accumulated_other_comprehensive_income,
        additional_paid_in_capital = EXCLUDED.additional_paid_in_capital,
        cash_and_equivalents = EXCLUDED.cash_and_equivalents,
        cik = EXCLUDED.cik,
        commitments_and_contingencies = EXCLUDED.commitments_and_contingencies,
        common_stock = EXCLUDED.common_stock,
        debt_current = EXCLUDED.debt_current,
        deferred_revenue_current = EXCLUDED.deferred_revenue_current,
        goodwill = EXCLUDED.goodwill,
        intangible_assets_net = EXCLUDED.intangible_assets_net,
        inventories = EXCLUDED.inventories,
        long_term_debt_and_capital_lease_obligations = EXCLUDED.long_term_debt_and_capital_lease_obligations,
        noncontrolling_interest = EXCLUDED.noncontrolling_interest,
        other_assets = EXCLUDED.other_assets,
        other_current_assets = EXCLUDED.other_current_assets,
        other_equity = EXCLUDED.other_equity,
        other_noncurrent_liabilities = EXCLUDED.other_noncurrent_liabilities,
        preferred_stock = EXCLUDED.preferred_stock,
        property_plant_equipment_net = EXCLUDED.property_plant_equipment_net,
        receivables = EXCLUDED.receivables,
        retained_earnings_deficit = EXCLUDED.retained_earnings_deficit,
        short_term_investments = EXCLUDED.short_term_investments,
        total_assets = EXCLUDED.total_assets,
        total_current_assets = EXCLUDED.total_current_assets,
        total_current_liabilities = EXCLUDED.total_current_liabilities,
        total_equity = EXCLUDED.total_equity,
        total_equity_attributable_to_parent = EXCLUDED.total_equity_attributable_to_parent,
        total_liabilities = EXCLUDED.total_liabilities,
        total_liabilities_and_equity = EXCLUDED.total_liabilities_and_equity,
        treasury_stock = EXCLUDED.treasury_stock,
        fetched_at = now()
    """
    _n_ph = sql.count("%s")
    if _n_ph != len(_STOCK_BALANCE_UPSERT_BIND_COLUMNS):
        raise RuntimeError(
            "stock_balance_sheets INSERT placeholder mismatch: "
            f"{_n_ph} vs {len(_STOCK_BALANCE_UPSERT_BIND_COLUMNS)}"
        )
    n = 0
    for row in rows:
        if not isinstance(row, dict):
            continue
        sym = _sym_from_row(row, fallback_symbol)
        pe = _parse_date(row.get("period_end"))
        if not sym or not pe:
            continue
        tf = _normalize_massive_statement_timeframe(row.get("timeframe"))
        fy = int(row.get("fiscal_year") or 0)
        fq = int(row.get("fiscal_quarter") or 0)
        fd = _parse_date(row.get("filing_date"))
        cik_v = _normalize_sec_cik(row.get("cik"))
        bind = _balance_sheet_bind_tuple(
            row,
            sym=sym,
            tf=tf,
            pe=pe,
            fd=fd,
            fy=fy,
            fq=fq,
            cik_v=cik_v,
            source=source,
        )
        if len(bind) != len(_STOCK_BALANCE_UPSERT_BIND_COLUMNS):
            raise RuntimeError("stock_balance_sheets bind tuple length drift")
        cur.execute(sql, bind)
        n += 1
    return n


def upsert_cash_flow_rows(
    cur: Any,
    rows: List[Dict[str, Any]],
    *,
    fallback_symbol: Optional[str] = None,
    source: str = SOURCE_DEFAULT,
) -> int:
    """Upsert rows into ``stock_cash_flows``. PG column names match Massive ``results[]`` keys."""
    if not rows:
        return 0
    sql = """
    INSERT INTO public.stock_cash_flows (
        symbol, timeframe, period_end, filing_date, fiscal_year, fiscal_quarter, cik,
        cash_from_operating_activities_continuing_operations,
        change_in_cash_and_equivalents,
        change_in_other_operating_assets_and_liabilities_net,
        depreciation_depletion_and_amortization,
        dividends,
        effect_of_currency_exchange_rate,
        income_loss_from_discontinued_operations,
        long_term_debt_issuances_repayments,
        net_cash_from_financing_activities,
        net_cash_from_financing_activities_continuing_operations,
        net_cash_from_financing_activities_discontinued_operations,
        net_cash_from_investing_activities,
        net_cash_from_investing_activities_continuing_operations,
        net_cash_from_investing_activities_discontinued_operations,
        net_cash_from_operating_activities,
        net_cash_from_operating_activities_discontinued_operations,
        net_income,
        noncontrolling_interests,
        other_cash_adjustments,
        other_financing_activities,
        other_investing_activities,
        other_operating_activities,
        purchase_of_property_plant_and_equipment,
        sale_of_property_plant_and_equipment,
        short_term_debt_issuances_repayments,
        source, fetched_at
    ) VALUES (
        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,now()
    )
    ON CONFLICT (symbol, timeframe, period_end, source) DO UPDATE SET
        filing_date = EXCLUDED.filing_date,
        fiscal_year = EXCLUDED.fiscal_year,
        fiscal_quarter = EXCLUDED.fiscal_quarter,
        cik = EXCLUDED.cik,
        cash_from_operating_activities_continuing_operations = EXCLUDED.cash_from_operating_activities_continuing_operations,
        change_in_cash_and_equivalents = EXCLUDED.change_in_cash_and_equivalents,
        change_in_other_operating_assets_and_liabilities_net = EXCLUDED.change_in_other_operating_assets_and_liabilities_net,
        depreciation_depletion_and_amortization = EXCLUDED.depreciation_depletion_and_amortization,
        dividends = EXCLUDED.dividends,
        effect_of_currency_exchange_rate = EXCLUDED.effect_of_currency_exchange_rate,
        income_loss_from_discontinued_operations = EXCLUDED.income_loss_from_discontinued_operations,
        long_term_debt_issuances_repayments = EXCLUDED.long_term_debt_issuances_repayments,
        net_cash_from_financing_activities = EXCLUDED.net_cash_from_financing_activities,
        net_cash_from_financing_activities_continuing_operations = EXCLUDED.net_cash_from_financing_activities_continuing_operations,
        net_cash_from_financing_activities_discontinued_operations = EXCLUDED.net_cash_from_financing_activities_discontinued_operations,
        net_cash_from_investing_activities = EXCLUDED.net_cash_from_investing_activities,
        net_cash_from_investing_activities_continuing_operations = EXCLUDED.net_cash_from_investing_activities_continuing_operations,
        net_cash_from_investing_activities_discontinued_operations = EXCLUDED.net_cash_from_investing_activities_discontinued_operations,
        net_cash_from_operating_activities = EXCLUDED.net_cash_from_operating_activities,
        net_cash_from_operating_activities_discontinued_operations = EXCLUDED.net_cash_from_operating_activities_discontinued_operations,
        net_income = EXCLUDED.net_income,
        noncontrolling_interests = EXCLUDED.noncontrolling_interests,
        other_cash_adjustments = EXCLUDED.other_cash_adjustments,
        other_financing_activities = EXCLUDED.other_financing_activities,
        other_investing_activities = EXCLUDED.other_investing_activities,
        other_operating_activities = EXCLUDED.other_operating_activities,
        purchase_of_property_plant_and_equipment = EXCLUDED.purchase_of_property_plant_and_equipment,
        sale_of_property_plant_and_equipment = EXCLUDED.sale_of_property_plant_and_equipment,
        short_term_debt_issuances_repayments = EXCLUDED.short_term_debt_issuances_repayments,
        fetched_at = now()
    """
    _n_ph = sql.count("%s")
    if _n_ph != len(_STOCK_CASH_FLOW_UPSERT_BIND_COLUMNS):
        raise RuntimeError(
            "stock_cash_flows INSERT placeholder mismatch: "
            f"{_n_ph} vs {len(_STOCK_CASH_FLOW_UPSERT_BIND_COLUMNS)} columns"
        )
    n = 0
    for row in rows:
        if not isinstance(row, dict):
            continue
        sym = _sym_from_row(row, fallback_symbol)
        pe = _parse_date(row.get("period_end"))
        if not sym or not pe:
            continue
        tf = _normalize_massive_statement_timeframe(row.get("timeframe"))
        fy = int(row.get("fiscal_year") or 0)
        fq = int(row.get("fiscal_quarter") or 0)
        fd = _parse_date(row.get("filing_date"))
        cik_v = _normalize_sec_cik(row.get("cik"))
        bind = _cash_flow_bind_tuple(
            row,
            sym=sym,
            tf=tf,
            pe=pe,
            fd=fd,
            fy=fy,
            fq=fq,
            cik_v=cik_v,
            source=source,
        )
        if len(bind) != len(_STOCK_CASH_FLOW_UPSERT_BIND_COLUMNS):
            raise RuntimeError("stock_cash_flows bind tuple length drift")
        cur.execute(sql, bind)
        n += 1
    return n


_STOCK_RATIOS_UPSERT_BIND_COLUMNS: Tuple[str, ...] = (
    "symbol",
    "date",
    "average_volume",
    "cash",
    "cik",
    "current",
    "debt_to_equity",
    "dividend_yield",
    "earnings_per_share",
    "enterprise_value",
    "ev_to_ebitda",
    "ev_to_sales",
    "free_cash_flow",
    "market_cap",
    "price",
    "price_to_book",
    "price_to_cash_flow",
    "price_to_earnings",
    "price_to_free_cash_flow",
    "price_to_sales",
    "quick",
    "return_on_assets",
    "return_on_equity",
    "source",
)


def _ratios_bind_tuple(
    row: Dict[str, Any],
    *,
    sym: str,
    d: Any,
    cik_v: Optional[str],
    source: str,
) -> Tuple[Any, ...]:
    """Build execute() params in :data:`_STOCK_RATIOS_UPSERT_BIND_COLUMNS` order."""
    return (
        sym,
        d,
        _f(row, "average_volume"),
        _f(row, "cash"),
        cik_v,
        _f(row, "current"),
        _f(row, "debt_to_equity"),
        _f(row, "dividend_yield"),
        _f(row, "earnings_per_share"),
        _f(row, "enterprise_value"),
        _f(row, "ev_to_ebitda"),
        _f(row, "ev_to_sales"),
        _f(row, "free_cash_flow"),
        _f(row, "market_cap"),
        _f(row, "price"),
        _f(row, "price_to_book"),
        _f(row, "price_to_cash_flow"),
        _f(row, "price_to_earnings"),
        _f(row, "price_to_free_cash_flow"),
        _f(row, "price_to_sales"),
        _f(row, "quick"),
        _f(row, "return_on_assets"),
        _f(row, "return_on_equity"),
        source,
    )


def upsert_ratios_rows(
    cur: Any,
    rows: List[Dict[str, Any]],
    *,
    fallback_symbol: Optional[str] = None,
    source: str = SOURCE_DEFAULT,
) -> int:
    """UPSERT Massive GET /stocks/financials/v1/ratios ``results[]`` (scalar keys match API)."""
    if not rows:
        return 0
    sql = """
    INSERT INTO public.stock_ratios (
        symbol, date,
        average_volume, cash, cik, "current",
        debt_to_equity, dividend_yield, earnings_per_share,
        enterprise_value, ev_to_ebitda, ev_to_sales,
        free_cash_flow, market_cap, price,
        price_to_book, price_to_cash_flow, price_to_earnings,
        price_to_free_cash_flow, price_to_sales,
        quick, return_on_assets, return_on_equity,
        source, fetched_at
    ) VALUES (
        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,now()
    )
    ON CONFLICT (symbol, date, source) DO UPDATE SET
        average_volume = EXCLUDED.average_volume,
        cash = EXCLUDED.cash,
        cik = EXCLUDED.cik,
        "current" = EXCLUDED."current",
        debt_to_equity = EXCLUDED.debt_to_equity,
        dividend_yield = EXCLUDED.dividend_yield,
        earnings_per_share = EXCLUDED.earnings_per_share,
        enterprise_value = EXCLUDED.enterprise_value,
        ev_to_ebitda = EXCLUDED.ev_to_ebitda,
        ev_to_sales = EXCLUDED.ev_to_sales,
        free_cash_flow = EXCLUDED.free_cash_flow,
        market_cap = EXCLUDED.market_cap,
        price = EXCLUDED.price,
        price_to_book = EXCLUDED.price_to_book,
        price_to_cash_flow = EXCLUDED.price_to_cash_flow,
        price_to_earnings = EXCLUDED.price_to_earnings,
        price_to_free_cash_flow = EXCLUDED.price_to_free_cash_flow,
        price_to_sales = EXCLUDED.price_to_sales,
        quick = EXCLUDED.quick,
        return_on_assets = EXCLUDED.return_on_assets,
        return_on_equity = EXCLUDED.return_on_equity,
        fetched_at = now()
    """
    _n_ph = sql.count("%s")
    if _n_ph != len(_STOCK_RATIOS_UPSERT_BIND_COLUMNS):
        raise RuntimeError(
            "stock_ratios INSERT placeholder mismatch: "
            f"{_n_ph} vs {len(_STOCK_RATIOS_UPSERT_BIND_COLUMNS)}"
        )
    n = 0
    for row in rows:
        if not isinstance(row, dict):
            continue
        sym = _sym_from_row(row, fallback_symbol)
        d = _parse_date(row.get("date"))
        if not sym or not d:
            continue
        cik_v = _normalize_sec_cik(row.get("cik"))
        bind = _ratios_bind_tuple(row, sym=sym, d=d, cik_v=cik_v, source=source)
        if len(bind) != len(_STOCK_RATIOS_UPSERT_BIND_COLUMNS):
            raise RuntimeError("stock_ratios bind tuple length drift")
        cur.execute(sql, bind)
        n += 1
    return n


_STOCK_SHORT_INTEREST_UPSERT_BIND_COLUMNS: Tuple[str, ...] = (
    "symbol",
    "settlement_date",
    "short_interest",
    "avg_daily_volume",
    "days_to_cover",
    "cik",
    "source",
)


def _short_interest_shares_from_row(row: Dict[str, Any]) -> Optional[int]:
    for k in ("short_interest", "short_interest_shares", "short_shares"):
        v = _i(row, k)
        if v is not None:
            return v
    return None


def _short_interest_bind_tuple(
    row: Dict[str, Any],
    *,
    sym: str,
    sd: Any,
    cik_v: Optional[str],
    source: str,
) -> Tuple[Any, ...]:
    """Build execute() params in :data:`_STOCK_SHORT_INTEREST_UPSERT_BIND_COLUMNS` order."""
    adv = _i(row, "avg_daily_volume")
    if adv is None:
        adv = _i(row, "avg_daily_volume_consolidated")
    return (
        sym,
        sd,
        _short_interest_shares_from_row(row),
        adv,
        _f(row, "days_to_cover"),
        cik_v,
        source,
    )


def upsert_short_interest_rows(
    cur: Any,
    rows: List[Dict[str, Any]],
    *,
    fallback_symbol: Optional[str] = None,
    source: str = SOURCE_DEFAULT,
) -> int:
    """UPSERT Massive ``GET /stocks/v1/short-interest`` ``results[]`` (API keys; ``symbol`` = ``ticker``)."""
    if not rows:
        return 0
    sql = """
    INSERT INTO public.stock_short_interest (
        symbol, settlement_date, short_interest, avg_daily_volume, days_to_cover, cik, source, fetched_at
    ) VALUES (%s,%s,%s,%s,%s,%s,%s,now())
    ON CONFLICT (symbol, settlement_date, source) DO UPDATE SET
        short_interest = EXCLUDED.short_interest,
        avg_daily_volume = EXCLUDED.avg_daily_volume,
        days_to_cover = EXCLUDED.days_to_cover,
        cik = EXCLUDED.cik,
        fetched_at = now()
    """
    _n_ph = sql.count("%s")
    if _n_ph != len(_STOCK_SHORT_INTEREST_UPSERT_BIND_COLUMNS):
        raise RuntimeError(
            "stock_short_interest INSERT placeholder mismatch: "
            f"{_n_ph} vs {len(_STOCK_SHORT_INTEREST_UPSERT_BIND_COLUMNS)}"
        )
    n = 0
    for row in rows:
        if not isinstance(row, dict):
            continue
        sym = _sym_from_row(row, fallback_symbol)
        sd = _parse_date(row.get("settlement_date"))
        if not sym or not sd:
            continue
        cik_v = _normalize_sec_cik(row.get("cik"))
        bind = _short_interest_bind_tuple(row, sym=sym, sd=sd, cik_v=cik_v, source=source)
        if len(bind) != len(_STOCK_SHORT_INTEREST_UPSERT_BIND_COLUMNS):
            raise RuntimeError("stock_short_interest bind tuple length drift")
        cur.execute(sql, bind)
        n += 1
    return n


_STOCK_SHORT_VOLUME_UPSERT_BIND_COLUMNS: Tuple[str, ...] = (
    "symbol",
    "trade_date",
    "adf_short_volume",
    "adf_short_volume_exempt",
    "exempt_volume",
    "nasdaq_carteret_short_volume",
    "nasdaq_carteret_short_volume_exempt",
    "nasdaq_chicago_short_volume",
    "nasdaq_chicago_short_volume_exempt",
    "non_exempt_volume",
    "nyse_short_volume",
    "nyse_short_volume_exempt",
    "short_volume",
    "short_volume_ratio",
    "total_volume",
    "exchanges",
    "cik",
    "source",
)


def _short_volume_bind_tuple(
    row: Dict[str, Any],
    *,
    sym: str,
    td: Any,
    ex_js: Any,
    cik_v: Optional[str],
    source: str,
) -> Tuple[Any, ...]:
    """Build execute() params in :data:`_STOCK_SHORT_VOLUME_UPSERT_BIND_COLUMNS` order."""
    return (
        sym,
        td,
        _i(row, "adf_short_volume"),
        _i(row, "adf_short_volume_exempt"),
        _f(row, "exempt_volume"),
        _i(row, "nasdaq_carteret_short_volume"),
        _i(row, "nasdaq_carteret_short_volume_exempt"),
        _i(row, "nasdaq_chicago_short_volume"),
        _i(row, "nasdaq_chicago_short_volume_exempt"),
        _f(row, "non_exempt_volume"),
        _i(row, "nyse_short_volume"),
        _i(row, "nyse_short_volume_exempt"),
        _i(row, "short_volume"),
        _f(row, "short_volume_ratio"),
        _i(row, "total_volume"),
        ex_js,
        cik_v,
        source,
    )


def upsert_short_volume_rows(
    cur: Any,
    rows: List[Dict[str, Any]],
    *,
    fallback_symbol: Optional[str] = None,
    source: str = SOURCE_DEFAULT,
) -> int:
    """UPSERT Massive ``GET /stocks/v1/short-volume`` ``results[]`` (API column names; ``symbol`` = ``ticker``, ``trade_date`` = ``date``)."""
    if not rows:
        return 0

    sql = """
    INSERT INTO public.stock_short_volume (
        symbol, trade_date,
        adf_short_volume, adf_short_volume_exempt, exempt_volume,
        nasdaq_carteret_short_volume, nasdaq_carteret_short_volume_exempt,
        nasdaq_chicago_short_volume, nasdaq_chicago_short_volume_exempt,
        non_exempt_volume, nyse_short_volume, nyse_short_volume_exempt,
        short_volume, short_volume_ratio, total_volume,
        exchanges, cik, source, fetched_at
    ) VALUES (
        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,now()
    )
    ON CONFLICT (symbol, trade_date, source) DO UPDATE SET
        adf_short_volume = EXCLUDED.adf_short_volume,
        adf_short_volume_exempt = EXCLUDED.adf_short_volume_exempt,
        exempt_volume = EXCLUDED.exempt_volume,
        nasdaq_carteret_short_volume = EXCLUDED.nasdaq_carteret_short_volume,
        nasdaq_carteret_short_volume_exempt = EXCLUDED.nasdaq_carteret_short_volume_exempt,
        nasdaq_chicago_short_volume = EXCLUDED.nasdaq_chicago_short_volume,
        nasdaq_chicago_short_volume_exempt = EXCLUDED.nasdaq_chicago_short_volume_exempt,
        non_exempt_volume = EXCLUDED.non_exempt_volume,
        nyse_short_volume = EXCLUDED.nyse_short_volume,
        nyse_short_volume_exempt = EXCLUDED.nyse_short_volume_exempt,
        short_volume = EXCLUDED.short_volume,
        short_volume_ratio = EXCLUDED.short_volume_ratio,
        total_volume = EXCLUDED.total_volume,
        exchanges = EXCLUDED.exchanges,
        cik = EXCLUDED.cik,
        fetched_at = now()
    """
    _n_ph = sql.count("%s")
    if _n_ph != len(_STOCK_SHORT_VOLUME_UPSERT_BIND_COLUMNS):
        raise RuntimeError(
            "stock_short_volume INSERT placeholder mismatch: "
            f"{_n_ph} vs {len(_STOCK_SHORT_VOLUME_UPSERT_BIND_COLUMNS)}"
        )
    n = 0
    for row in rows:
        if not isinstance(row, dict):
            continue
        sym = _sym_from_row(row, fallback_symbol)
        td = _parse_date(row.get("date") or row.get("trade_date"))
        if not sym or not td:
            continue
        ex = row.get("exchanges")
        ex_js = Json(ex) if ex is not None else None
        cik_v = _normalize_sec_cik(row.get("cik"))
        bind = _short_volume_bind_tuple(row, sym=sym, td=td, ex_js=ex_js, cik_v=cik_v, source=source)
        if len(bind) != len(_STOCK_SHORT_VOLUME_UPSERT_BIND_COLUMNS):
            raise RuntimeError("stock_short_volume bind tuple length drift")
        cur.execute(sql, bind)
        n += 1
    return n


def _throttle(sec: float) -> None:
    if sec > 0:
        time.sleep(sec)


def run_feed_stocks_income_statements_job(
    conn: Any,
    client: Any,
    payload: Dict[str, Any],
) -> Dict[str, Any]:
    symbols = payload.get("symbols") or []
    if not isinstance(symbols, list) or not symbols:
        raise ValueError("payload.symbols must be a non-empty list")
    throttle = float(payload.get("throttle_sec") or 0.22)
    rows_total = 0
    failures: List[Dict[str, str]] = []
    for raw in symbols:
        sym = str(raw).strip().upper()
        if not sym:
            continue
        # Include trailing-twelve-months so TTM rows persist under timeframe ``trailing_twelve_months`` (distinct PK from quarterly).
        for tf, lim in (
            ("quarterly", 12),
            ("annual", 5),
            ("trailing_twelve_months", 12),
        ):
            data = client.fetch_financials_v1_income_statements(
                tickers=sym, timeframe=tf, limit=lim, sort="period_end.desc"
            )
            if data.get("error"):
                failures.append({"symbol": sym, "timeframe": tf, "error": str(data["error"])})
                continue
            res = data.get("results")
            if not isinstance(res, list):
                continue
            with conn.cursor() as cur:
                n = upsert_income_statement_rows(cur, res, fallback_symbol=sym)
                rows_total += n
            conn.commit()
        _throttle(throttle)
    return {
        "ok": True,
        "kind": "feed_stocks_income_statements",
        "rows_upserted": rows_total,
        "symbols_processed": len([s for s in symbols if str(s).strip()]),
        "failures": failures[:50],
    }


def run_feed_stocks_balance_sheets_job(conn: Any, client: Any, payload: Dict[str, Any]) -> Dict[str, Any]:
    symbols = payload.get("symbols") or []
    if not isinstance(symbols, list) or not symbols:
        raise ValueError("payload.symbols must be a non-empty list")
    throttle = float(payload.get("throttle_sec") or 0.22)
    rows_total = 0
    failures: List[Dict[str, str]] = []
    for raw in symbols:
        sym = str(raw).strip().upper()
        if not sym:
            continue
        for tf, lim in (("quarterly", 12), ("annual", 5)):
            data = client.fetch_financials_v1_balance_sheets(
                tickers=sym, timeframe=tf, limit=lim, sort="period_end.desc"
            )
            if data.get("error"):
                failures.append({"symbol": sym, "timeframe": tf, "error": str(data["error"])})
                continue
            res = data.get("results")
            if not isinstance(res, list):
                continue
            with conn.cursor() as cur:
                rows_total += upsert_balance_sheet_rows(cur, res, fallback_symbol=sym)
            conn.commit()
        _throttle(throttle)
    return {
        "ok": True,
        "kind": "feed_stocks_balance_sheets",
        "rows_upserted": rows_total,
        "failures": failures[:50],
    }


def run_feed_stocks_cash_flows_job(conn: Any, client: Any, payload: Dict[str, Any]) -> Dict[str, Any]:
    symbols = payload.get("symbols") or []
    if not isinstance(symbols, list) or not symbols:
        raise ValueError("payload.symbols must be a non-empty list")
    throttle = float(payload.get("throttle_sec") or 0.22)
    rows_total = 0
    failures: List[Dict[str, str]] = []
    for raw in symbols:
        sym = str(raw).strip().upper()
        if not sym:
            continue
        # Include trailing-twelve-months so ``period_end`` rows match Massive TTM payloads (distinct PK from quarterly).
        for tf, lim in (
            ("quarterly", 12),
            ("annual", 5),
            ("trailing_twelve_months", 12),
        ):
            data = client.fetch_financials_v1_cash_flow_statements(
                tickers=sym, timeframe=tf, limit=lim, sort="period_end.desc"
            )
            if data.get("error"):
                failures.append({"symbol": sym, "timeframe": tf, "error": str(data["error"])})
                continue
            res = data.get("results")
            if not isinstance(res, list):
                continue
            with conn.cursor() as cur:
                rows_total += upsert_cash_flow_rows(cur, res, fallback_symbol=sym)
            conn.commit()
        _throttle(throttle)
    return {
        "ok": True,
        "kind": "feed_stocks_cash_flows",
        "rows_upserted": rows_total,
        "failures": failures[:50],
    }


def run_feed_stocks_ratios_job(conn: Any, client: Any, payload: Dict[str, Any]) -> Dict[str, Any]:
    symbols = payload.get("symbols") or []
    if not isinstance(symbols, list) or not symbols:
        raise ValueError("payload.symbols must be a non-empty list")
    throttle = float(payload.get("throttle_sec") or 0.22)
    rows_total = 0
    failures: List[Dict[str, str]] = []
    use_v1 = bool(payload.get("use_v1_endpoint", True))
    lim = int(payload.get("limit") or 120)
    sort_raw = payload.get("sort", None)
    sort: Optional[str] = (
        str(sort_raw).strip()
        if sort_raw is not None and str(sort_raw).strip()
        else None
    )
    api_rows_seen = 0
    for raw in symbols:
        sym = str(raw).strip().upper()
        if not sym:
            continue
        if not use_v1:
            failures.append(
                {"symbol": sym, "timeframe": "—", "error": "Ratios ingest requires v1 API (use_v1_endpoint=true)"},
            )
            _throttle(throttle)
            continue
        data = client.fetch_financials_v1_ratios(ticker=sym, limit=lim, sort=sort)
        if data.get("error"):
            failures.append({"symbol": sym, "timeframe": "—", "error": str(data["error"])})
            _throttle(throttle)
            continue
        res = data.get("results")
        if not isinstance(res, list):
            continue
        api_rows_seen += len(res)
        with conn.cursor() as cur:
            rows_total += upsert_ratios_rows(cur, res, fallback_symbol=sym)
        conn.commit()
        _throttle(throttle)
    return {
        "ok": True,
        "kind": "feed_stocks_ratios",
        "rows_upserted": rows_total,
        "api_rows_seen": api_rows_seen,
        "failures": failures[:50],
    }


def run_feed_stocks_short_interest_job(conn: Any, client: Any, payload: Dict[str, Any]) -> Dict[str, Any]:
    symbols = payload.get("symbols") or []
    if not isinstance(symbols, list) or not symbols:
        raise ValueError("payload.symbols must be a non-empty list")
    throttle = float(payload.get("throttle_sec") or 0.15)
    rows_total = 0
    failures: List[Dict[str, str]] = []
    lim = int(payload.get("limit") or 24)
    for raw in symbols:
        sym = str(raw).strip().upper()
        if not sym:
            continue
        data = client.fetch_stock_short_interest(sym, limit=lim, sort="settlement_date.desc")
        if data.get("error"):
            failures.append({"symbol": sym, "error": str(data["error"])})
            _throttle(throttle)
            continue
        res = data.get("results")
        if not isinstance(res, list):
            res = []
        with conn.cursor() as cur:
            rows_total += upsert_short_interest_rows(cur, res, fallback_symbol=sym)
        conn.commit()
        _throttle(throttle)
    return {"ok": True, "kind": "feed_stocks_short_interest", "rows_upserted": rows_total, "failures": failures[:50]}


def run_feed_stocks_short_volume_job(conn: Any, client: Any, payload: Dict[str, Any]) -> Dict[str, Any]:
    symbols = payload.get("symbols") or []
    if not isinstance(symbols, list) or not symbols:
        raise ValueError("payload.symbols must be a non-empty list")
    throttle = float(payload.get("throttle_sec") or 0.15)
    rows_total = 0
    failures: List[Dict[str, str]] = []
    lim = int(payload.get("limit") or 30)
    for raw in symbols:
        sym = str(raw).strip().upper()
        if not sym:
            continue
        data = client.fetch_stock_short_volume(sym, limit=lim, sort="date.desc")
        if data.get("error"):
            failures.append({"symbol": sym, "error": str(data["error"])})
            _throttle(throttle)
            continue
        res = data.get("results")
        if not isinstance(res, list):
            res = []
        with conn.cursor() as cur:
            rows_total += upsert_short_volume_rows(cur, res, fallback_symbol=sym)
        conn.commit()
        _throttle(throttle)
    return {"ok": True, "kind": "feed_stocks_short_volume", "rows_upserted": rows_total, "failures": failures[:50]}
