"""
ledger/registry/client.py — Applicant Registry read-only client
===============================================================
COMPLETION STATUS: STUB — implement the query methods.

This client reads from the applicant_registry schema in PostgreSQL.
It is READ-ONLY. No agent or event store component ever writes here.
The Applicant Registry is the external CRM — seeded by datagen/generate_all.py.
"""
from __future__ import annotations
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any
import asyncpg

@dataclass
class CompanyProfile:
    company_id: str; name: str; industry: str; naics: str
    jurisdiction: str; legal_type: str; founded_year: int
    employee_count: int; risk_segment: str; trajectory: str
    submission_channel: str; ip_region: str

@dataclass
class FinancialYear:
    fiscal_year: int; total_revenue: float; gross_profit: float
    operating_income: float; ebitda: float; net_income: float
    total_assets: float; total_liabilities: float; total_equity: float
    long_term_debt: float; cash_and_equivalents: float
    current_assets: float; current_liabilities: float
    accounts_receivable: float; inventory: float
    debt_to_equity: float | None; current_ratio: float | None
    debt_to_ebitda: float | None; interest_coverage_ratio: float | None
    gross_margin: float | None; ebitda_margin: float | None; net_margin: float | None

@dataclass
class ComplianceFlag:
    flag_type: str; severity: str; is_active: bool; added_date: str; note: str

class ApplicantRegistryClient:
    """
    READ-ONLY access to the Applicant Registry.
    Agents call these methods to get company profiles and historical data.
    Never write to this database from the event store system.
    """

    def __init__(self, pool: asyncpg.Pool):
        self._pool = pool

    async def get_company(self, company_id: str) -> CompanyProfile | None:
        """
        TODO: implement
        SELECT * FROM applicant_registry.companies WHERE company_id = $1
        """
        row = await self._pool.fetchrow(
            """
            SELECT company_id, name, industry, naics, jurisdiction, legal_type,
                   founded_year, employee_count, risk_segment, trajectory,
                   submission_channel, ip_region
            FROM applicant_registry.companies
            WHERE company_id = $1
            """,
            company_id,
        )
        if row is None:
            return None

        return CompanyProfile(
            company_id=row["company_id"],
            name=row["name"],
            industry=row["industry"],
            naics=row["naics"],
            jurisdiction=row["jurisdiction"],
            legal_type=row["legal_type"],
            founded_year=row["founded_year"],
            employee_count=row["employee_count"],
            risk_segment=row["risk_segment"],
            trajectory=row["trajectory"],
            submission_channel=row["submission_channel"],
            ip_region=row["ip_region"],
        )

    async def get_financial_history(self, company_id: str,
                                     years: list[int] | None = None) -> list[FinancialYear]:
        """
        TODO: implement
        SELECT * FROM applicant_registry.financial_history
        WHERE company_id = $1 [AND fiscal_year = ANY($2)]
        ORDER BY fiscal_year ASC
        """
        query = """
            SELECT fiscal_year, total_revenue, gross_profit, operating_income, ebitda,
                   net_income, total_assets, total_liabilities, total_equity,
                   long_term_debt, cash_and_equivalents, current_assets,
                   current_liabilities, accounts_receivable, inventory,
                   debt_to_equity, current_ratio, debt_to_ebitda,
                   interest_coverage_ratio, gross_margin, ebitda_margin, net_margin
            FROM applicant_registry.financial_history
            WHERE company_id = $1
        """
        params: list[Any] = [company_id]
        if years:
            query += " AND fiscal_year = ANY($2::int[])"
            params.append(years)
        query += " ORDER BY fiscal_year ASC"

        rows = await self._pool.fetch(query, *params)
        return [
            FinancialYear(
                fiscal_year=row["fiscal_year"],
                total_revenue=self._as_float(row["total_revenue"]),
                gross_profit=self._as_float(row["gross_profit"]),
                operating_income=self._as_float(row["operating_income"]),
                ebitda=self._as_float(row["ebitda"]),
                net_income=self._as_float(row["net_income"]),
                total_assets=self._as_float(row["total_assets"]),
                total_liabilities=self._as_float(row["total_liabilities"]),
                total_equity=self._as_float(row["total_equity"]),
                long_term_debt=self._as_float(row["long_term_debt"]),
                cash_and_equivalents=self._as_float(row["cash_and_equivalents"]),
                current_assets=self._as_float(row["current_assets"]),
                current_liabilities=self._as_float(row["current_liabilities"]),
                accounts_receivable=self._as_float(row["accounts_receivable"]),
                inventory=self._as_float(row["inventory"]),
                debt_to_equity=self._as_float(row["debt_to_equity"]),
                current_ratio=self._as_float(row["current_ratio"]),
                debt_to_ebitda=self._as_float(row["debt_to_ebitda"]),
                interest_coverage_ratio=self._as_float(row["interest_coverage_ratio"]),
                gross_margin=self._as_float(row["gross_margin"]),
                ebitda_margin=self._as_float(row["ebitda_margin"]),
                net_margin=self._as_float(row["net_margin"]),
            )
            for row in rows
        ]

    async def get_compliance_flags(self, company_id: str,
                                    active_only: bool = False) -> list[ComplianceFlag]:
        """
        TODO: implement
        SELECT * FROM applicant_registry.compliance_flags
        WHERE company_id = $1 [AND is_active = TRUE]
        """
        query = """
            SELECT flag_type, severity, is_active, added_date, note
            FROM applicant_registry.compliance_flags
            WHERE company_id = $1
        """
        params: list[Any] = [company_id]
        if active_only:
            query += " AND is_active = TRUE"
        query += " ORDER BY added_date ASC, flag_type ASC"

        rows = await self._pool.fetch(query, *params)
        return [
            ComplianceFlag(
                flag_type=row["flag_type"],
                severity=row["severity"],
                is_active=row["is_active"],
                added_date=self._as_iso_date(row["added_date"]),
                note=row["note"] or "",
            )
            for row in rows
        ]

    async def get_loan_relationships(self, company_id: str) -> list[dict]:
        """
        TODO: implement
        SELECT * FROM applicant_registry.loan_relationships WHERE company_id = $1
        """
        rows = await self._pool.fetch(
            """
            SELECT loan_amount, loan_year, was_repaid, default_occurred, note
            FROM applicant_registry.loan_relationships
            WHERE company_id = $1
            ORDER BY loan_year ASC, loan_amount ASC
            """,
            company_id,
        )
        return [self._normalize_record(dict(row)) for row in rows]

    @staticmethod
    def _as_float(value: Any) -> float | None:
        if value is None:
            return None
        if isinstance(value, Decimal):
            return float(value)
        return float(value)

    @staticmethod
    def _as_iso_date(value: Any) -> str:
        if isinstance(value, (date, datetime)):
            return value.isoformat()
        return str(value)

    def _normalize_record(self, record: dict[str, Any]) -> dict[str, Any]:
        normalized: dict[str, Any] = {}
        for key, value in record.items():
            if isinstance(value, Decimal):
                normalized[key] = float(value)
            elif isinstance(value, (date, datetime)):
                normalized[key] = value.isoformat()
            else:
                normalized[key] = value
        return normalized
