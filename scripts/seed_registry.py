from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path
from datetime import date, datetime, timezone

import asyncpg

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from datagen.company_generator import generate_companies

DEFAULT_DB_URL = "postgresql://postgres:postgres@localhost:5433/ledger"
DB_URL = os.environ.get("LEDGER_UI_DB_URL") or os.environ.get("DATABASE_URL") or DEFAULT_DB_URL


def _count_document_companies(default_count: int = 80) -> int:
    docs_root = ROOT / "documents"
    if not docs_root.exists() or not docs_root.is_dir():
        return default_count
    company_dirs = [p for p in docs_root.iterdir() if p.is_dir() and p.name.startswith("COMP-")]
    return len(company_dirs) if company_dirs else default_count


def _as_date(value: str | date) -> date:
    if isinstance(value, date):
        return value
    return date.fromisoformat(value)


def _as_datetime(value: str | datetime) -> datetime:
    if isinstance(value, datetime):
        return value
    if len(value) == 10:
        return datetime.fromisoformat(value + "T00:00:00+00:00")
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


async def seed() -> None:
    conn = await asyncpg.connect(DB_URL)
    try:
        print("Resetting applicant_registry schema...")
        await conn.execute("DROP SCHEMA IF EXISTS applicant_registry CASCADE")
        await conn.execute("CREATE SCHEMA applicant_registry")

        print("Creating applicant_registry tables...")
        await conn.execute(
            """
            CREATE TABLE applicant_registry.companies (
                company_id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                industry TEXT NOT NULL,
                naics TEXT NOT NULL,
                jurisdiction TEXT NOT NULL,
                legal_type TEXT NOT NULL,
                founded_year INT NOT NULL,
                employee_count INT NOT NULL,
                ein TEXT NOT NULL,
                address_city TEXT NOT NULL,
                address_state TEXT NOT NULL,
                relationship_start DATE NOT NULL,
                account_manager TEXT NOT NULL,
                risk_segment TEXT NOT NULL,
                trajectory TEXT NOT NULL,
                submission_channel TEXT NOT NULL,
                ip_region TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            CREATE TABLE applicant_registry.financial_history (
                company_id TEXT NOT NULL REFERENCES applicant_registry.companies(company_id),
                fiscal_year INT NOT NULL,
                total_revenue NUMERIC(18,2) NOT NULL,
                gross_profit NUMERIC(18,2) NOT NULL,
                operating_income NUMERIC(18,2) NOT NULL,
                ebitda NUMERIC(18,2) NOT NULL,
                net_income NUMERIC(18,2) NOT NULL,
                total_assets NUMERIC(18,2) NOT NULL,
                total_liabilities NUMERIC(18,2) NOT NULL,
                total_equity NUMERIC(18,2) NOT NULL,
                long_term_debt NUMERIC(18,2) NOT NULL,
                cash_and_equivalents NUMERIC(18,2) NOT NULL,
                current_assets NUMERIC(18,2) NOT NULL,
                current_liabilities NUMERIC(18,2) NOT NULL,
                accounts_receivable NUMERIC(18,2) NOT NULL,
                inventory NUMERIC(18,2) NOT NULL,
                debt_to_equity NUMERIC(18,2) NOT NULL,
                current_ratio NUMERIC(18,2) NOT NULL,
                debt_to_ebitda NUMERIC(18,2) NOT NULL,
                interest_coverage_ratio NUMERIC(18,2) NOT NULL,
                gross_margin NUMERIC(18,2) NOT NULL,
                ebitda_margin NUMERIC(18,2) NOT NULL,
                net_margin NUMERIC(18,2) NOT NULL,
                PRIMARY KEY (company_id, fiscal_year)
            );

            CREATE TABLE applicant_registry.compliance_flags (
                id SERIAL PRIMARY KEY,
                company_id TEXT NOT NULL REFERENCES applicant_registry.companies(company_id),
                flag_type TEXT NOT NULL,
                severity TEXT NOT NULL,
                is_active BOOLEAN NOT NULL DEFAULT TRUE,
                added_date TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                note TEXT
            );

            CREATE TABLE applicant_registry.loan_relationships (
                id SERIAL PRIMARY KEY,
                company_id TEXT NOT NULL REFERENCES applicant_registry.companies(company_id),
                lender_name TEXT NOT NULL,
                origination_date DATE NOT NULL,
                original_principal NUMERIC(18,2) NOT NULL,
                current_balance NUMERIC(18,2) NOT NULL,
                status TEXT NOT NULL
            );
            """
        )

        company_count = _count_document_companies()
        companies = generate_companies(company_count)
        print(f"Generated {len(companies)} companies for registry seed.")

        insert_company = """
            INSERT INTO applicant_registry.companies
            (company_id, name, industry, naics, jurisdiction, legal_type, founded_year, employee_count,
             ein, address_city, address_state, relationship_start, account_manager,
             risk_segment, trajectory, submission_channel, ip_region)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
        """

        insert_financial = """
            INSERT INTO applicant_registry.financial_history
            (company_id, fiscal_year, total_revenue, gross_profit, operating_income, ebitda, net_income,
             total_assets, total_liabilities, total_equity, long_term_debt, cash_and_equivalents,
             current_assets, current_liabilities, accounts_receivable, inventory, debt_to_equity,
             current_ratio, debt_to_ebitda, interest_coverage_ratio, gross_margin, ebitda_margin, net_margin)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16,
                    $17, $18, $19, $20, $21, $22, $23)
        """

        insert_flag = """
            INSERT INTO applicant_registry.compliance_flags
            (company_id, flag_type, severity, is_active, added_date, note)
            VALUES ($1, $2, $3, $4, $5, $6)
        """

        async with conn.transaction():
            for company in companies:
                await conn.execute(
                    insert_company,
                    company.company_id,
                    company.name,
                    company.industry,
                    company.naics,
                    company.jurisdiction,
                    company.legal_type,
                    company.founded_year,
                    company.employee_count,
                    company.ein,
                    company.address_city,
                    company.address_state,
                    _as_date(company.relationship_start),
                    company.account_manager,
                    company.risk_segment,
                    company.trajectory,
                    company.submission_channel,
                    company.ip_region,
                )

                for fin in company.financials:
                    await conn.execute(
                        insert_financial,
                        company.company_id,
                        fin["fiscal_year"],
                        fin["total_revenue"],
                        fin["gross_profit"],
                        fin["operating_income"],
                        fin["ebitda"],
                        fin["net_income"],
                        fin["total_assets"],
                        fin["total_liabilities"],
                        fin["total_equity"],
                        fin["long_term_debt"],
                        fin["cash_and_equivalents"],
                        fin["current_assets"],
                        fin["current_liabilities"],
                        fin["accounts_receivable"],
                        fin["inventory"],
                        fin["debt_to_equity"],
                        fin["current_ratio"],
                        fin["debt_to_ebitda"],
                        fin["interest_coverage_ratio"],
                        fin["gross_margin"],
                        fin["ebitda_margin"],
                        fin["net_margin"],
                    )

                await conn.execute(
                    insert_flag,
                    company.company_id,
                    "KYC_VERIFIED",
                    "LOW",
                    True,
                    datetime.now(timezone.utc),
                    "Seeded baseline KYC check.",
                )

                for flag in company.compliance_flags:
                    await conn.execute(
                        insert_flag,
                        company.company_id,
                        flag.get("flag_type", "MANUAL_REVIEW"),
                        flag.get("severity", "LOW"),
                        bool(flag.get("is_active", True)),
                        _as_datetime(str(flag.get("added_date", datetime.now(timezone.utc).isoformat()))),
                        flag.get("note", ""),
                    )

        count = await conn.fetchval("SELECT COUNT(*) FROM applicant_registry.companies")
        print(f"Seeding complete. companies={count}")
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(seed())
