import os
from pathlib import Path
import sys

import asyncpg
import pytest
import pytest_asyncio

sys.path.insert(0, str(Path(__file__).parent.parent))

from ledger.registry.client import ApplicantRegistryClient


DB_URL = os.environ.get(
    "TEST_DB_URL",
    os.environ.get("DATABASE_URL", "postgresql://postgres:apex@localhost/ledger"),
)


@pytest_asyncio.fixture
async def pool():
    try:
        pool = await asyncpg.create_pool(DB_URL, min_size=1, max_size=2)
    except Exception as exc:
        pytest.skip(f"PostgreSQL unavailable: {exc}")
    try:
        yield pool
    finally:
        await pool.close()


@pytest.fixture
def client(pool):
    return ApplicantRegistryClient(pool)


@pytest_asyncio.fixture
async def sample_company_id(pool):
    async with pool.acquire() as conn:
        value = await conn.fetchval(
            """
            SELECT company_id
            FROM applicant_registry.companies
            ORDER BY company_id
            LIMIT 1
            """
        )
    return value


@pytest_asyncio.fixture
async def company_with_relationships(pool):
    async with pool.acquire() as conn:
        value = await conn.fetchval(
            """
            SELECT company_id
            FROM applicant_registry.loan_relationships
            GROUP BY company_id
            ORDER BY company_id
            LIMIT 1
            """
        )
    return value


@pytest.mark.asyncio
async def test_get_company_returns_profile(client, sample_company_id):
    company = await client.get_company(sample_company_id)
    assert company is not None
    assert company.company_id == sample_company_id
    assert company.name
    assert company.industry


@pytest.mark.asyncio
async def test_get_financial_history_returns_sorted_years(client, sample_company_id):
    history = await client.get_financial_history(sample_company_id)
    years = [row.fiscal_year for row in history]

    assert len(history) == 3
    assert years == sorted(years)


@pytest.mark.asyncio
async def test_get_financial_history_filters_years(client, sample_company_id):
    history = await client.get_financial_history(sample_company_id, years=[2023, 2024])
    years = [row.fiscal_year for row in history]

    assert years == [2023, 2024]


@pytest.mark.asyncio
async def test_get_compliance_flags_active_filter(client, sample_company_id):
    flags = await client.get_compliance_flags(sample_company_id, active_only=True)
    assert all(flag.is_active for flag in flags)


@pytest.mark.asyncio
async def test_get_loan_relationships_returns_dicts(client, company_with_relationships):
    relationships = await client.get_loan_relationships(company_with_relationships)

    assert relationships
    assert isinstance(relationships[0], dict)
    assert "loan_amount" in relationships[0]
    assert "loan_year" in relationships[0]
