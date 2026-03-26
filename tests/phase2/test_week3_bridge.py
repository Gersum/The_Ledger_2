from __future__ import annotations

import pytest
from pathlib import Path

from ledger.agents.week3_adapter import extract_financial_document


@pytest.mark.asyncio
async def test_week3_adapter_returns_contract_keys_for_income_statement():
    result = await extract_financial_document(None, "income_statement")
    required = {
        "facts",
        "pipeline_version",
        "extraction_model",
        "raw_text_length",
        "tables_extracted",
        "processing_ms",
        "error_type",
        "error_message",
    }
    assert required.issubset(result.keys())
    assert isinstance(result["facts"], dict)
    assert "total_revenue" in result["facts"]


@pytest.mark.asyncio
async def test_week3_adapter_returns_contract_keys_for_balance_sheet():
    result = await extract_financial_document(None, "balance_sheet")
    required = {
        "facts",
        "pipeline_version",
        "extraction_model",
        "raw_text_length",
        "tables_extracted",
        "processing_ms",
        "error_type",
        "error_message",
    }
    assert required.issubset(result.keys())
    assert isinstance(result["facts"], dict)
    assert "total_assets" in result["facts"]


def test_document_corpus_meets_expected_minimum():
    docs = Path("documents")
    assert docs.exists(), "documents directory not found"

    all_files = [p for p in docs.rglob("*") if p.is_file()]
    company_dirs = [p for p in docs.iterdir() if p.is_dir()]

    assert len(company_dirs) >= 80, f"expected >=80 company folders, found {len(company_dirs)}"
    assert len(all_files) >= 160, f"expected >=160 files, found {len(all_files)}"
