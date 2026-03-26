#!/usr/bin/env python3
"""
Validate Week 3 bridge readiness for the document-to-decision pipeline.

Checks:
1) Document corpus shape and minimum size.
2) Week 3 extraction bridge invocation on one income statement and one balance sheet.
"""
from __future__ import annotations

import argparse
import asyncio
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv

load_dotenv(ROOT / ".env")

from ledger.agents.week3_adapter import extract_financial_document


def find_sample_files(docs_root: Path) -> tuple[Path | None, Path | None]:
    income = None
    balance = None
    for path in docs_root.rglob("*.pdf"):
        name = path.name.lower()
        if income is None and "income_statement" in name:
            income = path
        elif balance is None and "balance_sheet" in name:
            balance = path
        if income and balance:
            break
    return income, balance


def validate_corpus(docs_root: Path, min_files: int) -> dict:
    if not docs_root.exists():
        return {"ok": False, "reason": f"documents path not found: {docs_root}"}

    files = [p for p in docs_root.rglob("*") if p.is_file()]
    company_dirs = [p for p in docs_root.iterdir() if p.is_dir()]

    return {
        "ok": len(files) >= min_files,
        "file_count": len(files),
        "company_count": len(company_dirs),
        "min_required": min_files,
    }


async def main() -> int:
    parser = argparse.ArgumentParser(description="Validate Week 3 document bridge")
    parser.add_argument("--docs-dir", default="documents")
    parser.add_argument("--min-files", type=int, default=160)
    args = parser.parse_args()

    docs_root = Path(args.docs_dir)
    corpus = validate_corpus(docs_root, args.min_files)
    print("[Corpus]", corpus)
    if not corpus.get("ok"):
        return 1

    income_file, balance_file = find_sample_files(docs_root)
    if not income_file or not balance_file:
        print("[Bridge] Missing sample PDF files for extraction test")
        return 1

    income = await extract_financial_document(str(income_file), "income_statement")
    balance = await extract_financial_document(str(balance_file), "balance_sheet")

    print(
        "[Bridge] income_statement",
        {
            "pipeline_version": income.get("pipeline_version"),
            "model": income.get("extraction_model"),
            "fact_keys": sorted((income.get("facts") or {}).keys())[:8],
            "processing_ms": income.get("processing_ms"),
            "error": income.get("error_type"),
        },
    )
    print(
        "[Bridge] balance_sheet",
        {
            "pipeline_version": balance.get("pipeline_version"),
            "model": balance.get("extraction_model"),
            "fact_keys": sorted((balance.get("facts") or {}).keys())[:8],
            "processing_ms": balance.get("processing_ms"),
            "error": balance.get("error_type"),
        },
    )

    if not income.get("facts") or not balance.get("facts"):
        print("[Bridge] Extraction returned empty facts")
        return 1

    print("[OK] Week 3 bridge validation passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
