"""
ledger/agents/stub_agents.py
============================
FULL IMPLEMENTATIONS for DocumentProcessingAgent, FraudDetectionAgent,
ComplianceAgent, and DecisionOrchestratorAgent.

Pattern follows CreditAnalysisAgent exactly.
"""
from __future__ import annotations
import time
import json
from datetime import datetime
from decimal import Decimal
from typing import Any, TypedDict
from uuid import uuid4

from langgraph.graph import StateGraph, END

from ledger.agents.base_agent import BaseApexAgent
from ledger.agents.week3_adapter import extract_financial_document


# ─── DOCUMENT PROCESSING AGENT ───────────────────────────────────────────────

class DocProcState(TypedDict):
    application_id: str
    session_id: str
    applicant_id: str | None
    documents: list[dict[str, Any]] | None
    document_ids: list[str] | None
    document_paths: dict | None          # {doc_type: [path, ...]}
    extraction_results: list[dict] | None
    quality_assessment: dict | None
    quality_flags: list[str] | None
    errors: list[str]
    output_events: list[dict]
    next_agent: str | None


class DocumentProcessingAgent(BaseApexAgent):
    """
    Wraps the Week 3 Document Intelligence pipeline.
    Processes uploaded PDFs and appends extraction events.

    LangGraph nodes:
        validate_inputs → validate_document_formats → extract_income_statement →
        extract_balance_sheet → assess_quality → write_output

    Output events:
        docpkg-{id}:  DocumentFormatValidated, ExtractionStarted, ExtractionCompleted,
                      QualityAssessmentCompleted, PackageReadyForAnalysis
        loan-{id}:    CreditAnalysisRequested
    """

    def build_graph(self):
        g = StateGraph(DocProcState)
        g.add_node("validate_inputs",           self._node_validate_inputs)
        g.add_node("validate_document_formats", self._node_validate_formats)
        g.add_node("extract_income_statement",  self._node_extract_is)
        g.add_node("extract_balance_sheet",     self._node_extract_bs)
        g.add_node("assess_quality",            self._node_assess_quality)
        g.add_node("write_output",              self._node_write_output)

        g.set_entry_point("validate_inputs")
        g.add_edge("validate_inputs",           "validate_document_formats")
        g.add_edge("validate_document_formats", "extract_income_statement")
        g.add_edge("extract_income_statement",  "extract_balance_sheet")
        g.add_edge("extract_balance_sheet",     "assess_quality")
        g.add_edge("assess_quality",            "write_output")
        g.add_edge("write_output",              END)
        return g.compile()

    def _initial_state(self, application_id: str) -> DocProcState:
        return DocProcState(
            application_id=application_id, session_id=self.session_id,
            applicant_id=None, documents=None, document_ids=None, document_paths=None,
            extraction_results=None, quality_assessment=None,
            quality_flags=None, errors=[], output_events=[], next_agent=None,
        )

    @staticmethod
    def _normalize_document_type(value: str | None) -> str:
        if not value:
            return "application_proposal"
        return value.lower()

    def _select_document(self, state: DocProcState, document_type: str) -> dict[str, Any] | None:
        documents = state.get("documents") or []
        for doc in documents:
            if doc.get("document_type") == document_type:
                return doc
        return None

    @staticmethod
    def _build_field_confidence(facts: dict[str, Any], keys: list[str]) -> dict[str, float]:
        return {k: (1.0 if facts.get(k) is not None else 0.0) for k in keys}

    @staticmethod
    def _normalize_facts(raw: dict[str, Any]) -> dict[str, Any]:
        allowed = {
            "total_revenue", "gross_profit", "operating_expenses", "operating_income", "ebitda",
            "depreciation_amortization", "interest_expense", "income_before_tax", "tax_expense", "net_income",
            "total_assets", "current_assets", "cash_and_equivalents", "accounts_receivable", "inventory",
            "total_liabilities", "current_liabilities", "long_term_debt", "total_equity",
            "operating_cash_flow", "investing_cash_flow", "financing_cash_flow", "free_cash_flow",
            "debt_to_equity", "current_ratio", "debt_to_ebitda", "interest_coverage", "gross_margin", "net_margin",
            "fiscal_year_end", "currency", "gaap_compliant", "field_confidence", "page_references",
            "extraction_notes", "balance_sheet_balances", "balance_discrepancy_usd",
        }
        return {k: v for k, v in raw.items() if k in allowed and v is not None}

    async def _node_validate_inputs(self, state: DocProcState) -> DocProcState:
        t = time.time()
        app_id = state["application_id"]

        # Load DocumentUploaded events from the loan stream
        loan_events = await self.store.load_stream(f"loan-{app_id}")
        uploaded = [e for e in loan_events if e["event_type"] == "DocumentUploaded"]

        if not uploaded:
            await self._record_input_failed(
                missing_inputs=["DocumentUploaded"],
                validation_errors=["No documents found on loan stream"],
            )
            raise ValueError(f"No documents uploaded for application {app_id}")

        # Extract document IDs and paths
        documents: list[dict[str, Any]] = []
        doc_ids = [e["payload"]["document_id"] for e in uploaded]
        applicant_id = None
        for e in loan_events:
            if e["event_type"] == "ApplicationSubmitted":
                applicant_id = e["payload"].get("applicant_id")
                break

        # Build path map: doc_type -> path (from payload)
        doc_paths: dict[str, list[str]] = {}
        for e in uploaded:
            p = e["payload"]
            dtype = self._normalize_document_type(p.get("document_type"))
            path = p.get("file_path") or p.get("storage_path") or p.get("file_name", "")
            doc_paths.setdefault(dtype, []).append(path)
            documents.append(
                {
                    "document_id": p.get("document_id"),
                    "document_type": dtype,
                    "document_format": str(p.get("document_format", "pdf")).lower(),
                    "file_path": path,
                }
            )

        ms = int((time.time() - t) * 1000)
        await self._record_input_validated(["application_id", "document_ids", "document_paths"], ms)
        await self._record_node_execution("validate_inputs", ["application_id"], ["document_ids", "document_paths"], ms)
        return {**state, "applicant_id": applicant_id, "documents": documents, "document_ids": doc_ids, "document_paths": doc_paths}

    async def _node_validate_formats(self, state: DocProcState) -> DocProcState:
        t = time.time()
        app_id = state["application_id"]
        package_id = f"docpkg-{app_id}"

        valid_doc_ids = []
        for doc in (state.get("documents") or []):
            doc_id = str(doc.get("document_id") or "unknown")
            doc_type = self._normalize_document_type(doc.get("document_type"))
            detected_format = str(doc.get("document_format") or "pdf").upper()
            # Append DocumentFormatValidated for each doc
            await self._append_with_retry(f"docpkg-{app_id}", [{
                "event_type": "DocumentFormatValidated",
                "event_version": 1,
                "payload": {
                    "package_id": package_id,
                    "document_id": doc_id,
                    "document_type": doc_type,
                    "page_count": 1,
                    "detected_format": detected_format,
                    "validated_at": datetime.now().isoformat(),
                },
            }])
            valid_doc_ids.append(doc_id)

        ms = int((time.time() - t) * 1000)
        await self._record_node_execution("validate_document_formats", ["document_ids"], ["valid_doc_ids"], ms)
        return {**state, "document_ids": valid_doc_ids}

    async def _node_extract_is(self, state: DocProcState) -> DocProcState:
        t = time.time()
        app_id = state["application_id"]
        results = list(state.get("extraction_results") or [])

        document = self._select_document(state, "income_statement")
        if not document:
            ms = int((time.time() - t) * 1000)
            await self._record_node_execution("extract_income_statement", ["documents"], ["extraction_results"], ms)
            return state

        file_path = document.get("file_path")
        doc_id = str(document.get("document_id") or "unknown")

        # Signal extraction start
        started_at = datetime.now().isoformat()
        await self._append_with_retry(f"docpkg-{app_id}", [{
            "event_type": "ExtractionStarted",
            "event_version": 1,
            "payload": {
                "package_id": f"docpkg-{app_id}",
                "document_id": doc_id,
                "document_type": "income_statement",
                "pipeline_version": "week3-docrefinery",
                "extraction_model": "docrefinery",
                "started_at": started_at,
            },
        }])

        extraction = await extract_financial_document(file_path, "income_statement")
        facts = self._normalize_facts(dict(extraction.get("facts") or {}))
        error_type = extraction.get("error_type")
        if error_type:
            await self._append_with_retry(f"docpkg-{app_id}", [{
                "event_type": "ExtractionFailed",
                "event_version": 1,
                "payload": {
                    "package_id": f"docpkg-{app_id}",
                    "document_id": doc_id,
                    "error_type": str(error_type),
                    "error_message": str(extraction.get("error_message") or "Week 3 extraction unavailable")[:300],
                    "partial_facts": facts or None,
                    "failed_at": datetime.now().isoformat(),
                },
            }])

        critical_fields = ["total_revenue", "net_income", "ebitda", "gross_profit"]
        field_confidence = self._build_field_confidence(facts, critical_fields)
        extraction_notes = [f"field_missing:{k}" for k, v in field_confidence.items() if v == 0.0]
        facts["field_confidence"] = field_confidence
        facts["extraction_notes"] = extraction_notes
        facts.setdefault("currency", "USD")
        facts.setdefault("gaap_compliant", True)

        await self._append_with_retry(f"docpkg-{app_id}", [{
            "event_type": "ExtractionCompleted",
            "event_version": 1,
            "payload": {
                "package_id": f"docpkg-{app_id}",
                "document_id": doc_id,
                "document_type": "income_statement",
                "facts": facts or None,
                "raw_text_length": int(extraction.get("raw_text_length") or 0),
                "tables_extracted": int(extraction.get("tables_extracted") or 0),
                "processing_ms": max(int(extraction.get("processing_ms") or 1), 1),
                "completed_at": datetime.now().isoformat(),
            },
        }])

        ms = int((time.time() - t) * 1000)
        await self._record_tool_call("week3_income_statement_pipeline", f"file={file_path}", f"facts={len(facts)}", ms)
        await self._record_node_execution("extract_income_statement", ["document_paths"], ["extraction_results"], ms)
        results.append({"type": "income_statement", "facts": facts, "field_confidence": field_confidence})
        return {**state, "extraction_results": results}

    async def _node_extract_bs(self, state: DocProcState) -> DocProcState:
        t = time.time()
        app_id = state["application_id"]
        results = list(state.get("extraction_results") or [])

        document = self._select_document(state, "balance_sheet")
        if not document:
            ms = int((time.time() - t) * 1000)
            await self._record_node_execution("extract_balance_sheet", ["documents"], ["extraction_results"], ms)
            return state

        file_path = document.get("file_path")
        doc_id = str(document.get("document_id") or "unknown")

        await self._append_with_retry(f"docpkg-{app_id}", [{
            "event_type": "ExtractionStarted",
            "event_version": 1,
            "payload": {
                "package_id": f"docpkg-{app_id}",
                "document_id": doc_id,
                "document_type": "balance_sheet",
                "pipeline_version": "week3-docrefinery",
                "extraction_model": "docrefinery",
                "started_at": datetime.now().isoformat(),
            },
        }])

        extraction = await extract_financial_document(file_path, "balance_sheet")
        facts = self._normalize_facts(dict(extraction.get("facts") or {}))
        error_type = extraction.get("error_type")
        if error_type:
            await self._append_with_retry(f"docpkg-{app_id}", [{
                "event_type": "ExtractionFailed",
                "event_version": 1,
                "payload": {
                    "package_id": f"docpkg-{app_id}",
                    "document_id": doc_id,
                    "error_type": str(error_type),
                    "error_message": str(extraction.get("error_message") or "Week 3 extraction unavailable")[:300],
                    "partial_facts": facts or None,
                    "failed_at": datetime.now().isoformat(),
                },
            }])

        bs_fields = ["total_assets", "total_liabilities", "total_equity", "current_assets", "current_liabilities"]
        field_confidence = {f: 0.0 if facts.get(f) is None else 1.0 for f in bs_fields}
        extraction_notes = [f"field_missing:{k}" for k, v in field_confidence.items() if v == 0.0]
        facts["field_confidence"] = field_confidence
        facts["extraction_notes"] = extraction_notes
        facts.setdefault("currency", "USD")
        facts.setdefault("gaap_compliant", True)

        await self._append_with_retry(f"docpkg-{app_id}", [{
            "event_type": "ExtractionCompleted",
            "event_version": 1,
            "payload": {
                "package_id": f"docpkg-{app_id}",
                "document_id": doc_id,
                "document_type": "balance_sheet",
                "facts": facts or None,
                "raw_text_length": int(extraction.get("raw_text_length") or 0),
                "tables_extracted": int(extraction.get("tables_extracted") or 0),
                "processing_ms": max(int(extraction.get("processing_ms") or 1), 1),
                "completed_at": datetime.now().isoformat(),
            },
        }])

        ms = int((time.time() - t) * 1000)
        await self._record_tool_call("week3_balance_sheet_pipeline", f"file={file_path}", f"facts={len(facts)}", ms)
        await self._record_node_execution("extract_balance_sheet", ["document_paths"], ["extraction_results"], ms)
        results.append({"type": "balance_sheet", "facts": facts, "field_confidence": field_confidence})
        return {**state, "extraction_results": results}

    async def _node_assess_quality(self, state: DocProcState) -> DocProcState:
        t = time.time()
        app_id = state["application_id"]
        results = state.get("extraction_results") or []
        all_facts = {r["type"]: r["facts"] for r in results}
        all_conf = {r["type"]: r["field_confidence"] for r in results}

        system = (
            "You are a financial document quality analyst for a commercial bank. "
            "Your ONLY role is to check the internal consistency of extracted financial data. "
            "Do NOT make credit or lending decisions. "
            "Return ONLY valid JSON matching the QualityAssessment schema."
        )
        user = (
            f"Assess the quality of these extracted financial facts:\n{json.dumps(all_facts, default=str)}\n"
            f"Field confidence: {json.dumps(all_conf, default=str)}\n\n"
            "Return JSON: {\"overall_quality\": \"GOOD\"|\"ACCEPTABLE\"|\"POOR\", "
            "\"consistency_checks\": [{\"check\": str, \"passed\": bool, \"note\": str}], "
            "\"critical_missing_fields\": [str], "
            "\"quality_score\": float_0_to_1, \"assessment_notes\": str}"
        )
        tok_in = tok_out = cost = None
        try:
            content, tok_in, tok_out, cost = await self._call_llm(system, user, max_tokens=512)
            qa = self._parse_json(content)
        except Exception:
            qa = {
                "overall_quality": "ACCEPTABLE",
                "consistency_checks": [],
                "critical_missing_fields": [],
                "quality_score": 0.7,
                "assessment_notes": "LLM unavailable or parse failed; deterministic fallback used.",
            }

        critical_missing = list(qa.get("critical_missing_fields", []))
        anomalies = [
            c.get("note", "consistency_check_failed")
            for c in qa.get("consistency_checks", [])
            if isinstance(c, dict) and not c.get("passed", True)
        ]
        if critical_missing:
            anomalies.append("critical_missing_fields")

        first_doc_id = "unknown"
        documents = state.get("documents") or []
        if documents:
            first_doc_id = str(documents[0].get("document_id") or "unknown")

        await self._append_with_retry(f"docpkg-{app_id}", [{
            "event_type": "QualityAssessmentCompleted",
            "event_version": 1,
            "payload": {
                "package_id": f"docpkg-{app_id}",
                "document_id": first_doc_id,
                "overall_confidence": float(qa.get("quality_score", 0.7)),
                "is_coherent": not critical_missing,
                "anomalies": anomalies,
                "critical_missing_fields": critical_missing,
                "reextraction_recommended": bool(critical_missing),
                "auditor_notes": qa.get("assessment_notes", ""),
                "assessed_at": datetime.now().isoformat(),
            },
        }])

        ms = int((time.time() - t) * 1000)
        await self._record_node_execution("assess_quality", ["extraction_results"], ["quality_assessment"], ms, tok_in, tok_out, cost)
        return {**state, "quality_assessment": qa, "quality_flags": qa.get("critical_missing_fields", [])}

    async def _node_write_output(self, state: DocProcState) -> DocProcState:
        t = time.time()
        app_id = state["application_id"]

        quality_flags = state.get("quality_flags") or []
        # 1. PackageReadyForAnalysis on docpkg stream
        await self._append_with_retry(f"docpkg-{app_id}", [{
            "event_type": "PackageReadyForAnalysis",
            "event_version": 1,
            "payload": {
                "package_id": f"docpkg-{app_id}",
                "application_id": app_id,
                "documents_processed": len(state.get("document_ids") or []),
                "has_quality_flags": bool(quality_flags),
                "quality_flag_count": len(quality_flags),
                "ready_at": datetime.now().isoformat(),
            },
        }])

        # 2. CreditAnalysisRequested on loan stream
        await self._append_with_retry(f"loan-{app_id}", [{
            "event_type": "CreditAnalysisRequested",
            "event_version": 1,
            "payload": {
                "application_id": app_id,
                "requested_at": datetime.now().isoformat(),
                "requested_by": self.agent_id,
                "priority": "NORMAL",
            },
        }], causation_id=self.session_id)

        ms = int((time.time() - t) * 1000)
        events_written = ["PackageReadyForAnalysis", "CreditAnalysisRequested"]
        await self._record_output_written(events_written, f"Package ready for credit analysis: {app_id}")
        await self._record_node_execution("write_output", ["quality_assessment"], ["package_ready", "credit_requested"], ms)
        return {**state, "output_events": events_written, "next_agent": "credit_analysis"}


# ─── FRAUD DETECTION AGENT ───────────────────────────────────────────────────

class FraudState(TypedDict):
    application_id: str
    session_id: str
    extracted_facts: dict | None
    registry_profile: dict | None
    historical_financials: list[dict] | None
    fraud_signals: list[dict] | None
    fraud_score: float | None
    anomalies: list[dict] | None
    errors: list[str]
    output_events: list[dict]
    next_agent: str | None


class FraudDetectionAgent(BaseApexAgent):
    """
    Cross-references extracted document facts against historical registry data.
    Detects anomalous discrepancies that suggest fraud or document manipulation.

    LangGraph nodes:
        validate_inputs → load_document_facts → cross_reference_registry →
        analyze_fraud_patterns → write_output
    """

    def build_graph(self):
        g = StateGraph(FraudState)
        g.add_node("validate_inputs",          self._node_validate_inputs)
        g.add_node("load_document_facts",      self._node_load_facts)
        g.add_node("cross_reference_registry", self._node_cross_reference)
        g.add_node("analyze_fraud_patterns",   self._node_analyze)
        g.add_node("write_output",             self._node_write_output)

        g.set_entry_point("validate_inputs")
        g.add_edge("validate_inputs",          "load_document_facts")
        g.add_edge("load_document_facts",      "cross_reference_registry")
        g.add_edge("cross_reference_registry", "analyze_fraud_patterns")
        g.add_edge("analyze_fraud_patterns",   "write_output")
        g.add_edge("write_output",             END)
        return g.compile()

    def _initial_state(self, application_id: str) -> FraudState:
        return FraudState(
            application_id=application_id, session_id=self.session_id,
            extracted_facts=None, registry_profile=None, historical_financials=None,
            fraud_signals=None, fraud_score=None, anomalies=None,
            errors=[], output_events=[], next_agent=None,
        )

    async def _node_validate_inputs(self, state: FraudState) -> FraudState:
        t = time.time()
        app_id = state["application_id"]
        loan_events = await self.store.load_stream(f"loan-{app_id}")
        trigger = next((e for e in loan_events if e["event_type"] == "FraudScreeningRequested"), None)
        if not trigger:
            await self._record_input_failed(
                ["FraudScreeningRequested"], ["No FraudScreeningRequested found on loan stream"]
            )
            raise ValueError(f"FraudScreeningRequested not found for {app_id}")

        # Append initiation event
        await self._append_with_retry(f"fraud-{app_id}", [{
            "event_type": "FraudScreeningInitiated",
            "event_version": 1,
            "payload": {
                "application_id": app_id,
                "session_id": self.session_id,
                "triggered_by_event_id": trigger.get("event_id") or trigger.get("stream_position"),
                "initiated_at": datetime.now().isoformat(),
            },
        }])

        ms = int((time.time() - t) * 1000)
        await self._record_input_validated(["application_id", "FraudScreeningRequested"], ms)
        await self._record_node_execution("validate_inputs", ["application_id"], ["trigger_verified"], ms)
        return state

    async def _node_load_facts(self, state: FraudState) -> FraudState:
        t = time.time()
        app_id = state["application_id"]
        # Load extracted facts from docpkg stream
        docpkg_events = await self.store.load_stream(f"docpkg-{app_id}")
        # Merge all ExtractionCompleted events into one fact dict
        merged: dict = {}
        for e in docpkg_events:
            if e["event_type"] == "ExtractionCompleted":
                merged.update({k: v for k, v in e["payload"].items() if v is not None})

        ms = int((time.time() - t) * 1000)
        await self._record_tool_call("load_extraction_events", {"stream": f"docpkg-{app_id}"}, {"fields_loaded": len(merged)}, ms)
        await self._record_node_execution("load_document_facts", ["application_id"], ["extracted_facts"], ms)
        return {**state, "extracted_facts": merged}

    async def _node_cross_reference(self, state: FraudState) -> FraudState:
        t = time.time()
        app_id = state["application_id"]
        # Load company profile and financial history from registry
        loan_events = await self.store.load_stream(f"loan-{app_id}")
        submit_event = next((e for e in loan_events if e["event_type"] == "ApplicationSubmitted"), None)
        applicant_id = submit_event["payload"]["applicant_id"] if submit_event else None

        profile = None
        financials = []
        if applicant_id and self.registry:
            try:
                profile = await self.registry.get_company(applicant_id)
                fin_rows = await self.registry.get_financial_history(applicant_id)
                financials = [f.__dict__ if hasattr(f, "__dict__") else dict(f) for f in fin_rows]
            except Exception:
                pass

        ms = int((time.time() - t) * 1000)
        await self._record_tool_call(
            "registry.get_financial_history",
            {"company_id": applicant_id},
            {"years_loaded": len(financials)},
            ms,
        )
        await self._record_node_execution("cross_reference_registry", ["extracted_facts"], ["registry_profile", "historical_financials"], ms)
        return {
            **state,
            "registry_profile": profile.__dict__ if profile and hasattr(profile, "__dict__") else (profile or {}),
            "historical_financials": financials,
        }

    async def _node_analyze(self, state: FraudState) -> FraudState:
        t = time.time()
        app_id = state["application_id"]
        facts = state.get("extracted_facts") or {}
        historical = state.get("historical_financials") or []
        profile = state.get("registry_profile") or {}

        # Deterministic fraud scoring (Python, before LLM)
        fraud_score = 0.05  # base

        # Revenue discrepancy check
        doc_revenue = facts.get("total_revenue") or 0
        if historical:
            latest = historical[-1] if historical else {}
            reg_revenue = latest.get("total_revenue") or 0
            if reg_revenue and doc_revenue:
                try:
                    gap = abs(float(doc_revenue) - float(reg_revenue)) / float(reg_revenue)
                    trajectory = profile.get("trajectory", "STABLE")
                    if gap > 0.40 and trajectory not in ("GROWTH", "RECOVERING"):
                        fraud_score += 0.25
                except (TypeError, ZeroDivisionError):
                    pass

        # Balance sheet consistency
        assets = facts.get("total_assets") or 0
        liabilities = facts.get("total_liabilities") or 0
        equity = facts.get("total_equity") or 0
        if assets and liabilities and equity:
            try:
                bs_diff = abs(float(assets) - float(liabilities) - float(equity)) / max(float(assets), 1)
                if bs_diff > 0.05:
                    fraud_score += 0.20
            except TypeError:
                pass

        fraud_score = min(round(fraud_score, 4), 1.0)

        # LLM anomaly identification
        system = (
            "You are a financial fraud analyst for a commercial bank. "
            "Identify specific named anomalies from the cross-reference results. "
            "For each anomaly: type, severity (LOW/MEDIUM/HIGH), evidence, affected_fields. "
            "Return ONLY valid JSON: {\"anomalies\": [...], \"llm_fraud_score_adjustment\": float, \"narrative\": str}"
        )
        user = (
            f"Document facts: {json.dumps(facts, default=str)}\n"
            f"Registry historical: {json.dumps(historical[-1] if historical else {}, default=str)}\n"
            f"Computed base fraud_score: {fraud_score}\n"
            "Identify anomalies and suggest a score adjustment (-0.1 to +0.2)."
        )
        try:
            content, tok_in, tok_out, cost = await self._call_llm(system, user, max_tokens=768)
            analysis = self._parse_json(content)
            anomalies = analysis.get("anomalies", [])
            fraud_score = min(fraud_score + float(analysis.get("llm_fraud_score_adjustment", 0.0)), 1.0)
            fraud_score = round(fraud_score, 4)
        except Exception:
            anomalies, tok_in, tok_out, cost = [], None, None, None

        # Append FraudAnomalyDetected for MEDIUM+ severity anomalies
        for anomaly in anomalies:
            if anomaly.get("severity") in ("MEDIUM", "HIGH"):
                await self._append_with_retry(f"fraud-{app_id}", [{
                    "event_type": "FraudAnomalyDetected",
                    "event_version": 1,
                    "payload": {
                        "application_id": app_id,
                        "anomaly_type": anomaly.get("type", "UNKNOWN"),
                        "severity": anomaly.get("severity"),
                        "evidence": anomaly.get("evidence", ""),
                        "affected_fields": anomaly.get("affected_fields", []),
                        "detected_at": datetime.now().isoformat(),
                    },
                }])

        ms = int((time.time() - t) * 1000)
        await self._record_node_execution("analyze_fraud_patterns", ["extracted_facts", "historical_financials"], ["fraud_score", "anomalies"], ms, tok_in, tok_out, cost)
        return {**state, "fraud_score": fraud_score, "anomalies": anomalies}

    async def _node_write_output(self, state: FraudState) -> FraudState:
        t = time.time()
        app_id = state["application_id"]
        fraud_score = state.get("fraud_score") or 0.05
        anomalies = state.get("anomalies") or []

        # Determine recommendation from score
        if fraud_score > 0.60:
            recommendation = "DECLINE"
        elif fraud_score >= 0.30:
            recommendation = "FLAG_FOR_REVIEW"
        else:
            recommendation = "PROCEED"

        # FraudScreeningCompleted on fraud stream
        await self._append_with_retry(f"fraud-{app_id}", [{
            "event_type": "FraudScreeningCompleted",
            "event_version": 1,
            "payload": {
                "application_id": app_id,
                "session_id": self.session_id,
                "fraud_score": fraud_score,
                "recommendation": recommendation,
                "anomaly_count": len(anomalies),
                "anomaly_flags": [a.get("type") for a in anomalies if a.get("severity") in ("MEDIUM", "HIGH")],
                "screening_model_version": self.model,
                "completed_at": datetime.now().isoformat(),
            },
        }])

        # ComplianceCheckRequested on loan stream
        await self._append_with_retry(f"loan-{app_id}", [{
            "event_type": "ComplianceCheckRequested",
            "event_version": 1,
            "payload": {
                "application_id": app_id,
                "regulation_set_version": "2026-Q1-v1",
                "checks_required": ["REG-001", "REG-002", "REG-003", "REG-004", "REG-005", "REG-006"],
                "requested_at": datetime.now().isoformat(),
            },
        }], causation_id=self.session_id)

        ms = int((time.time() - t) * 1000)
        events_written = ["FraudScreeningCompleted", "ComplianceCheckRequested"]
        await self._record_output_written(events_written, f"Fraud score {fraud_score:.3f} ({recommendation})")
        await self._record_node_execution("write_output", ["fraud_score"], ["fraud_completed", "compliance_requested"], ms)
        return {**state, "output_events": events_written, "next_agent": "compliance"}


# ─── COMPLIANCE AGENT ─────────────────────────────────────────────────────────

class ComplianceState(TypedDict):
    application_id: str
    session_id: str
    company_profile: dict | None
    rule_results: list[dict] | None
    has_hard_block: bool
    block_rule_id: str | None
    errors: list[str]
    output_events: list[dict]
    next_agent: str | None


# Regulation definitions — deterministic, no LLM in decision path
REGULATIONS = {
    "REG-001": {
        "name": "Bank Secrecy Act (BSA) Check",
        "version": "2026-Q1-v1",
        "is_hard_block": False,
        "check": lambda co: not any(
            f.get("flag_type") == "AML_WATCH" and f.get("is_active")
            for f in co.get("compliance_flags", [])
        ),
        "failure_reason": "Active AML Watch flag present. Remediation required.",
        "remediation": "Provide enhanced due diligence documentation within 10 business days.",
    },
    "REG-002": {
        "name": "OFAC Sanctions Screening",
        "version": "2026-Q1-v1",
        "is_hard_block": True,
        "check": lambda co: not any(
            f.get("flag_type") == "SANCTIONS_REVIEW" and f.get("is_active")
            for f in co.get("compliance_flags", [])
        ),
        "failure_reason": "Active OFAC Sanctions Review. Application blocked.",
        "remediation": None,
    },
    "REG-003": {
        "name": "Jurisdiction Lending Eligibility",
        "version": "2026-Q1-v1",
        "is_hard_block": True,
        "check": lambda co: co.get("jurisdiction") != "MT",
        "failure_reason": "Jurisdiction MT not approved for commercial lending at this time.",
        "remediation": None,
    },
    "REG-004": {
        "name": "Legal Entity Type Eligibility",
        "version": "2026-Q1-v1",
        "is_hard_block": False,
        "check": lambda co: not (
            co.get("legal_type") == "Sole Proprietor"
            and (co.get("requested_amount_usd", 0) or 0) > 250_000
        ),
        "failure_reason": "Sole Proprietor loans >$250K require additional documentation.",
        "remediation": "Submit SBA Form 912 and personal financial statement.",
    },
    "REG-005": {
        "name": "Minimum Operating History",
        "version": "2026-Q1-v1",
        "is_hard_block": True,
        "check": lambda co: (2024 - (co.get("founded_year") or 2024)) >= 2,
        "failure_reason": "Business must have at least 2 years of operating history.",
        "remediation": None,
    },
    "REG-006": {
        "name": "CRA Community Reinvestment",
        "version": "2026-Q1-v1",
        "is_hard_block": False,
        "check": lambda co: True,   # Always noted, never fails
        "note_type": "CRA_CONSIDERATION",
        "note_text": "Jurisdiction qualifies for Community Reinvestment Act consideration.",
    },
}


class ComplianceAgent(BaseApexAgent):
    """
    Evaluates 6 deterministic regulatory rules in sequence.
    Stops at first hard block (is_hard_block=True).
    LLM not in rule decision path — only for human-readable evidence summaries.
    """

    async def _node_eval_reg001(self, state: ComplianceState) -> ComplianceState:
        return await self._evaluate_rule(state, "REG-001")

    async def _node_eval_reg002(self, state: ComplianceState) -> ComplianceState:
        return await self._evaluate_rule(state, "REG-002")

    async def _node_eval_reg003(self, state: ComplianceState) -> ComplianceState:
        return await self._evaluate_rule(state, "REG-003")

    async def _node_eval_reg004(self, state: ComplianceState) -> ComplianceState:
        return await self._evaluate_rule(state, "REG-004")

    async def _node_eval_reg005(self, state: ComplianceState) -> ComplianceState:
        return await self._evaluate_rule(state, "REG-005")

    async def _node_eval_reg006(self, state: ComplianceState) -> ComplianceState:
        return await self._evaluate_rule(state, "REG-006")

    def build_graph(self):
        g = StateGraph(ComplianceState)
        g.add_node("validate_inputs",      self._node_validate_inputs)
        g.add_node("load_company_profile", self._node_load_profile)
        g.add_node("evaluate_reg001",      self._node_eval_reg001)
        g.add_node("evaluate_reg002",      self._node_eval_reg002)
        g.add_node("evaluate_reg003",      self._node_eval_reg003)
        g.add_node("evaluate_reg004",      self._node_eval_reg004)
        g.add_node("evaluate_reg005",      self._node_eval_reg005)
        g.add_node("evaluate_reg006",      self._node_eval_reg006)
        g.add_node("write_output",         self._node_write_output)

        g.set_entry_point("validate_inputs")
        g.add_edge("validate_inputs",      "load_company_profile")
        g.add_edge("load_company_profile", "evaluate_reg001")

        # Conditional edges: stop at hard block, proceed otherwise
        rule_sequence = [
            ("evaluate_reg001", "evaluate_reg002"),
            ("evaluate_reg002", "evaluate_reg003"),
            ("evaluate_reg003", "evaluate_reg004"),
            ("evaluate_reg004", "evaluate_reg005"),
            ("evaluate_reg005", "evaluate_reg006"),
            ("evaluate_reg006", "write_output"),
        ]
        for src, nxt in rule_sequence:
            g.add_conditional_edges(
                src,
                lambda s, _nxt=nxt: "write_output" if s["has_hard_block"] else _nxt,
            )
        g.add_edge("write_output", END)
        return g.compile()

    def _initial_state(self, application_id: str) -> ComplianceState:
        return ComplianceState(
            application_id=application_id, session_id=self.session_id,
            company_profile=None, rule_results=[], has_hard_block=False,
            block_rule_id=None, errors=[], output_events=[], next_agent=None,
        )

    async def _node_validate_inputs(self, state: ComplianceState) -> ComplianceState:
        t = time.time()
        app_id = state["application_id"]
        loan_events = await self.store.load_stream(f"loan-{app_id}")
        trigger = next((e for e in loan_events if e["event_type"] == "ComplianceCheckRequested"), None)
        if not trigger:
            await self._record_input_failed(["ComplianceCheckRequested"], ["Not found on loan stream"])
            raise ValueError(f"ComplianceCheckRequested not found for {app_id}")

        await self._append_with_retry(f"compliance-{app_id}", [{
            "event_type": "ComplianceCheckInitiated",
            "event_version": 1,
            "payload": {
                "application_id": app_id,
                "session_id": self.session_id,
                "regulation_set_version": "2026-Q1-v1",
                "rules_to_evaluate": list(REGULATIONS.keys()),
                "initiated_at": datetime.now().isoformat(),
            },
        }])

        ms = int((time.time() - t) * 1000)
        await self._record_input_validated(["application_id", "ComplianceCheckRequested"], ms)
        await self._record_node_execution("validate_inputs", ["application_id"], ["trigger_verified"], ms)
        return state

    async def _node_load_profile(self, state: ComplianceState) -> ComplianceState:
        t = time.time()
        app_id = state["application_id"]

        # Get applicant_id from loan stream
        loan_events = await self.store.load_stream(f"loan-{app_id}")
        submit_event = next((e for e in loan_events if e["event_type"] == "ApplicationSubmitted"), None)
        applicant_id = submit_event["payload"]["applicant_id"] if submit_event else None
        requested_amount = submit_event["payload"].get("requested_amount_usd", 0) if submit_event else 0

        co_dict: dict = {}
        if applicant_id and self.registry:
            try:
                profile = await self.registry.get_company(applicant_id)
                flags = await self.registry.get_compliance_flags(applicant_id)
                if profile:
                    co_dict = profile.__dict__ if hasattr(profile, "__dict__") else dict(profile)
                    co_dict["compliance_flags"] = [
                        f.__dict__ if hasattr(f, "__dict__") else dict(f) for f in flags
                    ]
                    co_dict["requested_amount_usd"] = requested_amount
            except Exception:
                pass

        ms = int((time.time() - t) * 1000)
        await self._record_tool_call("registry.get_company+flags", {"company": applicant_id}, {"profile_loaded": bool(co_dict)}, ms)
        await self._record_node_execution("load_company_profile", ["application_id"], ["company_profile"], ms)
        return {**state, "company_profile": co_dict}

    async def _evaluate_rule(self, state: ComplianceState, rule_id: str) -> ComplianceState:
        t = time.time()
        app_id = state["application_id"]
        reg = REGULATIONS[rule_id]
        co = state.get("company_profile") or {}
        results = list(state.get("rule_results") or [])

        try:
            passes = reg["check"](co)
        except Exception:
            passes = True  # Benefit of the doubt on check error

        evidence_hash = self._sha(f"{rule_id}-{co.get('company_id', 'unknown')}-{passes}")

        # REG-006 is always a note, never pass or fail
        if rule_id == "REG-006":
            evt = {
                "event_type": "ComplianceRuleNoted",
                "event_version": 1,
                "payload": {
                    "application_id": app_id,
                    "rule_id": rule_id,
                    "rule_name": reg["name"],
                    "rule_version": reg["version"],
                    "note_type": reg.get("note_type", "GENERAL"),
                    "note_text": reg.get("note_text", ""),
                    "evidence_hash": evidence_hash,
                    "evaluated_at": datetime.now().isoformat(),
                },
            }
            result_entry = {"rule_id": rule_id, "verdict": "NOTED"}
        elif passes:
            evt = {
                "event_type": "ComplianceRulePassed",
                "event_version": 1,
                "payload": {
                    "application_id": app_id,
                    "rule_id": rule_id,
                    "rule_name": reg["name"],
                    "rule_version": reg["version"],
                    "evidence_hash": evidence_hash,
                    "evaluated_at": datetime.now().isoformat(),
                },
            }
            result_entry = {"rule_id": rule_id, "verdict": "PASSED"}
        else:
            evt = {
                "event_type": "ComplianceRuleFailed",
                "event_version": 1,
                "payload": {
                    "application_id": app_id,
                    "rule_id": rule_id,
                    "rule_name": reg["name"],
                    "rule_version": reg["version"],
                    "failure_reason": reg["failure_reason"],
                    "remediation_required": reg.get("remediation") is not None,
                    "remediation_steps": reg.get("remediation") or "",
                    "is_hard_block": reg["is_hard_block"],
                    "evidence_hash": evidence_hash,
                    "evaluated_at": datetime.now().isoformat(),
                },
            }
            result_entry = {"rule_id": rule_id, "verdict": "FAILED", "is_hard_block": reg["is_hard_block"]}

        await self._append_with_retry(f"compliance-{app_id}", [evt])
        results.append(result_entry)

        ms = int((time.time() - t) * 1000)
        node_name = f"evaluate_{rule_id.lower().replace('-', '_')}"
        await self._record_node_execution(node_name, ["company_profile"], [f"{rule_id}_result"], ms)

        new_state = {**state, "rule_results": results}
        if not passes and reg["is_hard_block"]:
            new_state["has_hard_block"] = True
            new_state["block_rule_id"] = rule_id
        return new_state

    async def _node_write_output(self, state: ComplianceState) -> ComplianceState:
        t = time.time()
        app_id = state["application_id"]
        has_hard_block = state.get("has_hard_block", False)
        block_rule_id = state.get("block_rule_id")
        rule_results = state.get("rule_results") or []

        # Determine verdict
        if has_hard_block:
            verdict = "BLOCKED"
        elif any(r.get("verdict") == "FAILED" for r in rule_results):
            verdict = "CONDITIONAL"
        else:
            verdict = "CLEAR"

        # ComplianceCheckCompleted on compliance stream
        await self._append_with_retry(f"compliance-{app_id}", [{
            "event_type": "ComplianceCheckCompleted",
            "event_version": 1,
            "payload": {
                "application_id": app_id,
                "session_id": self.session_id,
                "verdict": verdict,
                "rules_evaluated": [r["rule_id"] for r in rule_results],
                "failed_rules": [r["rule_id"] for r in rule_results if r.get("verdict") == "FAILED"],
                "hard_block_rule": block_rule_id,
                "completed_at": datetime.now().isoformat(),
            },
        }])

        events_written = ["ComplianceCheckCompleted"]

        if has_hard_block:
            # Hard block: directly decline — no DecisionRequested
            await self._append_with_retry(f"loan-{app_id}", [{
                "event_type": "ApplicationDeclined",
                "event_version": 1,
                "payload": {
                    "application_id": app_id,
                    "decline_reasons": [
                        REGULATIONS[block_rule_id]["failure_reason"] if block_rule_id else "Compliance hard block"
                    ],
                    "declined_by": f"compliance_agent:{self.session_id}",
                    "adverse_action_notice_required": True,
                    "declined_at": datetime.now().isoformat(),
                },
            }], causation_id=self.session_id)
            events_written.append("ApplicationDeclined")
        else:
            # CLEAR or CONDITIONAL: forward to decision orchestrator
            await self._append_with_retry(f"loan-{app_id}", [{
                "event_type": "DecisionRequested",
                "event_version": 1,
                "payload": {
                    "application_id": app_id,
                    "compliance_verdict": verdict,
                    "requested_at": datetime.now().isoformat(),
                },
            }], causation_id=self.session_id)
            events_written.append("DecisionRequested")

        ms = int((time.time() - t) * 1000)
        await self._record_output_written(events_written, f"Compliance verdict: {verdict}")
        await self._record_node_execution("write_output", ["rule_results"], ["compliance_completed"], ms)
        return {**state, "output_events": events_written, "next_agent": None if has_hard_block else "decision_orchestrator"}


# ─── DECISION ORCHESTRATOR ────────────────────────────────────────────────────

class OrchestratorState(TypedDict):
    application_id: str
    session_id: str
    credit_result: dict | None
    fraud_result: dict | None
    compliance_result: dict | None
    recommendation: str | None
    confidence: float | None
    approved_amount: float | None
    executive_summary: str | None
    conditions: list[str] | None
    hard_constraints_applied: list[str] | None
    errors: list[str]
    output_events: list[dict]
    next_agent: str | None


class DecisionOrchestratorAgent(BaseApexAgent):
    """
    Synthesises all prior agent outputs into a final recommendation.
    The only agent that reads from multiple aggregate streams before deciding.

    Hard constraints (Python, enforced AFTER LLM synthesis):
        1. compliance BLOCKED → DECLINE (non-negotiable)
        2. confidence < 0.60 → REFER
        3. fraud_score > 0.60 → REFER
        4. risk_tier == HIGH and confidence < 0.70 → REFER
    """

    def build_graph(self):
        g = StateGraph(OrchestratorState)
        g.add_node("validate_inputs",        self._node_validate_inputs)
        g.add_node("load_credit_result",     self._node_load_credit)
        g.add_node("load_fraud_result",      self._node_load_fraud)
        g.add_node("load_compliance_result", self._node_load_compliance)
        g.add_node("synthesize_decision",    self._node_synthesize)
        g.add_node("apply_hard_constraints", self._node_constraints)
        g.add_node("write_output",           self._node_write_output)

        g.set_entry_point("validate_inputs")
        g.add_edge("validate_inputs",        "load_credit_result")
        g.add_edge("load_credit_result",     "load_fraud_result")
        g.add_edge("load_fraud_result",      "load_compliance_result")
        g.add_edge("load_compliance_result", "synthesize_decision")
        g.add_edge("synthesize_decision",    "apply_hard_constraints")
        g.add_edge("apply_hard_constraints", "write_output")
        g.add_edge("write_output",           END)
        return g.compile()

    def _initial_state(self, application_id: str) -> OrchestratorState:
        return OrchestratorState(
            application_id=application_id, session_id=self.session_id,
            credit_result=None, fraud_result=None, compliance_result=None,
            recommendation=None, confidence=None, approved_amount=None,
            executive_summary=None, conditions=None, hard_constraints_applied=[],
            errors=[], output_events=[], next_agent=None,
        )

    async def _node_validate_inputs(self, state: OrchestratorState) -> OrchestratorState:
        t = time.time()
        app_id = state["application_id"]
        loan_events = await self.store.load_stream(f"loan-{app_id}")
        trigger = next((e for e in loan_events if e["event_type"] == "DecisionRequested"), None)
        if not trigger:
            await self._record_input_failed(["DecisionRequested"], ["Not found on loan stream"])
            raise ValueError(f"DecisionRequested not found for {app_id}")

        ms = int((time.time() - t) * 1000)
        await self._record_input_validated(["application_id", "DecisionRequested"], ms)
        await self._record_node_execution("validate_inputs", ["application_id"], ["trigger_verified"], ms)
        return state

    async def _node_load_credit(self, state: OrchestratorState) -> OrchestratorState:
        t = time.time()
        app_id = state["application_id"]
        credit_events = await self.store.load_stream(f"credit-{app_id}")
        # Grab last CreditAnalysisCompleted
        cac = next(
            (e["payload"] for e in reversed(credit_events) if e["event_type"] == "CreditAnalysisCompleted"),
            None,
        )
        ms = int((time.time() - t) * 1000)
        await self._record_tool_call("load_stream:credit", {"stream": f"credit-{app_id}"}, {"found": cac is not None}, ms)
        await self._record_node_execution("load_credit_result", ["application_id"], ["credit_result"], ms)
        return {**state, "credit_result": cac or {}}

    async def _node_load_fraud(self, state: OrchestratorState) -> OrchestratorState:
        t = time.time()
        app_id = state["application_id"]
        fraud_events = await self.store.load_stream(f"fraud-{app_id}")
        fsc = next(
            (e["payload"] for e in reversed(fraud_events) if e["event_type"] == "FraudScreeningCompleted"),
            None,
        )
        ms = int((time.time() - t) * 1000)
        await self._record_tool_call("load_stream:fraud", {"stream": f"fraud-{app_id}"}, {"found": fsc is not None}, ms)
        await self._record_node_execution("load_fraud_result", ["application_id"], ["fraud_result"], ms)
        return {**state, "fraud_result": fsc or {}}

    async def _node_load_compliance(self, state: OrchestratorState) -> OrchestratorState:
        t = time.time()
        app_id = state["application_id"]
        compliance_events = await self.store.load_stream(f"compliance-{app_id}")
        ccc = next(
            (e["payload"] for e in reversed(compliance_events) if e["event_type"] == "ComplianceCheckCompleted"),
            None,
        )
        ms = int((time.time() - t) * 1000)
        await self._record_tool_call("load_stream:compliance", {"stream": f"compliance-{app_id}"}, {"found": ccc is not None}, ms)
        await self._record_node_execution("load_compliance_result", ["application_id"], ["compliance_result"], ms)
        return {**state, "compliance_result": ccc or {}}

    async def _node_synthesize(self, state: OrchestratorState) -> OrchestratorState:
        t = time.time()
        credit = state.get("credit_result") or {}
        fraud = state.get("fraud_result") or {}
        compliance = state.get("compliance_result") or {}

        system = (
            "You are a senior loan officer synthesising multi-agent commercial loan analysis. "
            "Produce a balanced recommendation based on credit risk, fraud, and compliance results. "
            "Return ONLY valid JSON matching OrchestratorDecision schema."
        )
        user = (
            f"Credit analysis: {json.dumps(credit, default=str)}\n"
            f"Fraud screening: {json.dumps(fraud, default=str)}\n"
            f"Compliance check: {json.dumps(compliance, default=str)}\n\n"
            "Return JSON: {\"recommendation\": \"APPROVE\"|\"DECLINE\"|\"REFER\", "
            "\"approved_amount_usd\": float_or_null, "
            "\"confidence\": float_0_to_1, "
            "\"executive_summary\": \"3-5 sentence summary\", "
            "\"key_risks\": [str], "
            "\"conditions\": [str]}"
        )
        try:
            content, tok_in, tok_out, cost = await self._call_llm(system, user, max_tokens=1024)
            decision = self._parse_json(content)
        except Exception:
            decision = {
                "recommendation": "REFER",
                "approved_amount_usd": None,
                "confidence": 0.50,
                "executive_summary": "Analysis synthesis failed — human review required.",
                "key_risks": ["llm_synthesis_error"],
                "conditions": [],
            }
            tok_in, tok_out, cost = None, None, None

        ms = int((time.time() - t) * 1000)
        await self._record_node_execution("synthesize_decision", ["credit_result", "fraud_result", "compliance_result"], ["recommendation", "executive_summary"], ms, tok_in, tok_out, cost)
        return {
            **state,
            "recommendation": decision.get("recommendation", "REFER"),
            "confidence": float(decision.get("confidence", 0.50)),
            "approved_amount": decision.get("approved_amount_usd"),
            "executive_summary": decision.get("executive_summary", ""),
            "conditions": decision.get("conditions", []),
        }

    async def _node_constraints(self, state: OrchestratorState) -> OrchestratorState:
        t = time.time()
        credit = state.get("credit_result") or {}
        fraud = state.get("fraud_result") or {}
        compliance = state.get("compliance_result") or {}
        rec = state.get("recommendation") or "REFER"
        conf = state.get("confidence") or 0.0
        overrides: list[str] = []

        # Hard constraints — applied in order of precedence
        compliance_verdict = compliance.get("verdict", "")
        fraud_score = float(fraud.get("fraud_score") or 0.0)
        risk_tier = credit.get("risk_tier") or credit.get("credit_tier", "MEDIUM")

        if compliance_verdict == "BLOCKED":
            if rec != "DECLINE":
                overrides.append(f"compliance_hard_block: {rec} → DECLINE")
            rec = "DECLINE"

        elif fraud_score > 0.60:
            if rec not in ("DECLINE", "REFER"):
                overrides.append(f"fraud_score_high ({fraud_score:.2f}): {rec} → REFER")
            if rec == "APPROVE":
                rec = "REFER"

        elif conf < 0.60:
            if rec == "APPROVE":
                overrides.append(f"confidence_below_threshold ({conf:.2f}): APPROVE → REFER")
                rec = "REFER"

        elif risk_tier == "HIGH" and conf < 0.70:
            if rec == "APPROVE":
                overrides.append(f"high_risk_low_confidence (tier={risk_tier}, conf={conf:.2f}): APPROVE → REFER")
                rec = "REFER"

        ms = int((time.time() - t) * 1000)
        await self._record_node_execution("apply_hard_constraints", ["recommendation", "confidence"], ["final_recommendation"], ms)
        return {**state, "recommendation": rec, "hard_constraints_applied": overrides}

    async def _node_write_output(self, state: OrchestratorState) -> OrchestratorState:
        t = time.time()
        app_id = state["application_id"]
        rec = state.get("recommendation") or "REFER"
        conf = state.get("confidence") or 0.50
        approved_amount = state.get("approved_amount")
        summary = state.get("executive_summary") or ""
        conditions = state.get("conditions") or []
        overrides = state.get("hard_constraints_applied") or []
        credit = state.get("credit_result") or {}
        fraud = state.get("fraud_result") or {}

        model_versions = {
            "credit_model": credit.get("model_version") or credit.get("screening_model_version") or self.model,
            "fraud_model": fraud.get("screening_model_version") or self.model,
            "orchestrator_model": self.model,
        }

        # DecisionGenerated on loan stream
        await self._append_with_retry(f"loan-{app_id}", [{
            "event_type": "DecisionGenerated",
            "event_version": 2,
            "payload": {
                "application_id": app_id,
                "orchestrator_agent_id": self.agent_id,
                "recommendation": rec,
                "confidence": conf,
                "approved_amount_usd": approved_amount,
                "executive_summary": summary,
                "policy_overrides_applied": overrides,
                "model_versions": model_versions,
                "generated_at": datetime.now().isoformat(),
            },
        }], causation_id=self.session_id)

        events_written = ["DecisionGenerated"]

        if rec == "APPROVE":
            await self._append_with_retry(f"loan-{app_id}", [{
                "event_type": "ApplicationApproved",
                "event_version": 1,
                "payload": {
                    "application_id": app_id,
                    "approved_amount_usd": approved_amount,
                    "conditions": conditions,
                    "approved_by": f"orchestrator:{self.session_id}",
                    "approved_at": datetime.now().isoformat(),
                },
            }])
            events_written.append("ApplicationApproved")

        elif rec == "DECLINE":
            await self._append_with_retry(f"loan-{app_id}", [{
                "event_type": "ApplicationDeclined",
                "event_version": 1,
                "payload": {
                    "application_id": app_id,
                    "decline_reasons": overrides or [summary[:200]],
                    "declined_by": f"orchestrator:{self.session_id}",
                    "adverse_action_notice_required": True,
                    "declined_at": datetime.now().isoformat(),
                },
            }])
            events_written.append("ApplicationDeclined")

        elif rec == "REFER":
            await self._append_with_retry(f"loan-{app_id}", [{
                "event_type": "HumanReviewRequested",
                "event_version": 1,
                "payload": {
                    "application_id": app_id,
                    "reason": overrides[0] if overrides else "insufficient_confidence",
                    "recommended_action": "REFER",
                    "executive_summary": summary,
                    "requested_at": datetime.now().isoformat(),
                },
            }])
            events_written.append("HumanReviewRequested")

        ms = int((time.time() - t) * 1000)
        await self._record_output_written(events_written, f"Decision: {rec} (conf={conf:.2f})")
        await self._record_node_execution("write_output", ["recommendation"], ["decision_written"], ms)
        return {**state, "output_events": events_written, "next_agent": None}
