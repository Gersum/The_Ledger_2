import asyncio
import asyncpg
import random
from datetime import datetime, date, timedelta

DB_URL = "postgresql://postgres:postgres@localhost:5433/ledger"

async def seed():
    conn = await asyncpg.connect(DB_URL)
    try:
        print("Cleaning up old tables for a clean seed...")
        await conn.execute("DROP SCHEMA IF EXISTS applicant_registry CASCADE")
        await conn.execute("CREATE SCHEMA applicant_registry")
        
        print("Creating tables...")
        await conn.execute("""
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
        """)
        
        insert_query = """
            INSERT INTO applicant_registry.companies 
            (company_id, name, industry, naics, jurisdiction, legal_type, founded_year, employee_count, 
             ein, address_city, address_state, relationship_start, account_manager,
             risk_segment, trajectory, submission_channel, ip_region)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
        """
        
        print("Inserting Apex Corp...")
        await conn.execute(insert_query, 
            "COMP-APEX-001", "Apex Corp", "Technology", "541511", "DE", "C-Corp", 2015, 120, 
            "12-3456789", "San Francisco", "CA", date(2021, 1, 1), "Alex Manager",
            "CORE", "GROWTH", "DIRECT", "US")
        
        print("Inserting Gersum Tech...")
        await conn.execute(insert_query, 
            "COMP-GERSUM-002", "Gersum Technologies", "AI & Robotics", "541715", "NY", "LLC", 2019, 45, 
            "98-7654321", "New York", "NY", date(2022, 6, 15), "Beth Manager",
            "EMERGING", "HYPERGROWTH", "PARTNER", "US")

        print("Inserting financials...")
        fin_query = """
            INSERT INTO applicant_registry.financial_history 
            (company_id, fiscal_year, total_revenue, gross_profit, operating_income, ebitda, net_income, total_assets, total_liabilities, total_equity, long_term_debt, cash_and_equivalents, current_assets, current_liabilities, accounts_receivable, inventory, debt_to_equity, current_ratio, debt_to_ebitda, interest_coverage_ratio, gross_margin, ebitda_margin, net_margin)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23)
        """
        for cid in ["COMP-APEX-001", "COMP-GERSUM-002"]:
            for year in [2022, 2023]:
                await conn.execute(fin_query, cid, year, 5000000.0, 3500000.0, 1000000.0, 1200000.0, 800000.0, 10000000.0, 4000000.0, 6000000.0, 2000000.0, 500000.0, 2000000.0, 1000000.0, 1000000.0, 200000.0, 0.33, 2.0, 1.66, 5.0, 0.7, 0.24, 0.16)

        print("Inserting compliance flags...")
        comp_query = """
            INSERT INTO applicant_registry.compliance_flags (company_id, flag_type, severity, is_active, added_date, note)
            VALUES ($1, $2, $3, $4, $5, $6)
        """
        await conn.execute(comp_query, "COMP-APEX-001", "KYC_VERIFIED", "LOW", True, datetime.now(), "All founders verified.")
        await conn.execute(comp_query, "COMP-GERSUM-002", "KYC_VERIFIED", "LOW", True, datetime.now(), "Entity structure confirmed.")

        print("Seeding complete!")
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(seed())
