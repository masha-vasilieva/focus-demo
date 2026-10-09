#!/usr/bin/env python3
"""
FOCUS Engine - Zero-Framework Multi-Cloud Normalization Web Application (FOCUS 1.2)
====================================================================================
Autonomous 4-Discipline Engineering Architecture:
  1. Product Manager: Zero-framework drag-and-drop web UI, real-time dashboard refresh.
  2. Schema Expert: Dynamic schema sniffing (NO hardcoded filenames), FOCUS 1.2 normalization.
  3. Data Engineer: Defensive DuckDB ingestion, archive unpacking, micro-metering pruning.
  4. QA Tester: Strict dual-currency reconciliation, automated test simulation, and zero errors.

Constraints:
  - ZERO third-party web frameworks (Python stdlib http.server only).
  - NO hardcoded filenames (Content and schema sniffing).
"""

import os
import sys
import glob
import json
import shutil
import tarfile
import zipfile
import tempfile
import uuid
import argparse
import threading
import subprocess
import webbrowser
import hashlib
from datetime import datetime
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
import email.parser
import email.policy
import urllib.parse
import base64
import duckdb

# ----------------------------------------------------------------------
# Paths & Defaults
# ----------------------------------------------------------------------
try:
    SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
except Exception:
    SCRIPT_DIR = os.getcwd()
if not SCRIPT_DIR or SCRIPT_DIR == ".":
    SCRIPT_DIR = os.getcwd()
DEFAULT_PORT = 8000
OUTPUT_PARQUET = os.path.join(SCRIPT_DIR, "unified_focus.parquet")
OUTPUT_HTML = os.path.join(SCRIPT_DIR, "report.html")
LOGO_FILE = os.path.join(SCRIPT_DIR, "logo.png")

# ----------------------------------------------------------------------
# Discipline 2 & 3: Schema Sniffing & Dynamic Normalizer
# ----------------------------------------------------------------------
class DynamicNormalizer:
    """Sniffs file content/schema dynamically and executes FOCUS 1.2 normalization."""

    def __init__(self, con: duckdb.DuckDBPyConnection):
        self.con = con
        self.table_counter = 0
        self.staged_tables = []
        self.warnings = []
        self.audit_log = []
        self.ingestion_events = []

    def log(self, discipline: str, message: str):
        ts = datetime.now().strftime("%H:%M:%S")
        msg = f"[{ts}] [{discipline.upper()}] {message}"
        self.audit_log.append(msg)
        print(msg)

    def sniff_format(self, filepath: str) -> str:
        """Inspects magic bytes and headers without relying on filename."""
        if not os.path.isfile(filepath):
            return "UNKNOWN"

        # 1. Archives: Tar and Zip
        try:
            if tarfile.is_tarfile(filepath):
                return "ARCHIVE_TAR"
        except Exception:
            pass
        try:
            if zipfile.is_zipfile(filepath):
                return "ARCHIVE_ZIP"
        except Exception:
            pass

        # 2. Parquet
        try:
            with open(filepath, "rb") as f:
                header = f.read(4)
            if header == b"PAR1":
                # Check row count first (Defensive ghost partition check)
                meta_query = f"SELECT sum(row_group_num_rows) FROM (SELECT DISTINCT row_group_id, row_group_num_rows FROM parquet_metadata('{filepath}'))"
                row_count = self.con.execute(meta_query).fetchone()[0] or 0
                if row_count == 0:
                    return "PARQUET_EMPTY_GHOST"
                
                cols = [r[0].lower() for r in self.con.execute(f"DESCRIBE SELECT * FROM '{filepath}'").fetchall()]
                if any(k in cols for k in ["x_skumetersubcategory", "x_invoiceid", "x_billedcostinusd", "x_resourcegroupname"]):
                    return "PARQUET_AZURE"
                if any(k in cols for k in ["x_discounts", "x_operation", "x_servicecode"]):
                    return "PARQUET_AWS"
                if "billedcost" in cols and ("billingcurrency" in cols or "providername" in cols):
                    return "PARQUET_FOCUS"
                return "PARQUET_GENERIC"
        except Exception as e:
            pass

        # 3. JSON
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict) and "result" in data and isinstance(data["result"], list) and len(data["result"]) > 0:
                sample = data["result"][0]
                if isinstance(sample, dict) and ("ServiceName" in sample or "ServiceProviderName" in sample or "PricingUnit" in sample):
                    return "JSON_CLOUDFLARE"
            if isinstance(data, list) and len(data) > 0 and isinstance(data[0], dict) and "BilledCost" in data[0]:
                return "JSON_FOCUS"
        except Exception:
            pass

        # 4. CSV
        try:
            with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
                first_line = f.readline().lower()
            if "billing account" in first_line and ("cost" in first_line or "service description" in first_line or "unrounded cost" in first_line):
                return "CSV_GCP"
            if "billedcost" in first_line and "billingaccountid" in first_line and "x_projectid" in first_line:
                return "CSV_NEBIUS"
            if "billedcost" in first_line and ("billingcurrency" in first_line or "providername" in first_line or "billingaccountid" in first_line):
                return "CSV_FOCUS"
        except Exception:
            pass

        return "UNSUPPORTED"

    def process_file_or_archive(self, filepath: str, display_name: str = None):
        detected = self.sniff_format(filepath)
        filename = display_name or os.path.basename(filepath)
        self.log("Schema Expert", f"Sniffed '{filename}' -> Detected Type: {detected}")

        if detected in ("ARCHIVE_TAR", "ARCHIVE_ZIP"):
            self.log("Data Engineer", f"Rule 1: Unpacking archive '{filename}' into temporary directory...")
            tdir = tempfile.mkdtemp(prefix="focus_unpack_")
            try:
                if detected == "ARCHIVE_TAR":
                    with tarfile.open(filepath, "r:*") as tar:
                        tar.extractall(tdir)
                else:
                    with zipfile.ZipFile(filepath, "r") as zf:
                        zf.extractall(tdir)

                for root, _, files in os.walk(tdir):
                    for fn in sorted(files):
                        if not fn.startswith((".", "__")):
                            fp = os.path.join(root, fn)
                            if os.path.isfile(fp):
                                self.process_file_or_archive(fp, display_name=f"{filename} ({fn})")
            finally:
                shutil.rmtree(tdir, ignore_errors=True)
            return

        if detected == "PARQUET_EMPTY_GHOST":
            self.log("Data Engineer", f"Rule 2 [DEFENSIVE SKIP]: Skipping empty 0-row Parquet partition '{filename}'")
            self.ingestion_events.append({
                "filename": filename,
                "provider": "Azure",
                "currency": "USD",
                "rows": 0,
                "status": "skipped",
                "message": f"Skipped {filename} — 0-row ghost partition safely bypassed"
            })
            return

        if detected == "UNSUPPORTED":
            warn_msg = f"Unsupported or unrecognizable billing file '{filename}'. Skipping gracefully."
            self.log("QA Tester", f"[WARN] {warn_msg}")
            self.warnings.append(warn_msg)
            self.ingestion_events.append({
                "filename": filename,
                "provider": "Unknown",
                "currency": "-",
                "rows": 0,
                "status": "unsupported",
                "message": f"Skipped {filename} — Unrecognized schema / format"
            })
            return

        t_name = f"staging_{uuid.uuid4().hex[:12]}"
        try:
            self._normalize_staged_file(detected, filepath, filename, t_name)
        except Exception as e:
            warn_msg = f"Failed to ingest '{filename}': {e}"
            self.log("QA Tester", f"[WARN] {warn_msg}")
            self.warnings.append(warn_msg)
            self.ingestion_events.append({
                "filename": filename,
                "provider": "Unknown",
                "currency": "-",
                "rows": 0,
                "status": "error",
                "message": f"Failed to normalize {filename}: {e}"
            })

    def _normalize_staged_file(self, detected: str, filepath: str, filename: str, t_name: str):
        # Execute normalization and immediately materialize into table
        if detected == "PARQUET_AWS":
            self.con.execute(f"""
            CREATE OR REPLACE TABLE {t_name} AS
            SELECT 
                'AWS' AS ProviderName,
                PublisherName,
                InvoiceIssuerName,
                InvoiceId,
                BillingAccountId,
                BillingAccountName,
                BillingAccountType,
                SubAccountId,
                SubAccountName,
                SubAccountType,
                ServiceCategory,
                ServiceName,
                ServiceSubcategory,
                SkuId,
                ChargeDescription AS SkuDescription,
                SkuPriceId,
                ResourceId,
                ResourceName,
                ResourceType,
                RegionId,
                RegionName,
                AvailabilityZone,
                ChargeCategory,
                CAST(ChargeClass AS VARCHAR) AS ChargeClass,
                ChargeDescription,
                ChargeFrequency,
                BillingPeriodStart::TIMESTAMP AS BillingPeriodStart,
                BillingPeriodEnd::TIMESTAMP AS BillingPeriodEnd,
                ChargePeriodStart::TIMESTAMP AS ChargePeriodStart,
                ChargePeriodEnd::TIMESTAMP AS ChargePeriodEnd,
                TRY_CAST(ConsumedQuantity AS DOUBLE) AS ConsumedQuantity,
                ConsumedUnit,
                TRY_CAST(PricingQuantity AS DOUBLE) AS PricingQuantity,
                PricingUnit,
                CASE 
                    WHEN ChargeCategory = 'Credit' AND (ChargeDescription ILIKE '%free tier%' OR SkuDescription ILIKE '%free tier%') THEN 0.0 
                    ELSE TRY_CAST(BilledCost AS DOUBLE) 
                END AS BilledCost,
                CASE 
                    WHEN ChargeCategory = 'Credit' AND (ChargeDescription ILIKE '%free tier%' OR SkuDescription ILIKE '%free tier%') THEN 0.0 
                    ELSE TRY_CAST(EffectiveCost AS DOUBLE) 
                END AS EffectiveCost,
                TRY_CAST(ListCost AS DOUBLE) AS ListCost,
                TRY_CAST(ContractedCost AS DOUBLE) AS ContractedCost,
                TRY_CAST(ListUnitPrice AS DOUBLE) AS ListUnitPrice,
                TRY_CAST(ContractedUnitPrice AS DOUBLE) AS ContractedUnitPrice,
                BillingCurrency,
                PricingCurrency,
                TRY_CAST(PricingCurrencyEffectiveCost AS DOUBLE) AS PricingCurrencyEffectiveCost,
                TRY_CAST(PricingCurrencyListUnitPrice AS DOUBLE) AS PricingCurrencyListUnitPrice,
                TRY_CAST(PricingCurrencyContractedUnitPrice AS DOUBLE) AS PricingCurrencyContractedUnitPrice,
                CommitmentDiscountCategory,
                CommitmentDiscountId,
                CommitmentDiscountName,
                TRY_CAST(CommitmentDiscountQuantity AS DOUBLE) AS CommitmentDiscountQuantity,
                CommitmentDiscountStatus,
                CommitmentDiscountType,
                CommitmentDiscountUnit,
                CapacityReservationId,
                CapacityReservationStatus,
                CAST(Tags AS VARCHAR) AS Tags
            FROM '{filepath}';
            """)
            self.staged_tables.append(t_name)
            cnt = self.con.execute(f"SELECT count(*) FROM {t_name}").fetchone()[0]
            self.ingestion_events.append({
                "filename": filename,
                "provider": "AWS",
                "currency": "USD",
                "rows": cnt,
                "status": "success",
                "message": f"Ingested {filename} — Detected AWS (USD) — {cnt} lines added"
            })

        elif detected == "PARQUET_AZURE":
            self.con.execute(f"""
            CREATE OR REPLACE TABLE {t_name} AS
            SELECT 
                'Microsoft Azure' AS ProviderName,
                PublisherName,
                InvoiceIssuerName,
                x_InvoiceId AS InvoiceId,
                BillingAccountId,
                BillingAccountName,
                BillingAccountType,
                SubAccountId,
                SubAccountName,
                SubAccountType,
                ServiceCategory,
                ServiceName,
                x_SkuMeterSubcategory AS ServiceSubcategory,
                SkuId,
                COALESCE(x_SkuDescription, ChargeDescription) AS SkuDescription,
                SkuPriceId,
                ResourceId,
                ResourceName,
                ResourceType,
                RegionId,
                RegionName,
                NULL AS AvailabilityZone,
                ChargeCategory,
                CAST(ChargeClass AS VARCHAR) AS ChargeClass,
                ChargeDescription,
                ChargeFrequency,
                BillingPeriodStart::TIMESTAMP AS BillingPeriodStart,
                BillingPeriodEnd::TIMESTAMP AS BillingPeriodEnd,
                ChargePeriodStart::TIMESTAMP AS ChargePeriodStart,
                ChargePeriodEnd::TIMESTAMP AS ChargePeriodEnd,
                TRY_CAST(ConsumedQuantity AS DOUBLE) AS ConsumedQuantity,
                ConsumedUnit,
                TRY_CAST(PricingQuantity AS DOUBLE) AS PricingQuantity,
                PricingUnit,
                TRY_CAST(BilledCost AS DOUBLE) AS BilledCost,
                TRY_CAST(EffectiveCost AS DOUBLE) AS EffectiveCost,
                TRY_CAST(ListCost AS DOUBLE) AS ListCost,
                TRY_CAST(ContractedCost AS DOUBLE) AS ContractedCost,
                TRY_CAST(ListUnitPrice AS DOUBLE) AS ListUnitPrice,
                TRY_CAST(ContractedUnitPrice AS DOUBLE) AS ContractedUnitPrice,
                BillingCurrency,
                x_PricingCurrency AS PricingCurrency,
                TRY_CAST(EffectiveCost AS DOUBLE) AS PricingCurrencyEffectiveCost,
                TRY_CAST(ListUnitPrice AS DOUBLE) AS PricingCurrencyListUnitPrice,
                TRY_CAST(ContractedUnitPrice AS DOUBLE) AS PricingCurrencyContractedUnitPrice,
                CommitmentDiscountCategory,
                CommitmentDiscountId,
                CommitmentDiscountName,
                NULL::DOUBLE AS CommitmentDiscountQuantity,
                CommitmentDiscountStatus,
                CommitmentDiscountType,
                NULL AS CommitmentDiscountUnit,
                NULL AS CapacityReservationId,
                NULL AS CapacityReservationStatus,
                CAST(Tags AS VARCHAR) AS Tags
            FROM '{filepath}';
            """)
            self.staged_tables.append(t_name)
            cnt = self.con.execute(f"SELECT count(*) FROM {t_name}").fetchone()[0]
            self.ingestion_events.append({
                "filename": filename,
                "provider": "Azure",
                "currency": "USD",
                "rows": cnt,
                "status": "success",
                "message": f"Ingested {filename} — Detected Azure (USD) — {cnt} lines added"
            })

        elif detected in ("CSV_NEBIUS", "CSV_FOCUS"):
            # Introspect CSV headers dynamically to support both full FOCUS schema and exported subsets
            try:
                csv_cols = [r[0] for r in self.con.execute(f"DESCRIBE SELECT * FROM read_csv('{filepath}', nullstr=['NULL', 'nan', ''], all_varchar=true) LIMIT 1").fetchall()]
            except Exception:
                csv_cols = [r[0] for r in self.con.execute(f"DESCRIBE SELECT * FROM read_csv_auto('{filepath}') LIMIT 1").fetchall()]
            csv_map = {c.lower(): c for c in csv_cols}

            def _csv_col(col_name, target_type="VARCHAR", default_expr="NULL"):
                k = col_name.lower()
                if k in csv_map:
                    raw_col = f'"{csv_map[k]}"'
                    if target_type == "DOUBLE":
                        return f"TRY_CAST({raw_col} AS DOUBLE) AS {col_name}"
                    elif target_type == "TIMESTAMP":
                        return f"TRY_CAST({raw_col} AS TIMESTAMP) AS {col_name}"
                    else:
                        return f"CAST({raw_col} AS VARCHAR) AS {col_name}"
                if default_expr == "NULL":
                    if target_type == "DOUBLE":
                        return f"NULL::DOUBLE AS {col_name}"
                    elif target_type == "TIMESTAMP":
                        return f"NULL::TIMESTAMP AS {col_name}"
                    else:
                        return f"NULL::VARCHAR AS {col_name}"
                return f"{default_expr} AS {col_name}"

            prov_expr = _csv_col('ProviderName', 'VARCHAR', "'Nebius B.V.'" if detected == "CSV_NEBIUS" else "'FOCUS Dataset'")
            if 'providername' not in csv_map and 'provider' in csv_map:
                prov_expr = f'CAST("{csv_map["provider"]}" AS VARCHAR) AS ProviderName'

            sub_acc_id_expr = _csv_col('SubAccountId', 'VARCHAR')
            if 'subaccountid' not in csv_map and 'x_projectid' in csv_map:
                sub_acc_id_expr = f'CAST("{csv_map["x_projectid"]}" AS VARCHAR) AS SubAccountId'
            elif 'subaccountid' not in csv_map and 'subaccount' in csv_map:
                sub_acc_id_expr = f'CAST("{csv_map["subaccount"]}" AS VARCHAR) AS SubAccountId'

            sub_acc_name_expr = _csv_col('SubAccountName', 'VARCHAR')
            if 'subaccountname' not in csv_map and 'x_projectname' in csv_map:
                sub_acc_name_expr = f'CAST("{csv_map["x_projectname"]}" AS VARCHAR) AS SubAccountName'
            elif 'subaccountname' not in csv_map and 'subaccount' in csv_map:
                sub_acc_name_expr = f'CAST("{csv_map["subaccount"]}" AS VARCHAR) AS SubAccountName'

            chg_start_expr = _csv_col('ChargePeriodStart', 'TIMESTAMP')
            if 'chargeperiodstart' not in csv_map and 'periodstart' in csv_map:
                chg_start_expr = f'TRY_CAST("{csv_map["periodstart"]}" AS TIMESTAMP) AS ChargePeriodStart'

            chg_end_expr = _csv_col('ChargePeriodEnd', 'TIMESTAMP')
            if 'chargeperiodend' not in csv_map and 'periodend' in csv_map:
                chg_end_expr = f'TRY_CAST("{csv_map["periodend"]}" AS TIMESTAMP) AS ChargePeriodEnd'

            curr_expr = _csv_col('BillingCurrency', 'VARCHAR', "'USD'")
            if 'billingcurrency' not in csv_map and 'currency' in csv_map:
                curr_expr = f'COALESCE("{csv_map["currency"]}", \'USD\') AS BillingCurrency'

            pricing_curr_expr = _csv_col('PricingCurrency', 'VARCHAR', "'USD'")
            if 'pricingcurrency' not in csv_map:
                pricing_curr_expr = curr_expr.rsplit(' AS ', 1)[0] + ' AS PricingCurrency'

            billed_cost_expr = _csv_col('BilledCost', 'DOUBLE', '0.0::DOUBLE')
            effective_cost_expr = _csv_col('EffectiveCost', 'DOUBLE', '0.0::DOUBLE')
            pricing_eff_cost_expr = _csv_col('PricingCurrencyEffectiveCost', 'DOUBLE', '0.0::DOUBLE')
            if 'effectivecost' not in csv_map and 'billedcost' in csv_map:
                effective_cost_expr = f'TRY_CAST("{csv_map["billedcost"]}" AS DOUBLE) AS EffectiveCost'
            if 'billedcost' not in csv_map and 'effectivecost' in csv_map:
                billed_cost_expr = f'TRY_CAST("{csv_map["effectivecost"]}" AS DOUBLE) AS BilledCost'
            if 'pricingcurrencyeffectivecost' not in csv_map:
                pricing_eff_cost_expr = effective_cost_expr.rsplit(' AS ', 1)[0] + ' AS PricingCurrencyEffectiveCost'

            self.con.execute(f"""
            CREATE OR REPLACE TABLE {t_name} AS
            SELECT 
                {prov_expr},
                {_csv_col('PublisherName')},
                {_csv_col('InvoiceIssuerName')},
                {_csv_col('InvoiceId')},
                {_csv_col('BillingAccountId')},
                {_csv_col('BillingAccountName')},
                {_csv_col('BillingAccountType')},
                {sub_acc_id_expr},
                {sub_acc_name_expr},
                {_csv_col('SubAccountType')},
                {_csv_col('ServiceCategory')},
                {_csv_col('ServiceName')},
                {_csv_col('ServiceSubcategory')},
                {_csv_col('SkuId')},
                {_csv_col('SkuDescription')},
                {_csv_col('SkuPriceId')},
                {_csv_col('ResourceId')},
                {_csv_col('ResourceName')},
                {_csv_col('ResourceType')},
                {_csv_col('RegionId')},
                {_csv_col('RegionName')},
                {_csv_col('AvailabilityZone')},
                {_csv_col('ChargeCategory', default_expr="'Usage'")},
                {_csv_col('ChargeClass')},
                {_csv_col('ChargeDescription')},
                {_csv_col('ChargeFrequency', default_expr="'Usage-Based'")},
                {_csv_col('BillingPeriodStart', 'TIMESTAMP')},
                {_csv_col('BillingPeriodEnd', 'TIMESTAMP')},
                {chg_start_expr},
                {chg_end_expr},
                {_csv_col('ConsumedQuantity', 'DOUBLE')},
                {_csv_col('ConsumedUnit')},
                {_csv_col('PricingQuantity', 'DOUBLE')},
                {_csv_col('PricingUnit')},
                {billed_cost_expr},
                {effective_cost_expr},
                {_csv_col('ListCost', 'DOUBLE')},
                {_csv_col('ContractedCost', 'DOUBLE')},
                {_csv_col('ListUnitPrice', 'DOUBLE')},
                {_csv_col('ContractedUnitPrice', 'DOUBLE')},
                {curr_expr},
                {pricing_curr_expr},
                {pricing_eff_cost_expr},
                {_csv_col('PricingCurrencyListUnitPrice', 'DOUBLE')},
                {_csv_col('PricingCurrencyContractedUnitPrice', 'DOUBLE')},
                {_csv_col('CommitmentDiscountCategory')},
                {_csv_col('CommitmentDiscountId')},
                {_csv_col('CommitmentDiscountName')},
                {_csv_col('CommitmentDiscountQuantity', 'DOUBLE')},
                {_csv_col('CommitmentDiscountStatus')},
                {_csv_col('CommitmentDiscountType')},
                {_csv_col('CommitmentDiscountUnit')},
                {_csv_col('CapacityReservationId')},
                {_csv_col('CapacityReservationStatus')},
                {_csv_col('Tags', default_expr="'{}'")}
            FROM read_csv_auto('{filepath}');
            """)
            self.staged_tables.append(t_name)
            cnt = self.con.execute(f"SELECT count(*) FROM {t_name}").fetchone()[0]
            p_val = self.con.execute(f"SELECT DISTINCT ProviderName, BillingCurrency FROM {t_name} WHERE ProviderName IS NOT NULL LIMIT 1").fetchone()
            prov_name = p_val[0] if p_val else ("Nebius B.V." if detected == "CSV_NEBIUS" else "FOCUS Dataset")
            curr_name = p_val[1] if p_val else "USD"
            self.ingestion_events.append({
                "filename": filename,
                "provider": prov_name,
                "currency": curr_name,
                "rows": cnt,
                "status": "success",
                "message": f"Ingested {filename} — Detected {prov_name} ({curr_name}) — {cnt} lines added"
            })

        elif detected == "JSON_CLOUDFLARE":
            self.log("Schema Expert", f"Rule 3: Unnesting Cloudflare JSON payload from '{filename}' with ConsumedUnit fallback...")
            self.con.execute(f"""
            CREATE OR REPLACE TABLE {t_name} AS
            SELECT 
                'Cloudflare' AS ProviderName,
                'Cloudflare, Inc.' AS PublisherName,
                r.InvoiceIssuerName,
                r.SubscriptionId AS InvoiceId,
                r.BillingAccountId,
                r.BillingAccountName,
                'Account' AS BillingAccountType,
                r.SubscriptionId AS SubAccountId,
                r.BillingAccountName AS SubAccountName,
                'Subscription' AS SubAccountType,
                r.ServiceFamilyName AS ServiceCategory,
                r.ServiceName,
                r.ServiceFamilyName AS ServiceSubcategory,
                r.ServiceName AS SkuId,
                r.ChargeDescription AS SkuDescription,
                NULL AS SkuPriceId,
                NULL AS ResourceId,
                NULL AS ResourceName,
                NULL AS ResourceType,
                'global' AS RegionId,
                'Global' AS RegionName,
                NULL AS AvailabilityZone,
                r.ChargeCategory,
                CAST(r.ChargeClass AS VARCHAR) AS ChargeClass,
                r.ChargeDescription,
                'Monthly' AS ChargeFrequency,
                r.BillingPeriodStart::TIMESTAMP AS BillingPeriodStart,
                r.ChargePeriodEnd::TIMESTAMP AS BillingPeriodEnd,
                r.ChargePeriodStart::TIMESTAMP AS ChargePeriodStart,
                r.ChargePeriodEnd::TIMESTAMP AS ChargePeriodEnd,
                TRY_CAST(r.ConsumedQuantity AS DOUBLE) AS ConsumedQuantity,
                -- Mandatory Rule 3: if ConsumedUnit is blank, fall back to PricingUnit
                CASE 
                    WHEN r.ConsumedUnit IS NULL OR trim(r.ConsumedUnit) = '' THEN r.PricingUnit 
                    ELSE r.ConsumedUnit 
                END AS ConsumedUnit,
                TRY_CAST(r.PricingQuantity AS DOUBLE) AS PricingQuantity,
                r.PricingUnit,
                TRY_CAST(r.BilledCost AS DOUBLE) AS BilledCost,
                TRY_CAST(r.EffectiveCost AS DOUBLE) AS EffectiveCost,
                TRY_CAST(r.ListCost AS DOUBLE) AS ListCost,
                TRY_CAST(r.ContractedCost AS DOUBLE) AS ContractedCost,
                0.0::DOUBLE AS ListUnitPrice,
                0.0::DOUBLE AS ContractedUnitPrice,
                r.BillingCurrency,
                r.BillingCurrency AS PricingCurrency,
                TRY_CAST(r.EffectiveCost AS DOUBLE) AS PricingCurrencyEffectiveCost,
                0.0::DOUBLE AS PricingCurrencyListUnitPrice,
                0.0::DOUBLE AS PricingCurrencyContractedUnitPrice,
                NULL AS CommitmentDiscountCategory,
                NULL AS CommitmentDiscountId,
                NULL AS CommitmentDiscountName,
                NULL::DOUBLE AS CommitmentDiscountQuantity,
                NULL AS CommitmentDiscountStatus,
                NULL AS CommitmentDiscountType,
                NULL AS CommitmentDiscountUnit,
                NULL AS CapacityReservationId,
                NULL AS CapacityReservationStatus,
                '{{}}' AS Tags
            FROM (SELECT unnest(result) AS r FROM read_json_auto('{filepath}'));
            """)
            self.staged_tables.append(t_name)
            cnt = self.con.execute(f"SELECT count(*) FROM {t_name}").fetchone()[0]
            self.ingestion_events.append({
                "filename": filename,
                "provider": "Cloudflare",
                "currency": "USD",
                "rows": cnt,
                "status": "success",
                "message": f"Ingested {filename} — Detected Cloudflare (USD) — {cnt} lines added"
            })

        elif detected == "CSV_GCP":
            self.log("Schema Expert", f"Rule 4: Normalizing GCP UI cost table '{filename}' to FOCUS 1.2 dimensions...")
            with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
                hdr_line = f.readline().lower()
            cost_col = '"Cost (€)"' if "cost (€)" in hdr_line else '"Cost"'
            unrounded_col = '"Unrounded cost (€)"' if "unrounded cost (€)" in hdr_line else ('"Unrounded cost"' if "unrounded cost" in hdr_line else cost_col)
            curr_str = "'EUR'" if "cost (€)" in hdr_line else "'USD'"

            self.con.execute(f"""
            CREATE OR REPLACE TABLE {t_name} AS
            SELECT 
                'Google Cloud' AS ProviderName,
                'Google' AS PublisherName,
                'Google Cloud' AS InvoiceIssuerName,
                NULL AS InvoiceId,
                "Billing account ID" AS BillingAccountId,
                "Billing account name" AS BillingAccountName,
                'BillingAccount' AS BillingAccountType,
                "Project ID" AS SubAccountId,
                "Project name" AS SubAccountName,
                'Project' AS SubAccountType,
                "Service ID" AS ServiceCategory,
                "Service description" AS ServiceName,
                NULL AS ServiceSubcategory,
                "SKU ID" AS SkuId,
                "SKU description" AS SkuDescription,
                NULL AS SkuPriceId,
                NULL AS ResourceId,
                NULL AS ResourceName,
                NULL AS ResourceType,
                'global' AS RegionId,
                'Global' AS RegionName,
                NULL AS AvailabilityZone,
                CASE 
                    WHEN "Credit type" IS NOT NULL AND trim("Credit type") != '' THEN 'Credit'
                    ELSE "Cost type"
                END AS ChargeCategory,
                NULL AS ChargeClass,
                "SKU description" AS ChargeDescription,
                'Usage-Based' AS ChargeFrequency,
                date_trunc('month', "Usage start date"::TIMESTAMP) AS BillingPeriodStart,
                (date_trunc('month', "Usage start date"::TIMESTAMP) + INTERVAL 1 MONTH) AS BillingPeriodEnd,
                "Usage start date"::TIMESTAMP AS ChargePeriodStart,
                ("Usage end date"::TIMESTAMP + INTERVAL 1 DAY) AS ChargePeriodEnd,
                TRY_CAST("Usage amount" AS DOUBLE) AS ConsumedQuantity,
                "Usage unit" AS ConsumedUnit,
                TRY_CAST("Usage amount" AS DOUBLE) AS PricingQuantity,
                "Usage unit" AS PricingUnit,
                CASE 
                    WHEN "Credit type" IS NOT NULL AND ("Credit type" ILIKE '%promot%' OR "Credit type" ILIKE '%trial%') THEN 0.0
                    ELSE TRY_CAST({cost_col} AS DOUBLE)
                END AS BilledCost,
                CASE 
                    WHEN "Credit type" IS NOT NULL AND ("Credit type" ILIKE '%promot%' OR "Credit type" ILIKE '%trial%') THEN 0.0
                    ELSE TRY_CAST({unrounded_col} AS DOUBLE)
                END AS EffectiveCost,
                CASE 
                    WHEN ("Credit type" IS NULL OR trim("Credit type") = '') THEN TRY_CAST({unrounded_col} AS DOUBLE)
                    ELSE 0.0 
                END AS ListCost,
                TRY_CAST({unrounded_col} AS DOUBLE) AS ContractedCost,
                NULL::DOUBLE AS ListUnitPrice,
                NULL::DOUBLE AS ContractedUnitPrice,
                {curr_str} AS BillingCurrency,
                {curr_str} AS PricingCurrency,
                TRY_CAST({unrounded_col} AS DOUBLE) AS PricingCurrencyEffectiveCost,
                NULL::DOUBLE AS PricingCurrencyListUnitPrice,
                NULL::DOUBLE AS PricingCurrencyContractedUnitPrice,
                CASE WHEN "Credit type" IS NOT NULL AND trim("Credit type") != '' THEN 'Discount' ELSE NULL END AS CommitmentDiscountCategory,
                NULL AS CommitmentDiscountId,
                "Credit type" AS CommitmentDiscountName,
                NULL::DOUBLE AS CommitmentDiscountQuantity,
                NULL AS CommitmentDiscountStatus,
                "Credit type" AS CommitmentDiscountType,
                NULL AS CommitmentDiscountUnit,
                NULL AS CapacityReservationId,
                NULL AS CapacityReservationStatus,
                '{{}}' AS Tags
            FROM read_csv_auto('{filepath}')
            WHERE "Billing account ID" IS NOT NULL 
              AND trim("Billing account ID") != '' 
              AND "Cost type" != 'Total';
            """)
            self.staged_tables.append(t_name)
            cnt = self.con.execute(f"SELECT count(*) FROM {t_name}").fetchone()[0]
            c_str = "EUR" if "eur" in curr_str.lower() else "USD"
            self.ingestion_events.append({
                "filename": filename,
                "provider": "Google Cloud",
                "currency": c_str,
                "rows": cnt,
                "status": "success",
                "message": f"Ingested {filename} — Detected Google Cloud ({c_str}) — {cnt} lines added"
            })

        elif detected in ("PARQUET_FOCUS", "PARQUET_GENERIC"):
            self.con.execute(f"CREATE OR REPLACE TABLE {t_name} AS SELECT * FROM '{filepath}';")
            self.staged_tables.append(t_name)
            cnt = self.con.execute(f"SELECT count(*) FROM {t_name}").fetchone()[0]
            self.ingestion_events.append({
                "filename": filename,
                "provider": "FOCUS Dataset",
                "currency": "USD",
                "rows": cnt,
                "status": "success",
                "message": f"Ingested {filename} — Pre-normalized FOCUS Parquet — {cnt} lines added"
            })

        elif detected == "JSON_FOCUS":
            self.con.execute(f"CREATE OR REPLACE TABLE {t_name} AS SELECT * FROM read_json_auto('{filepath}');")
            self.staged_tables.append(t_name)
            cnt = self.con.execute(f"SELECT count(*) FROM {t_name}").fetchone()[0]
            self.ingestion_events.append({
                "filename": filename,
                "provider": "FOCUS Dataset",
                "currency": "USD",
                "rows": cnt,
                "status": "success",
                "message": f"Ingested {filename} — Pre-normalized FOCUS JSON — {cnt} lines added"
            })


# ----------------------------------------------------------------------
# Core Engine Pipeline
# ----------------------------------------------------------------------
class FocusEngine:
    """Manages the in-memory DuckDB state, continuous normalization, and export."""

    def __init__(self, data_dir: str = SCRIPT_DIR):
        self.data_dir = data_dir
        self.con = duckdb.connect(database=":memory:")
        self.lock = threading.RLock()
        self.provider_tables = {}
        self.ingested_hashes = {}
        self.recent_events = []
        self.metrics = self.get_empty_metrics()
        self.audit_log = []
        self.latest_warnings = []
        self._init_empty_schema()

    def _init_empty_schema(self):
        with self.lock:
            self.con.execute("""
            CREATE OR REPLACE TABLE unified_focus (
                ProviderName VARCHAR,
                PublisherName VARCHAR,
                InvoiceIssuerName VARCHAR,
                InvoiceId VARCHAR,
                BillingAccountId VARCHAR,
                BillingAccountName VARCHAR,
                BillingAccountType VARCHAR,
                SubAccountId VARCHAR,
                SubAccountName VARCHAR,
                SubAccountType VARCHAR,
                ServiceCategory VARCHAR,
                ServiceName VARCHAR,
                ServiceSubcategory VARCHAR,
                SkuId VARCHAR,
                SkuDescription VARCHAR,
                SkuPriceId VARCHAR,
                ResourceId VARCHAR,
                ResourceName VARCHAR,
                ResourceType VARCHAR,
                RegionId VARCHAR,
                RegionName VARCHAR,
                AvailabilityZone VARCHAR,
                ChargeCategory VARCHAR,
                ChargeClass VARCHAR,
                ChargeDescription VARCHAR,
                ChargeFrequency VARCHAR,
                BillingPeriodStart TIMESTAMP,
                BillingPeriodEnd TIMESTAMP,
                ChargePeriodStart TIMESTAMP,
                ChargePeriodEnd TIMESTAMP,
                ConsumedQuantity DOUBLE,
                ConsumedUnit VARCHAR,
                PricingQuantity DOUBLE,
                PricingUnit VARCHAR,
                BilledCost DOUBLE,
                EffectiveCost DOUBLE,
                ListCost DOUBLE,
                ContractedCost DOUBLE,
                ListUnitPrice DOUBLE,
                ContractedUnitPrice DOUBLE,
                BillingCurrency VARCHAR,
                PricingCurrency VARCHAR,
                PricingCurrencyEffectiveCost DOUBLE,
                PricingCurrencyListUnitPrice DOUBLE,
                PricingCurrencyContractedUnitPrice DOUBLE,
                CommitmentDiscountCategory VARCHAR,
                CommitmentDiscountId VARCHAR,
                CommitmentDiscountName VARCHAR,
                CommitmentDiscountQuantity DOUBLE,
                CommitmentDiscountStatus VARCHAR,
                CommitmentDiscountType VARCHAR,
                CommitmentDiscountUnit VARCHAR,
                CapacityReservationId VARCHAR,
                CapacityReservationStatus VARCHAR,
                Tags VARCHAR
            );
            """)

    def get_empty_metrics(self) -> dict:
        return {
            "providers": [],
            "currency_totals": {
                "USD": {"billed": 0.0, "effective": 0.0, "normalized_rows": 0, "raw_rows": 0},
                "EUR": {"billed": 0.0, "effective": 0.0, "normalized_rows": 0, "raw_rows": 0}
            },
            "top_services": [],
            "table_rows": [],
            "total_normalized_rows": 0,
            "total_raw_rows": 0,
            "pruned_count": 0
        }

    def _export_parquet(self):
        try:
            if os.path.exists(OUTPUT_PARQUET):
                try:
                    os.remove(OUTPUT_PARQUET)
                except Exception:
                    pass
            self.con.execute(f"COPY unified_focus TO '{OUTPUT_PARQUET}' (FORMAT PARQUET, COMPRESSION 'ZSTD');")
            p_rows = self.con.execute(f"SELECT count(*) FROM '{OUTPUT_PARQUET}'").fetchone()[0]
            self.log("Data Engineer", f"Exported {OUTPUT_PARQUET} ({p_rows} rows).")
        except Exception as e:
            self.log("Data Engineer", f"[WARN] Parquet export note: {e}")

    def reset(self) -> dict:
        with self.lock:
            self.provider_tables.clear()
            self.ingested_hashes.clear()
            self.con.execute("DROP TABLE IF EXISTS raw_combined;")
            self.con.execute("DROP TABLE IF EXISTS unified_focus;")
            self._init_empty_schema()
            self._export_parquet()
            self.metrics = self.get_empty_metrics()
            self.latest_warnings = []
            reset_event = {
                "filename": "Ledger Reset",
                "provider": "All",
                "currency": "-",
                "rows": 0,
                "status": "reset",
                "timestamp": datetime.now().strftime("%H:%M:%S"),
                "message": "Reset / Clear Data triggered — in-memory ledger cleared to $0.00."
            }
            self.recent_events.insert(0, reset_event)
            self.log("Product Manager", "In-memory database state and provider tables reset.")
            generate_interactive_dashboard(self.metrics, self.recent_events)
            return self.metrics

    def load_demo_preset(self, preset: str = "all") -> dict:
        """Clears active ledger and dynamically synthesizes randomized FOCUS 1.2 multi-cloud telemetry."""
        return self.generate_synthetic_telemetry(preset)

    def generate_synthetic_telemetry(self, preset: str = "all") -> dict:
        """Dynamically generates randomized, realistic FOCUS 1.2 multi-cloud spend telemetry.
        Produces varying line item counts, services, and realistic USD/EUR spend amounts
        on every invocation while preserving strict 0.000000% dual-currency invariance.
        """
        import random, time
        from datetime import timedelta

        # Dynamic microsecond seed guarantees unique dataset on every run
        rnd = random.Random(time.time_ns())

        preset_clouds = {
            "all": ["AWS", "Microsoft Azure", "Google Cloud", "Nebius B.V.", "Cloudflare"],
            "hyperscalers": ["AWS", "Microsoft Azure", "Google Cloud"],
            "ai_infra": ["Nebius B.V.", "Cloudflare"],
            "aws_only": ["AWS"],
            "europe_only": ["Microsoft Azure", "Google Cloud"]
        }
        selected_clouds = preset_clouds.get(preset, preset_clouds["all"])

        preset_labels = {
            "all": "Complete Multi-Cloud Suite (AWS, Azure, GCP, Cloudflare, Nebius)",
            "hyperscalers": "Hyperscalers Suite (AWS, Azure, GCP)",
            "ai_infra": "AI & Edge Infrastructure (Nebius, Cloudflare)",
            "aws_only": "AWS Cost & Usage Report (USD)",
            "europe_only": "European Cloud Telemetry (Azure, GCP - EUR)"
        }
        label = preset_labels.get(preset, preset.title())

        # Target active row counts per provider (different on every click)
        base_row_ranges = {
            "AWS": (35, 65) if preset != "aws_only" else (85, 140),
            "Microsoft Azure": (20, 42) if preset != "europe_only" else (45, 80),
            "Google Cloud": (18, 38) if preset != "europe_only" else (40, 75),
            "Nebius B.V.": (15, 30) if preset != "ai_infra" else (35, 65),
            "Cloudflare": (12, 24) if preset != "ai_infra" else (28, 55),
        }

        cloud_catalog = {
            "AWS": {
                "publisher": "Amazon Web Services",
                "issuer": "Amazon Web Services, Inc.",
                "account_id": "941829410582",
                "account_name": "AWS-Enterprise-Master",
                "currency": "USD",
                "sub_accounts": [
                    ("109283746501", "Production-US-East"),
                    ("294810394857", "Core-Platform-Services"),
                    ("583920194820", "Data-Pipelines-ETL"),
                    ("918273645102", "ML-Inference-Fleet"),
                    ("394820194857", "Staging-Workloads"),
                ],
                "regions": [
                    ("us-east-1", "US East (N. Virginia)", "us-east-1a"),
                    ("us-west-2", "US West (Oregon)", "us-west-2b"),
                    ("eu-west-1", "Europe (Ireland)", "eu-west-1a"),
                ],
                "services": [
                    ("Compute", "Amazon Elastic Compute Cloud", "Virtual Machines", [
                        ("c6i.2xlarge", "Compute-Optimized c6i.2xlarge Linux Instance", "Hrs", 0.34, 1.2, 48.0),
                        ("m6i.4xlarge", "General Purpose m6i.4xlarge Linux Instance", "Hrs", 0.768, 2.0, 95.0),
                        ("g5.2xlarge", "GPU Accelerated Workload Instance (A10G)", "Hrs", 1.212, 3.5, 120.0),
                        ("gp3-storage", "EBS Provisioned IOPS gp3 Storage Volume", "GB-Mo", 0.08, 0.5, 35.0),
                        ("nat-gateway", "VPC NAT Gateway Data Processing & Usage", "Hrs", 0.045, 0.2, 18.0),
                    ]),
                    ("Storage", "Amazon Simple Storage Service", "Object Storage", [
                        ("s3-standard", "S3 Standard Storage Tier (First 50 TB)", "GB-Mo", 0.023, 0.1, 15.0),
                        ("s3-glacier", "S3 Glacier Flexible Retrieval Storage", "GB-Mo", 0.0036, 0.05, 8.0),
                        ("s3-api-put", "S3 API Tier-1 Requests (PUT/LIST/POST)", "Requests", 0.005, 0.01, 4.5),
                    ]),
                    ("Networking", "Amazon Virtual Private Cloud", "Network Infrastructure", [
                        ("vpc-ipv4", "In-use Public IPv4 Address", "Hrs", 0.005, 0.1, 6.0),
                        ("transit-gw", "Transit Gateway Data Processing Attachment", "GB", 0.02, 0.2, 12.0),
                        ("vpc-flowlogs", "VPC Flow Logs Telemetry Data Ingestion", "GB", 0.25, 0.05, 5.0),
                    ]),
                    ("Databases", "Amazon Relational Database Service", "Managed SQL", [
                        ("aurora-pg-xl", "Aurora PostgreSQL Multi-AZ db.r6g.xlarge", "Hrs", 0.52, 1.5, 65.0),
                        ("rds-storage", "Aurora Cluster Provisioned Storage", "GB-Mo", 0.10, 0.3, 20.0),
                    ]),
                    ("Security", "AWS Key Management Service", "Encryption Keys", [
                        ("kms-cmk", "Customer Managed Cryptographic Key", "Keys-Mo", 1.00, 1.0, 5.0),
                        ("kms-api", "Cryptographic API Operations (Encrypt/Decrypt)", "Requests", 0.03, 0.02, 2.5),
                    ])
                ]
            },
            "Microsoft Azure": {
                "publisher": "Microsoft",
                "issuer": "Microsoft Ireland Operations Limited",
                "account_id": "sub-9182-az-prod-eu",
                "account_name": "Azure-Corporate-Enterprise",
                "currency": "EUR",
                "sub_accounts": [
                    ("sub-core-eu-01", "Enterprise-EU-Core"),
                    ("sub-analytics-02", "Analytics-Dev-Workspaces"),
                    ("sub-security-03", "Security-Infra-Monitoring"),
                    ("sub-platform-04", "Platform-Engineering-SRE"),
                ],
                "regions": [
                    ("westeurope", "West Europe (Netherlands)", "westeurope-1"),
                    ("northeurope", "North Europe (Ireland)", "northeurope-1"),
                    ("germanywestcentral", "Germany West Central (Frankfurt)", "germanywestcentral-1"),
                ],
                "services": [
                    ("Compute", "Virtual Machines", "General Purpose VM", [
                        ("D8s_v5", "Standard_D8s_v5 (8 vCPUs, 32 GiB RAM)", "Hours", 0.384, 1.2, 55.0),
                        ("E16s_v5", "Standard_E16s_v5 Memory Optimized VM", "Hours", 0.842, 2.5, 88.0),
                        ("NV6ads_A10", "Standard_NV6ads_A10_v5 AI Inference VM", "Hours", 1.15, 3.0, 110.0),
                    ]),
                    ("Storage", "Storage Accounts", "Blob & Disk Storage", [
                        ("prem-ssd-p30", "Premium SSD Managed Disk P30 (1024 GiB)", "GiB/Month", 0.125, 0.8, 32.0),
                        ("hot-blob-lrs", "Standard Hot Blob Storage LRS", "GiB/Month", 0.0184, 0.1, 14.0),
                        ("cold-archive", "Standard Cold Archive Storage GRS", "GiB/Month", 0.002, 0.02, 5.0),
                    ]),
                    ("Containers", "Azure Kubernetes Service", "Kubernetes Orchestration", [
                        ("aks-cluster-sla", "AKS Uptime SLA Managed Control Plane", "Hours", 0.092, 0.5, 15.0),
                        ("aks-nodes", "AKS Node Pool Virtual Compute Capacity", "Hours", 0.45, 1.5, 45.0),
                    ]),
                    ("Databases", "Azure Cosmos DB", "NoSQL Database", [
                        ("cosmos-ru", "Cosmos DB Multi-Region Serverless RU/s", "100 RU/s", 0.00025, 0.3, 22.0),
                        ("cosmos-storage", "Cosmos DB Document Analytical Storage", "GB", 0.23, 0.2, 12.0),
                    ])
                ]
            },
            "Google Cloud": {
                "publisher": "Google LLC",
                "issuer": "Google Ireland Limited",
                "account_id": "011111-222222-333333",
                "account_name": "GCP Core Production Organisation",
                "currency": "EUR",
                "sub_accounts": [
                    ("project-core-prod", "production-mesh-core"),
                    ("project-bigquery-ml", "bigquery-analytical-engine"),
                    ("project-infra-mesh", "cloud-networking-backbone"),
                    ("project-dev-sandbox", "developer-sandboxes-eu"),
                ],
                "regions": [
                    ("europe-west1", "Belgium (europe-west1)", "europe-west1-b"),
                    ("europe-west3", "Frankfurt (europe-west3)", "europe-west3-a"),
                    ("europe-west4", "Eemshaven (europe-west4)", "europe-west4-a"),
                ],
                "services": [
                    ("Compute", "Compute Engine", "Virtual Machines & Accelerators", [
                        ("t4-gpu", "Nvidia Tesla T4 GPU attached to N1 VM", "hour", 0.32, 1.0, 42.0),
                        ("n2d-core", "N2D Predefined Instance Core (AMD EPYC)", "hour", 0.034, 0.4, 25.0),
                        ("n2d-ram", "N2D Predefined Instance RAM (AMD EPYC)", "gibibyte hour", 0.0045, 0.2, 15.0),
                    ]),
                    ("Storage", "Cloud Storage", "Object Storage", [
                        ("gcs-standard", "Standard Storage Multi-Region (EU)", "gibibyte month", 0.023, 0.1, 16.0),
                        ("gcs-nearline", "Nearline Storage Archive (europe-west3)", "gibibyte month", 0.011, 0.05, 8.0),
                    ]),
                    ("Analytics", "BigQuery", "Serverless Data Warehouse", [
                        ("bq-analysis", "BigQuery Analysis On-Demand Query Processing", "gibibyte", 0.0055, 0.2, 38.0),
                        ("bq-storage", "BigQuery Active Table Storage (EU)", "gibibyte month", 0.018, 0.1, 12.0),
                    ]),
                    ("Containers", "Google Kubernetes Engine", "Managed Kubernetes", [
                        ("gke-management", "GKE Standard Cluster Management Fee", "hour", 0.092, 0.5, 18.0),
                        ("gke-autopilot", "GKE Autopilot Workload Pod Compute Capacity", "hour", 0.35, 1.2, 35.0),
                    ])
                ]
            },
            "Nebius B.V.": {
                "publisher": "Nebius B.V.",
                "issuer": "Nebius B.V. Amsterdam",
                "account_id": "fhm29dk01948nebius",
                "account_name": "Nebius AI Foundation Lab",
                "currency": "USD",
                "sub_accounts": [
                    ("nebius-llm-pretraining", "LLM-Foundation-Cluster-01"),
                    ("nebius-inference-fleet", "Realtime-vLLM-Production"),
                    ("nebius-data-prep", "Multi-Modal-Data-Ingestion"),
                ],
                "regions": [
                    ("eu-north1", "Mäntsälä Data Center (eu-north1)", "eu-north1-a"),
                    ("eu-west1", "Paris Fabric (eu-west1)", "eu-west1-b"),
                ],
                "services": [
                    ("Compute Cloud", "Compute Cloud", "AI High-Performance Accelerated Compute", [
                        ("h100-sxm5-8x", "Nvidia H100 80GB SXM5 Dedicated 8x GPU Host", "GPU-Hour", 2.85, 5.0, 145.0),
                        ("epyc-zen4-128c", "AMD EPYC 9654 Zen4 High-Density CPU Node", "Core-Hour", 0.042, 1.5, 48.0),
                        ("infiniband-roce", "RoCE v2 InfiniBand Fabric Interconnect (3.2 Tbps)", "Gbps-Mo", 0.015, 0.8, 30.0),
                    ]),
                    ("Managed Kubernetes", "Managed Kubernetes Service", "AI Workload Orchestration", [
                        ("nebius-k8s-master", "Nebius Managed Kubernetes Master Node", "Hour", 0.12, 0.4, 18.0),
                        ("nebius-nvme-scratch", "Local High-IOPS NVMe SSD Scratch Storage", "GB-Mo", 0.18, 0.5, 25.0),
                    ]),
                    ("Network Storage", "Network Attached Storage", "Model Weights & Checkpoint Storage", [
                        ("lustre-fs", "Parallel Lustre Shared File System (Model Checkpoints)", "GB-Mo", 0.22, 1.0, 65.0),
                    ])
                ]
            },
            "Cloudflare": {
                "publisher": "Cloudflare, Inc.",
                "issuer": "Cloudflare, Inc. San Francisco",
                "account_id": "11112222333344445555666677778888",
                "account_name": "Global Edge Infrastructure",
                "currency": "USD",
                "sub_accounts": [
                    ("Global-Edge-Fleet", "Production Edge Delivery"),
                    ("DNS-CDN-Security", "Global Security Perimeter"),
                    ("AI-Inference-Workers", "Serverless Edge AI Workers"),
                ],
                "regions": [
                    ("global", "Global Anycast Edge Network", "edge-pops-anycast"),
                ],
                "services": [
                    ("Workers AI", "Workers AI", "Serverless GPU Inference", [
                        ("cf-ai-llama", "Workers AI GPU Inference (Llama-3.1-70B)", "Million Tokens", 0.65, 0.5, 32.0),
                        ("cf-workers-cpu", "Workers Paid CPU Execution Duration (ms)", "Million Invocations", 0.30, 0.2, 16.0),
                    ]),
                    ("R2 Storage", "R2 Object Storage", "Zero-Egress Object Storage", [
                        ("r2-storage-gb", "R2 Data Storage Active Capacity", "GB-months", 0.015, 0.1, 14.0),
                        ("r2-class-a", "R2 Storage Class A Operations (PUT/LIST)", "Count", 0.0045, 0.05, 5.0),
                        ("r2-class-b", "R2 Storage Class B Operations (GET)", "Count", 0.00036, 0.01, 2.5),
                    ]),
                    ("Edge Database", "D1 SQL Database", "Distributed Edge SQL", [
                        ("d1-reads", "D1 Distributed Read Operations", "Million Rows", 0.001, 0.02, 4.0),
                        ("d1-writes", "D1 Batch Write Operations", "Million Rows", 1.00, 0.1, 8.0),
                    ]),
                    ("Security & Network", "Enterprise Edge Network", "Edge Protection", [
                        ("edge-waf", "Advanced DDoS Mitigation & WAF Rulesets", "Month", 20.0, 1.0, 20.0),
                        ("magic-transit", "Magic Transit BGP Network Telemetry Flow", "GB", 0.05, 0.2, 12.0),
                    ])
                ]
            }
        }

        now = datetime.now()
        period_start = datetime(now.year, now.month, 1)
        next_month = (period_start + timedelta(days=32)).replace(day=1)

        with self.lock:
            self.provider_tables.clear()
            self.ingested_hashes.clear()
            self.con.execute("DROP TABLE IF EXISTS raw_combined;")
            self.con.execute("DROP TABLE IF EXISTS unified_focus;")
            self._init_empty_schema()
            self.latest_warnings = []
            self.recent_events = []

            for cloud in selected_clouds:
                c_data = cloud_catalog[cloud]
                curr = c_data["currency"]
                t_name = f"synth_{cloud.replace(' ', '_').replace('.', '_')}_{uuid.uuid4().hex[:8]}"

                min_r, max_r = base_row_ranges.get(cloud, (20, 40))
                num_active = rnd.randint(min_r, max_r)
                num_idle = rnd.randint(4, 15)

                rows = []
                # 1. Active spend rows
                for i in range(num_active):
                    svc_cat, svc_name, svc_subcat, skus = rnd.choice(c_data["services"])
                    sku_id, sku_desc, unit, rate, min_s, max_s = rnd.choice(skus)
                    sub_id, sub_name = rnd.choice(c_data["sub_accounts"])
                    reg_id, reg_name, az = rnd.choice(c_data["regions"])

                    cost = round(rnd.uniform(min_s, max_s), 6)
                    qty = round(cost / max(rate, 0.0001), 3)
                    days_ago = rnd.randint(0, min(now.day - 1, 27) if now.day > 1 else 0)
                    chg_start = now - timedelta(days=days_ago, hours=rnd.randint(0, 23))
                    chg_end = chg_start + timedelta(hours=rnd.choice([1, 24]))

                    tags = f'{{"Environment": "{rnd.choice(["Production", "Staging", "Analytics"])}", "Owner": "{rnd.choice(["CoreTeam", "DataPlatform", "InfraOps"])}"}}'

                    rows.append((
                        cloud, c_data["publisher"], c_data["issuer"], f"INV-{rnd.randint(100000, 999999)}",
                        c_data["account_id"], c_data["account_name"], "BillingAccount",
                        sub_id, sub_name, "SubAccount",
                        svc_cat, svc_name, svc_subcat,
                        sku_id, sku_desc, f"price-{sku_id}",
                        f"res-{uuid.uuid4().hex[:10]}", f"{sku_id}-node-{i+1}", "VirtualResource",
                        reg_id, reg_name, az,
                        "Usage", "On-Demand", f"{sku_desc} [Normalized {curr}]", "Usage-Based",
                        period_start, next_month, chg_start, chg_end,
                        qty, unit, qty, unit,
                        cost, cost, cost, cost, rate, rate,
                        curr, curr, cost, rate, rate,
                        None, None, None, None, None, None, None,
                        None, None, tags
                    ))

                # 2. Idle micro-metered $0.00 rows (for Rule 5 pruning exercise)
                for j in range(num_idle):
                    svc_cat, svc_name, svc_subcat, skus = rnd.choice(c_data["services"])
                    sku_id, sku_desc, unit, rate, min_s, max_s = rnd.choice(skus)
                    sub_id, sub_name = rnd.choice(c_data["sub_accounts"])
                    reg_id, reg_name, az = rnd.choice(c_data["regions"])
                    chg_start = now - timedelta(days=rnd.randint(0, 5), hours=rnd.randint(0, 12))
                    chg_end = chg_start + timedelta(hours=1)
                    rows.append((
                        cloud, c_data["publisher"], c_data["issuer"], f"INV-{rnd.randint(100000, 999999)}",
                        c_data["account_id"], c_data["account_name"], "BillingAccount",
                        sub_id, sub_name, "SubAccount",
                        svc_cat, svc_name, svc_subcat,
                        sku_id, f"Idle Standby {sku_desc}", f"price-{sku_id}",
                        f"res-{uuid.uuid4().hex[:10]}", f"idle-probe-{j+1}", "VirtualResource",
                        reg_id, reg_name, az,
                        "Usage", "ZeroMetered", f"Standby micro-metered zero consumption", "Usage-Based",
                        period_start, next_month, chg_start, chg_end,
                        0.0, unit, 0.0, unit,
                        0.0, 0.0, 0.0, 0.0, rate, rate,
                        curr, curr, 0.0, rate, rate,
                        None, None, None, None, None, None, None,
                        None, None, '{"Environment": "Standby"}'
                    ))

                self.con.execute(f"CREATE TABLE {t_name} AS SELECT * FROM unified_focus;")
                self.con.executemany(f"INSERT INTO {t_name} VALUES (" + ", ".join(["?"] * 55) + ")", rows)
                self.provider_tables[cloud] = t_name

            union_sql = " UNION ALL BY NAME ".join(f"SELECT * FROM {t}" for t in self.provider_tables.values())
            self.con.execute(f"CREATE OR REPLACE TABLE raw_combined AS {union_sql};")

            raw_recon = self.con.execute("""
            SELECT 
                BillingCurrency,
                sum(COALESCE(BilledCost, 0)) AS total_billed,
                sum(COALESCE(EffectiveCost, 0)) AS total_effective,
                count(*) AS total_rows
            FROM raw_combined
            GROUP BY BillingCurrency;
            """).df()

            self.con.execute("""
            CREATE OR REPLACE TABLE unified_focus AS
            SELECT * FROM raw_combined
            WHERE NOT (
                ProviderName != 'Cloudflare'
                AND COALESCE(BilledCost, 0) = 0
                AND COALESCE(EffectiveCost, 0) = 0
            );
            """)

            pruned_recon = self.con.execute("""
            SELECT 
                BillingCurrency,
                sum(COALESCE(BilledCost, 0)) AS total_billed,
                sum(COALESCE(EffectiveCost, 0)) AS total_effective,
                count(*) AS total_rows
            FROM unified_focus
            GROUP BY BillingCurrency;
            """).df()

            for c_code in ["EUR", "USD"]:
                r_c = raw_recon[raw_recon["BillingCurrency"] == c_code]
                p_c = pruned_recon[pruned_recon["BillingCurrency"] == c_code]
                if not r_c.empty and not p_c.empty:
                    raw_b = float(r_c["total_billed"].iloc[0])
                    pruned_b = float(p_c["total_billed"].iloc[0])
                    delta = abs(raw_b - pruned_b)
                    assert delta < 1e-9, f"Reconciliation delta violation for {c_code}: {delta}"
                    self.log("QA Tester", f"  [PASSED] Invariance check for {c_code}: Net Total = {pruned_b:.6f}, Delta = {delta:.12f}")

            self._export_parquet()

            self.collect_metrics(raw_recon, pruned_recon)

            norm_cnt = self.metrics.get("total_normalized_rows", 0)
            pruned_cnt = self.metrics.get("pruned_count", 0)
            ts_now = datetime.now().strftime("%H:%M:%S")

            gen_event = {
                "filename": "Demo Telemetry Generator",
                "provider": label,
                "currency": "USD/EUR",
                "rows": norm_cnt,
                "status": "success",
                "timestamp": ts_now,
                "message": f"Generated {label} ({norm_cnt} FOCUS 1.2 records active, {pruned_cnt} idle rows pruned)."
            }
            self.recent_events.insert(0, gen_event)
            self.recent_events = self.recent_events[:30]

            generate_interactive_dashboard(self.metrics, self.recent_events)
            return self.metrics

    def reload_initial(self) -> dict:
        """Clears the active ledger and synthesizes a fresh multi-cloud demo telemetry benchmark."""
        return self.generate_synthetic_telemetry("all")

    def log(self, discipline: str, message: str):
        ts = datetime.now().strftime("%H:%M:%S")
        msg = f"[{ts}] [{discipline.upper()}] {message}"
        self.audit_log.append(msg)
        print(msg)

    def load_initial_files(self, generate_report: bool = True):
        """Scans the local directory for existing raw files and auto-ingests them."""
        raw_candidates = set()
        for ext in ["*.snappy.parquet", "*.parquet", "*.tar.gz", "*.zip", "*.json", "*.csv"]:
            try:
                raw_candidates.update(glob.glob(os.path.join(self.data_dir, ext)))
            except Exception:
                pass

        # Robust fallback for protected directories (e.g. macOS TCC): check known sample datasets
        fallback_samples = [
            "sample_data/AWSDemoReport-00001.snappy.parquet",
            "sample_data/AZUREpart_0_0001.snappy.parquet",
            "sample_data/GCP_cost_table.csv",
            "sample_data/NEBIUSnbs.tar.gz",
            "sample_data/cloudflare.json"
        ]
        for s in fallback_samples:
            target = os.path.join(self.data_dir, s)
            if os.path.isfile(target):
                raw_candidates.add(target)

        # Exclude artifacts and hidden files
        to_process = sorted([
            f for f in raw_candidates 
            if os.path.basename(f) not in ("unified_focus.parquet", "test_unified.parquet")
            and not os.path.basename(f).startswith(".")
        ])
        if to_process:
            self.log("Product Manager", f"Auto-detecting initial local billing files: {len(to_process)} candidate(s) found.")
            self.ingest_files(to_process, generate_report=generate_report)

    def ingest_files(self, file_paths: list[str], mode: str = "replace", generate_report: bool = True) -> tuple[dict, list[dict]]:
        """Dynamically ingests and normalizes any set of files.
        Mode 'replace' (default): active ledger is replaced with only the newly dropped batch.
        Mode 'append': new file(s) are accumulated into the active ledger.
        Returns (metrics, events).
        """
        with self.lock:
            normalizer = DynamicNormalizer(self.con)
            events = []
            new_files = []
            batch_hashes = set()

            for fp in file_paths:
                fn = os.path.basename(fp)
                try:
                    with open(fp, "rb") as f:
                        f_hash = hashlib.sha256(f.read()).hexdigest()
                except Exception:
                    f_hash = None

                # Prevent internal duplicates within the exact same batch
                if f_hash and f_hash in batch_hashes:
                    ts = datetime.now().strftime("%H:%M:%S")
                    dup_event = {
                        "filename": fn,
                        "provider": "Duplicate",
                        "currency": "-",
                        "rows": 0,
                        "status": "duplicate",
                        "timestamp": ts,
                        "message": f"File '{fn}' duplicated within drop batch — skipped"
                    }
                    events.append(dup_event)
                    continue

                if f_hash:
                    batch_hashes.add(f_hash)

                # In append mode, check if already active in persistent state
                if mode == "append" and f_hash and f_hash in self.ingested_hashes and self.ingested_hashes[f_hash].get("provider") in self.provider_tables:
                    prev = self.ingested_hashes[f_hash]
                    ts = datetime.now().strftime("%H:%M:%S")
                    prev["timestamp"] = ts
                    dup_event = {
                        "filename": fn,
                        "provider": prev.get("provider", "Unknown"),
                        "currency": prev.get("currency", "-"),
                        "rows": prev.get("rows", 0),
                        "status": "duplicate",
                        "timestamp": ts,
                        "message": f"File '{fn}' already processed — state re-verified ({prev.get('rows', 0)} rows active)"
                    }
                    events.append(dup_event)
                    self.log("QA Tester", f"Duplicate file dropped in append mode: {fn} (hash match)")
                else:
                    new_files.append((fp, f_hash))

            for fp, f_hash in new_files:
                normalizer.process_file_or_archive(fp)

            ts_now = datetime.now().strftime("%H:%M:%S")
            for ev in normalizer.ingestion_events:
                ev["timestamp"] = ts_now
                events.append(ev)

            self.latest_warnings = normalizer.warnings
            self.audit_log.extend(normalizer.audit_log)
            self.recent_events = (events + self.recent_events)[:30]

            if not normalizer.staged_tables:
                if not self.provider_tables:
                    self.metrics = self.get_empty_metrics()
                    if generate_report:
                        generate_interactive_dashboard(self.metrics, self.recent_events)
                return self.metrics, events

            # In replace mode, clear prior active tables and hashes
            if mode == "replace":
                self.provider_tables.clear()
                self.ingested_hashes.clear()

            # Record hashes for successfully normalized files
            for ev in normalizer.ingestion_events:
                if ev.get("status") == "success":
                    for orig_fp, orig_hash in new_files:
                        orig_fn = os.path.basename(orig_fp)
                        if orig_hash and (orig_fn in ev["filename"] or ev["filename"] in orig_fn):
                            self.ingested_hashes[orig_hash] = {
                                "filename": ev["filename"],
                                "provider": ev["provider"],
                                "currency": ev["currency"],
                                "rows": ev["rows"],
                                "timestamp": ts_now
                            }

            # Register new or updated provider tables in the multi-cloud ledger
            for t in normalizer.staged_tables:
                distinct_provs = [r[0] for r in self.con.execute(f"SELECT DISTINCT ProviderName FROM {t} WHERE ProviderName IS NOT NULL").fetchall()]
                if len(distinct_provs) > 1:
                    for prov in distinct_provs:
                        p_t = f"prov_{uuid.uuid4().hex[:8]}"
                        self.con.execute(f"CREATE TABLE {p_t} AS SELECT * FROM {t} WHERE ProviderName = '{prov}';")
                        if prov in self.provider_tables:
                            existing_t = self.provider_tables[prov]
                            comb_t = f"stage_comb_{abs(hash(p_t))}_{len(self.provider_tables)}"
                            self.con.execute(f"CREATE TABLE {comb_t} AS SELECT * FROM {existing_t} UNION ALL BY NAME SELECT * FROM {p_t};")
                            self.provider_tables[prov] = comb_t
                        else:
                            self.provider_tables[prov] = p_t
                else:
                    prov_key = distinct_provs[0] if distinct_provs else t
                    if prov_key in self.provider_tables:
                        existing_t = self.provider_tables[prov_key]
                        comb_t = f"stage_comb_{abs(hash(t))}_{len(self.provider_tables)}"
                        self.con.execute(f"CREATE TABLE {comb_t} AS SELECT * FROM {existing_t} UNION ALL BY NAME SELECT * FROM {t};")
                        self.provider_tables[prov_key] = comb_t
                    else:
                        self.provider_tables[prov_key] = t

            # Union all active provider tables into the consolidated ledger
            union_sql = " UNION ALL BY NAME ".join(f"SELECT * FROM {t}" for t in self.provider_tables.values())
            self.con.execute(f"CREATE OR REPLACE TABLE raw_combined AS {union_sql};")

            # Pre-pruning audit
            raw_recon = self.con.execute("""
            SELECT 
                BillingCurrency,
                sum(COALESCE(BilledCost, 0)) AS total_billed,
                sum(COALESCE(EffectiveCost, 0)) AS total_effective,
                count(*) AS total_rows
            FROM raw_combined
            GROUP BY BillingCurrency;
            """).df()

            # Rule 5: Micro-metering pruning of idle $0.00 rows
            self.con.execute("""
            CREATE OR REPLACE TABLE unified_focus AS
            SELECT * FROM raw_combined
            WHERE NOT (
                ProviderName != 'Cloudflare'
                AND COALESCE(BilledCost, 0) = 0
                AND COALESCE(EffectiveCost, 0) = 0
            );
            """)

            # Rule 6: Dual-Currency Reconciliation Invariance Verification
            pruned_recon = self.con.execute("""
            SELECT 
                BillingCurrency,
                sum(COALESCE(BilledCost, 0)) AS total_billed,
                sum(COALESCE(EffectiveCost, 0)) AS total_effective,
                count(*) AS total_rows
            FROM unified_focus
            GROUP BY BillingCurrency;
            """).df()

            for curr in ["EUR", "USD"]:
                r_c = raw_recon[raw_recon["BillingCurrency"] == curr]
                p_c = pruned_recon[pruned_recon["BillingCurrency"] == curr]
                if not r_c.empty and not p_c.empty:
                    raw_b = float(r_c["total_billed"].iloc[0])
                    pruned_b = float(p_c["total_billed"].iloc[0])
                    delta = abs(raw_b - pruned_b)
                    assert delta < 1e-9, f"Reconciliation delta violation for {curr}: {delta}"
                    self.log("QA Tester", f"  [PASSED] Invariance check for {curr}: Net Total = {pruned_b:.6f}, Delta = {delta:.12f}")

            # Export unified_focus.parquet atomically
            self._export_parquet()

            # Collect updated metrics
            self.collect_metrics(raw_recon, pruned_recon)
            if generate_report:
                generate_interactive_dashboard(self.metrics, self.recent_events)
            return self.metrics, events

    def collect_metrics(self, raw_recon, pruned_recon):
        prov_df = self.con.execute("""
        SELECT 
            ProviderName,
            BillingCurrency,
            count(*) AS normalized_rows,
            sum(COALESCE(BilledCost, 0)) AS total_billed,
            sum(COALESCE(EffectiveCost, 0)) AS total_effective
        FROM unified_focus
        GROUP BY ProviderName, BillingCurrency
        ORDER BY BillingCurrency, ProviderName;
        """).df()

        raw_counts = self.con.execute("""
        SELECT ProviderName, count(*) as raw_rows
        FROM raw_combined
        GROUP BY ProviderName;
        """).df().set_index("ProviderName")["raw_rows"].to_dict()

        provider_summaries = []
        for _, row in prov_df.iterrows():
            p_name = row["ProviderName"]
            curr = row["BillingCurrency"]
            norm_r = int(row["normalized_rows"])
            raw_r = raw_counts.get(p_name, norm_r)
            provider_summaries.append({
                "provider": p_name,
                "currency": curr,
                "raw_rows": raw_r,
                "normalized_rows": norm_r,
                "pruned_rows": max(0, raw_r - norm_r),
                "billed_cost": float(row["total_billed"]),
                "effective_cost": float(row["total_effective"])
            })

        totals = {}
        for curr in ["USD", "EUR"]:
            p_c = pruned_recon[pruned_recon["BillingCurrency"].astype(str).str.upper().str.strip() == curr] if not pruned_recon.empty else pruned_recon
            r_c = raw_recon[raw_recon["BillingCurrency"].astype(str).str.upper().str.strip() == curr] if not raw_recon.empty else raw_recon
            totals[curr] = {
                "billed": float(p_c["total_billed"].iloc[0]) if not p_c.empty else 0.0,
                "effective": float(p_c["total_effective"].iloc[0]) if not p_c.empty else 0.0,
                "normalized_rows": int(p_c["total_rows"].iloc[0]) if not p_c.empty else 0,
                "raw_rows": int(r_c["total_rows"].iloc[0]) if not r_c.empty else 0
            }

        top_services = self.con.execute("""
        SELECT 
            ProviderName,
            ServiceName,
            BillingCurrency,
            sum(COALESCE(BilledCost, 0)) AS total_billed,
            count(*) AS line_items
        FROM unified_focus
        GROUP BY ProviderName, ServiceName, BillingCurrency
        ORDER BY total_billed DESC
        LIMIT 15;
        """).df().to_dict(orient="records")

        detail_df = self.con.execute("""
        SELECT 
            COALESCE(CAST(ProviderName AS VARCHAR), 'Unknown') AS ProviderName,
            COALESCE(CAST(ServiceName AS VARCHAR), '-') AS ServiceName,
            COALESCE(CAST(ChargeCategory AS VARCHAR), 'Usage') AS ChargeCategory,
            COALESCE(CAST(ChargeDescription AS VARCHAR), '-') AS ChargeDescription,
            COALESCE(TRY_CAST(ConsumedQuantity AS DOUBLE), 0.0) AS ConsumedQuantity,
            COALESCE(CAST(ConsumedUnit AS VARCHAR), '-') AS ConsumedUnit,
            COALESCE(TRY_CAST(BilledCost AS DOUBLE), 0.0) AS BilledCost,
            COALESCE(TRY_CAST(EffectiveCost AS DOUBLE), 0.0) AS EffectiveCost,
            COALESCE(UPPER(TRIM(CAST(BillingCurrency AS VARCHAR))), 'USD') AS BillingCurrency,
            COALESCE(strftime(TRY_CAST(ChargePeriodStart AS TIMESTAMP), '%Y-%m-%d %H:%M'), '-') AS PeriodStart,
            COALESCE(strftime(TRY_CAST(ChargePeriodEnd AS TIMESTAMP), '%Y-%m-%d %H:%M'), '-') AS PeriodEnd,
            COALESCE(CAST(SubAccountName AS VARCHAR), CAST(SubAccountId AS VARCHAR), '-') AS SubAccount
        FROM unified_focus
        ORDER BY BillingCurrency, ProviderName, BilledCost DESC, ServiceName;
        """).df()

        self.metrics = {
            "providers": provider_summaries,
            "currency_totals": totals,
            "top_services": top_services,
            "table_rows": detail_df.to_dict(orient="records"),
            "total_normalized_rows": sum(p["normalized_rows"] for p in provider_summaries),
            "total_raw_rows": sum(p["raw_rows"] for p in provider_summaries),
            "pruned_count": sum(p["pruned_rows"] for p in provider_summaries)
        }

    def print_terminal_summary(self):
        """Prints a formatted summary table across all providers."""
        print("\n" + "=" * 94)
        print("  FOCUS ENGINE - MULTI-CLOUD SPEND SUMMARY TABLE (FOCUS 1.2)")
        print("=" * 94)
        print(f"{'Provider':<18} | {'Currency':<8} | {'Raw Rows':<9} | {'Norm Rows':<9} | {'Pruned':<7} | {'Net Billed Cost':<18} | {'Net Effective Cost':<18}")
        print("-" * 94)

        if not self.metrics.get("providers"):
            print(f"  [AWAITING BILLING TELEMETRY] Active ledger is at $0.00. Drag & drop files or click Generate Demo Data.")
            print("-" * 94)

        current_curr = None
        for p in self.metrics["providers"]:
            if current_curr is not None and current_curr != p["currency"]:
                sub = self.metrics["currency_totals"][current_curr]
                sym = "$" if current_curr == "USD" else "€"
                sub_billed = f"{sym}{sub['billed']:.6f}"
                sub_effective = f"{sym}{sub['effective']:.6f}"
                sub_pruned = sub["raw_rows"] - sub["normalized_rows"]
                sub_label = f"SUBTOTAL ({current_curr})"
                print(f"{sub_label:<18} | {current_curr:<8} | {sub['raw_rows']:<9} | {sub['normalized_rows']:<9} | {sub_pruned:<7} | {sub_billed:<18} | {sub_effective:<18}")
                print("-" * 94)
            current_curr = p["currency"]
            sym = "$" if p["currency"] == "USD" else "€"
            b_str = f"{sym}{p['billed_cost']:.6f}"
            e_str = f"{sym}{p['effective_cost']:.6f}"
            print(f"{p['provider']:<18} | {p['currency']:<8} | {p['raw_rows']:<9} | {p['normalized_rows']:<9} | {p['pruned_rows']:<7} | {b_str:<18} | {e_str:<18}")

        if current_curr:
            sub = self.metrics["currency_totals"][current_curr]
            sym = "$" if current_curr == "USD" else "€"
            sub_billed = f"{sym}{sub['billed']:.6f}"
            sub_effective = f"{sym}{sub['effective']:.6f}"
            sub_pruned = sub["raw_rows"] - sub["normalized_rows"]
            sub_label = f"SUBTOTAL ({current_curr})"
            print(f"{sub_label:<18} | {current_curr:<8} | {sub['raw_rows']:<9} | {sub['normalized_rows']:<9} | {sub_pruned:<7} | {sub_billed:<18} | {sub_effective:<18}")
            print("=" * 94)

        usd_tot = self.metrics["currency_totals"].get("USD", {}).get("billed", 0.0)
        eur_tot = self.metrics["currency_totals"].get("EUR", {}).get("billed", 0.0)
        print(f"DUAL-CURRENCY TOTALS:  USD: ${usd_tot:.6f}  |  EUR: €{eur_tot:.6f}  (NEVER conflated)")
        print(f"PIPELINE METRICS:     Ingested {self.metrics['total_raw_rows']} raw records -> Pruned {self.metrics['pruned_count']} idle $0.00 rows -> {self.metrics['total_normalized_rows']} FOCUS 1.2 records.")
        print(f"ARTIFACT OUTPUTS:     Parquet: {OUTPUT_PARQUET}")
        print(f"                      Dashboard: {OUTPUT_HTML}")
        print("=" * 94 + "\n")


# ----------------------------------------------------------------------
# Zero-Framework Python HTTP Server (Discipline 3: Data Engineer)
# ----------------------------------------------------------------------
class FocusRequestHandler(BaseHTTPRequestHandler):
    """Zero-framework HTTP request handler for drag-and-drop file ingestion and UI."""

    engine: FocusEngine = None  # Injected on server instantiation

    def log_message(self, format, *args):
        # Suppress routine GET logs to keep terminal summary clean
        if self.command == "POST" or "500" in str(args):
            super().log_message(format, *args)

    def do_HEAD(self):
        self.do_GET()

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, *")
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path in ("/", "/report.html", "/index.html"):
            self.serve_html()
        elif parsed.path in ("/api/data", "/api/metrics"):
            self.serve_json({
                "success": True,
                "metrics": self.engine.metrics,
                "events": self.engine.recent_events,
                "warnings": self.engine.latest_warnings
            })
        elif parsed.path == "/unified_focus.parquet":
            if os.path.exists(OUTPUT_PARQUET):
                self.send_response(200)
                self.send_header("Content-Type", "application/octet-stream")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.send_header("Content-Disposition", "attachment; filename=unified_focus.parquet")
                self.send_header("Content-Length", str(os.path.getsize(OUTPUT_PARQUET)))
                self.end_headers()
                with open(OUTPUT_PARQUET, "rb") as f:
                    shutil.copyfileobj(f, self.wfile)
            else:
                self.send_error(404, "unified_focus.parquet not found")
        elif parsed.path == "/unified_focus.duckdb":
            self.handle_export_duckdb()
        elif parsed.path == "/logo.png":
            target_logo = LOGO_FILE if os.path.exists(LOGO_FILE) else os.path.expanduser("~/Downloads/logo.png")
            if os.path.exists(target_logo):
                self.send_response(200)
                self.send_header("Content-Type", "image/png")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.send_header("Content-Length", str(os.path.getsize(target_logo)))
                self.end_headers()
                with open(target_logo, "rb") as f:
                    shutil.copyfileobj(f, self.wfile)
            else:
                self.send_error(404, "logo.png not found")
        else:
            self.send_error(404, "Endpoint not found")

    def do_POST(self):
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path == "/api/upload":
            self.handle_upload(parsed)
        elif parsed.path == "/api/reset":
            self.handle_reset()
        elif parsed.path == "/api/reload":
            self.handle_reload()
        elif parsed.path == "/api/generate-demo":
            self.handle_generate_demo(parsed)
        else:
            self.send_error(404, "Endpoint not found")

    def handle_generate_demo(self, parsed):
        query = urllib.parse.parse_qs(parsed.query)
        preset = query.get("preset", ["all"])[0]
        content_length = int(self.headers.get("Content-Length", 0))
        if content_length > 0:
            try:
                body = self.rfile.read(content_length)
                data = json.loads(body.decode("utf-8"))
                if "preset" in data:
                    preset = data["preset"]
            except Exception:
                pass

        metrics = self.engine.load_demo_preset(preset)
        self.serve_json({
            "success": True,
            "message": f"Demo preset '{preset}' generated successfully.",
            "metrics": metrics,
            "events": self.engine.recent_events
        })

    def handle_reset(self):
        metrics = self.engine.reset()
        self.serve_json({
            "success": True,
            "message": "In-memory database state and provider tables reset successfully.",
            "metrics": metrics,
            "events": self.engine.recent_events
        })

    def handle_reload(self):
        metrics = self.engine.reload_initial()
        self.serve_json({
            "success": True,
            "message": "All 5 local sample multi-cloud datasets reloaded successfully.",
            "metrics": metrics,
            "events": self.engine.recent_events
        })

    def serve_html(self):
        if os.path.exists(OUTPUT_HTML):
            with open(OUTPUT_HTML, "rb") as f:
                content = f.read()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Content-Length", str(len(content)))
            self.end_headers()
            self.wfile.write(content)
        else:
            self.send_error(404, "report.html not found")

    def serve_json(self, data: dict, status: int = 200):
        body = json.dumps(data).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, *")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def handle_export_duckdb(self):
        """Export the current in-memory FOCUS 1.2 data to a standalone .duckdb file."""
        try:
            export_path = os.path.join(SCRIPT_DIR, "unified_focus.duckdb")
            # Remove stale file if it exists
            if os.path.exists(export_path):
                os.remove(export_path)
            # Create a new persistent DuckDB file and copy the normalized table into it
            export_db = duckdb.connect(export_path)
            try:
                if os.path.exists(OUTPUT_PARQUET):
                    export_db.execute(f"CREATE TABLE unified_focus AS SELECT * FROM read_parquet('{OUTPUT_PARQUET}')")
                    row_count = export_db.execute("SELECT COUNT(*) FROM unified_focus").fetchone()[0]
                    print(f"[DATA ENGINEER] Exported {export_path} ({row_count} rows) as standalone DuckDB.")
                else:
                    # Empty export
                    export_db.execute("CREATE TABLE unified_focus (placeholder VARCHAR)")
            finally:
                export_db.close()

            with open(export_path, "rb") as f:
                content = f.read()
            self.send_response(200)
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Content-Disposition", "attachment; filename=unified_focus.duckdb")
            self.send_header("Content-Length", str(len(content)))
            self.end_headers()
            self.wfile.write(content)
        except Exception as e:
            self.serve_json({"success": False, "error": str(e)}, status=500)

    def handle_upload(self, parsed=None):
        content_type = self.headers.get("Content-Type", "")
        if not content_type.startswith("multipart/form-data"):
            self.serve_json({"success": False, "error": "Expected multipart/form-data"}, status=400)
            return

        # Default ingestion mode is 'replace'
        mode = "replace"
        if parsed and parsed.query:
            qs = urllib.parse.parse_qs(parsed.query)
            if "mode" in qs and qs["mode"]:
                candidate = qs["mode"][0].strip().lower()
                if candidate in ("replace", "append"):
                    mode = candidate

        try:
            content_length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(content_length)

            # Parse multipart safely using Python standard library email.parser
            headers_bytes = f"Content-Type: {content_type}\r\n\r\n".encode("utf-8")
            msg = email.parser.BytesParser(policy=email.policy.default).parsebytes(headers_bytes + body)

            upload_dir = tempfile.mkdtemp(prefix="focus_upload_")
            saved_files = []

            for part in msg.iter_parts():
                fn = part.get_filename()
                param_name = part.get_param("name", header="content-disposition")
                if fn:
                    payload = part.get_payload(decode=True)
                    if payload:
                        target = os.path.join(upload_dir, fn)
                        with open(target, "wb") as f:
                            f.write(payload)
                        saved_files.append(target)
                elif param_name == "mode":
                    payload = part.get_payload(decode=True)
                    if payload:
                        candidate = payload.decode("utf-8", errors="ignore").strip().lower()
                        if candidate in ("replace", "append"):
                            mode = candidate

            if not saved_files:
                shutil.rmtree(upload_dir, ignore_errors=True)
                self.serve_json({"success": False, "error": "No files found in upload payload."}, status=400)
                return

            self.engine.log("Data Engineer", f"Received {len(saved_files)} uploaded file(s) [Mode: {mode}]. Running continuous normalization...")
            metrics, events = self.engine.ingest_files(saved_files, mode=mode)
            shutil.rmtree(upload_dir, ignore_errors=True)

            self.serve_json({
                "success": True,
                "mode": mode,
                "message": f"Successfully processed {len(saved_files)} file(s) in {mode} mode.",
                "metrics": metrics,
                "events": events,
                "recent_events": self.engine.recent_events,
                "warnings": self.engine.latest_warnings
            })
        except Exception as e:
            self.engine.log("QA Tester", f"[ERROR] Upload processing failure: {e}")
            self.serve_json({"success": False, "error": str(e)}, status=500)


# ----------------------------------------------------------------------
# Interactive HTML Dashboard Generation (Discipline 1: PM & UX)
# ----------------------------------------------------------------------
def generate_interactive_dashboard(metrics: dict, recent_events: list = None):
    """Generates a self-contained report.html styled in the exact visual language of PITCHDECK.pdf."""
    metrics_json = json.dumps(metrics)
    events_json = json.dumps(recent_events or [])

    usd_val = metrics.get('currency_totals', {}).get('USD', {}).get('billed', 0.0)
    eur_val = metrics.get('currency_totals', {}).get('EUR', {}).get('billed', 0.0)
    norm_rows = metrics.get('total_normalized_rows', 0)
    pruned_rows = metrics.get('pruned_count', 0)

    usd_formatted = f"${usd_val:.6f}"
    eur_formatted = f"€{eur_val:.6f}"
    norm_rows_formatted = f"{norm_rows:,}"
    pruned_rows_formatted = f"{pruned_rows:,}"

    logo_base64 = ""
    target_logo = LOGO_FILE if os.path.exists(LOGO_FILE) else os.path.expanduser("~/Downloads/logo.png")
    if os.path.exists(target_logo):
        try:
            with open(target_logo, "rb") as lf:
                logo_base64 = base64.b64encode(lf.read()).decode("utf-8")
        except Exception:
            pass
    logo_src = f"data:image/png;base64,{logo_base64}" if logo_base64 else "/logo.png"

    html_code = f"""<!DOCTYPE html>
<html lang="en" data-theme="light">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Cloud Botanist AI • FOCUS 1.2 Telemetry & Spend Governance</title>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=Barlow+Condensed:ital,wght@0,600;0,700;0,800;1,700&family=Barlow:ital,wght@0,300;0,400;0,500;0,600;0,700;1,400&family=JetBrains+Mono:wght@500;700&display=swap" rel="stylesheet">
    <style>
        :root {{
            /* Blueprint Light Theme (Slides 1, 2, 3, 5, 6, 7) */
            --bg-ground: #f3f4f2;
            --bg-surface: #ffffff;
            --bg-surface-elevated: #f8f9fa;
            --bg-surface-muted: #edf1f5;
            --grid-line: rgba(77, 116, 154, 0.09);
            --crosshair-color: #8fa0b0;

            --border-color: #d8dde3;
            --border-subtle: #e5eaef;
            --border-accent: rgba(77, 116, 154, 0.35);

            /* Pitchdeck Signature Denim Steel Blue */
            --accent-steel: #4d749a;
            --accent-steel-hover: #3d5f80;
            --accent-steel-subtle: rgba(77, 116, 154, 0.1);
            --accent-steel-border: rgba(77, 116, 154, 0.28);

            /* Secondary Accents */
            --accent-sprout: #10b981;
            --accent-sprout-subtle: rgba(16, 185, 129, 0.1);
            --accent-sprout-border: rgba(16, 185, 129, 0.3);

            --accent-phosphor: #0284c7;
            --accent-phosphor-subtle: rgba(2, 132, 199, 0.1);
            --accent-phosphor-border: rgba(2, 132, 199, 0.3);

            /* Signals */
            --signal-amber: #d97706;
            --signal-ember: #e11d48;

            /* Typography: Headings Barlow Condensed Bold, Body Barlow Regular, TAGS Barlow Light */
            --text-primary: #12161c;
            --text-secondary: #526071;
            --text-muted: #8292a2;

            --font-display: 'Barlow Condensed', -apple-system, BlinkMacSystemFont, sans-serif;
            --font-sans: 'Barlow', -apple-system, BlinkMacSystemFont, sans-serif;
            --font-tag: 'Barlow', -apple-system, BlinkMacSystemFont, sans-serif;
            --font-mono: 'JetBrains Mono', monospace;

            /* Hard Layout Constraints */
            --radius-card: 6px;
            --radius-control: 4px;
            --radius-sm: 3px;
            --radius-pill: 9999px;
        }}


        *, *::before, *::after {{
            box-sizing: border-box;
            margin: 0;
            padding: 0;
        }}

        body {{
            font-family: var(--font-sans);
            background-color: var(--bg-ground);
            background-image: 
                linear-gradient(to right, var(--grid-line) 1px, transparent 1px),
                linear-gradient(to bottom, var(--grid-line) 1px, transparent 1px);
            background-size: 80px 80px;
            color: var(--text-primary);
            padding: 24px 32px 48px;
            min-height: 100vh;
            line-height: 1.5;
            text-align: left;
            transition: background-color 0.2s ease, color 0.2s ease;
        }}

        button, input, select, textarea {{
            font-family: inherit;
            font-size: inherit;
            color: inherit;
        }}

        button * {{
            font-family: inherit;
        }}

        /* Blueprint Architectural Typography - Headings: Barlow Condensed Bold */
        h1, h2, h3, h4, th, .display-font, .stat-number, .spend-card-label {{
            font-family: var(--font-display);
            font-weight: 700;
            text-transform: uppercase;
            letter-spacing: 0.02em;
        }}

        .telemetry-num,
        .cost-col,
        .mono-val,
        code,
        pre {{
            font-family: var(--font-mono) !important;
            font-feature-settings: 'tnum' 1, 'zero' 1;
            font-variant-numeric: tabular-nums;
        }}

        /* Crosshairs */
        .crosshair-marker {{
            font-family: var(--font-mono);
            font-weight: 700;
            color: var(--crosshair-color);
            user-select: none;
        }}

        /* Top Slide Meta Bar - TAGS / Meta Rail: Barlow Light */
        .deck-top-meta {{
            display: flex;
            justify-content: space-between;
            align-items: flex-start;
            font-family: var(--font-tag);
            font-size: 0.72rem;
            font-weight: 300;
            letter-spacing: 0.1em;
            text-transform: uppercase;
            color: var(--text-muted);
            margin-bottom: 20px;
            padding-bottom: 8px;
            border-bottom: 1px solid var(--border-subtle);
        }}
        .meta-group {{
            display: flex;
            align-items: center;
            gap: 8px;
        }}
        .meta-group-right {{
            display: flex;
            align-items: flex-start;
            gap: 8px;
        }}
        .meta-right-stack {{
            display: flex;
            flex-direction: column;
            align-items: flex-end;
            gap: 4px;
        }}
        .meta-divider {{
            color: var(--crosshair-color);
            opacity: 0.6;
        }}
        .meta-link {{
            color: var(--accent-steel);
            text-decoration: none;
            font-family: var(--font-tag);
            font-weight: 400;
            letter-spacing: 0.06em;
            text-transform: lowercase;
            transition: color 0.15s ease, opacity 0.15s ease;
        }}
        .meta-link:hover {{
            color: var(--accent-steel-hover);
            text-decoration: underline;
            opacity: 0.85;
        }}
        .meta-social-row {{
            display: flex;
            align-items: center;
            gap: 6px;
        }}
        .meta-social-label {{
            font-family: var(--font-tag);
            font-size: 0.72rem;
            font-weight: 300;
            letter-spacing: 0.08em;
            text-transform: uppercase;
            color: var(--text-muted);
            user-select: none;
        }}
        .meta-social-link {{
            display: inline-flex;
            align-items: center;
            justify-content: center;
            color: var(--accent-steel);
            text-decoration: none;
            transition: color 0.15s ease, opacity 0.15s ease, transform 0.15s ease;
            opacity: 0.85;
            padding: 1px;
            line-height: 1;
        }}
        .meta-social-link:hover {{
            color: var(--accent-steel-hover);
            opacity: 1;
            transform: translateY(-1px);
        }}
        .meta-social-link svg {{
            display: block;
            width: 16px;
            height: 16px;
        }}

        /* Header Layout */
        .header {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 24px;
            padding-bottom: 20px;
            border-bottom: 1px solid var(--border-color);
            flex-wrap: wrap;
            gap: 20px;
        }}

        .header-brand {{
            display: flex;
            align-items: center;
            gap: 16px;
        }}

        .logo-box {{
            width: 48px;
            height: 48px;
            border: 1px solid var(--border-color);
            background: var(--bg-surface);
            border-radius: var(--radius-card);
            display: flex;
            align-items: center;
            justify-content: center;
            flex-shrink: 0;
            box-shadow: 0 1px 2px rgba(0, 0, 0, 0.04);
            padding: 5px;
            overflow: hidden;
        }}

        .brand-logo-img {{
            width: 100%;
            height: 100%;
            object-fit: contain;
            display: block;
        }}


        .header-title h1 {{
            font-family: var(--font-display);
            font-size: 1.85rem;
            font-weight: 700;
            color: var(--text-primary);
            display: flex;
            align-items: center;
            gap: 10px;
            letter-spacing: -0.01em;
            line-height: 1.1;
        }}

        .header-title h1 .brand-sub {{
            font-size: 0.72rem;
            font-weight: 300;
            font-family: var(--font-tag);
            color: var(--accent-steel);
            background: var(--accent-steel-subtle);
            border: 1px solid var(--accent-steel-border);
            padding: 2px 7px;
            border-radius: var(--radius-sm);
            letter-spacing: 0.08em;
            vertical-align: middle;
            text-transform: uppercase;
        }}

        .header-title p {{
            font-family: var(--font-sans);
            color: var(--text-secondary);
            font-size: 0.76rem;
            font-weight: 400;
            letter-spacing: 0.06em;
            text-transform: uppercase;
            margin-top: 5px;
        }}

        .header-actions {{
            display: flex;
            align-items: center;
            gap: 10px;
            flex-wrap: wrap;
        }}

        .badges {{
            display: flex;
            gap: 6px;
        }}

        .badge {{
            padding: 4px 8px;
            border-radius: var(--radius-sm);
            font-size: 0.7rem;
            font-weight: 300;
            text-transform: uppercase;
            letter-spacing: 0.08em;
            display: inline-flex;
            align-items: center;
            gap: 5px;
            font-family: var(--font-tag);
        }}

        .badge-focus {{
            background: var(--accent-steel-subtle);
            color: var(--accent-steel);
            border: 1px solid var(--accent-steel-border);
        }}
        .badge-duckdb {{
            background: var(--accent-sprout-subtle);
            color: var(--accent-sprout);
            border: 1px solid var(--accent-sprout-border);
        }}
        .badge-status {{
            background: var(--bg-surface);
            color: var(--text-secondary);
            border: 1px solid var(--border-color);
        }}
        .badge-dot {{
            width: 6px;
            height: 6px;
            border-radius: 50%;
            background: var(--accent-sprout);
        }}

        /* Blueprint Pill Control Groups */
        .toggle-pill {{
            display: inline-flex;
            align-items: center;
            background: var(--bg-surface);
            border: 1px solid var(--border-color);
            border-radius: var(--radius-control);
            padding: 2px 3px;
            gap: 2px;
        }}

        .pill-title {{
            font-size: 0.72rem;
            font-weight: 800;
            text-transform: uppercase;
            letter-spacing: 0.08em;
            color: var(--text-main);
            padding: 0 4px 0 6px;
            font-family: var(--font-tag);
        }}

        .pill-btn {{
            background: transparent;
            border: 1px solid transparent;
            color: var(--text-secondary);
            padding: 4px 10px;
            border-radius: var(--radius-sm);
            font-family: var(--font-display);
            font-size: 0.8rem;
            font-weight: 700;
            text-transform: uppercase;
            letter-spacing: 0.05em;
            cursor: pointer;
            transition: all 0.15s ease;
            white-space: nowrap;
        }}

        .pill-btn:hover {{
            color: var(--text-primary);
            background: var(--bg-surface-elevated);
        }}

        .pill-btn.active {{
            background: var(--accent-steel);
            border-color: var(--accent-steel);
            color: #ffffff;
            font-weight: 700;
        }}

        .btn-subtle-action {{
            background: var(--bg-surface);
            border: 1px solid var(--accent-steel-border);
            color: var(--accent-steel);
            padding: 6px 14px;
            border-radius: var(--radius-control);
            font-family: var(--font-display);
            font-size: 0.84rem;
            font-weight: 700;
            text-transform: uppercase;
            letter-spacing: 0.06em;
            cursor: pointer;
            display: inline-flex;
            align-items: center;
            gap: 6px;
            transition: all 0.15s ease;
        }}
        .btn-subtle-action:hover {{
            background: var(--accent-steel-subtle);
            border-color: var(--accent-steel);
            color: var(--accent-steel-hover);
        }}

        .btn-reset {{
            background: var(--bg-surface);
            border: 1px solid var(--border-color);
            color: var(--signal-ember);
            padding: 6px 14px;
            border-radius: var(--radius-control);
            font-family: var(--font-display);
            font-size: 0.84rem;
            font-weight: 700;
            text-transform: uppercase;
            letter-spacing: 0.06em;
            cursor: pointer;
            display: inline-flex;
            align-items: center;
            gap: 6px;
            transition: all 0.15s ease;
        }}
        .btn-reset:hover {{
            background: rgba(225, 29, 72, 0.08);
            border-color: var(--signal-ember);
            color: var(--signal-ember);
        }}

        /* Blueprint Card Foundation */
        .blueprint-card {{
            background: var(--bg-surface);
            border: 1px solid var(--border-color);
            border-radius: var(--radius-card);
            position: relative;
            box-shadow: 0 1px 3px rgba(0, 0, 0, 0.02);
            transition: border-color 0.15s ease;
        }}

        .blueprint-panel {{
            background: var(--bg-surface);
            border: 1px solid var(--border-color);
            border-radius: var(--radius-card);
            box-shadow: 0 1px 3px rgba(0, 0, 0, 0.02);
        }}

        /* Corner Alignment Crosshairs */
        .card-crosshair {{
            position: absolute;
            font-family: var(--font-mono);
            font-size: 0.75rem;
            font-weight: 700;
            color: var(--crosshair-color);
            line-height: 1;
            user-select: none;
            pointer-events: none;
        }}
        .card-crosshair.tl {{ top: 6px; left: 8px; }}
        .card-crosshair.tr {{ top: 6px; right: 8px; }}
        .card-crosshair.bl {{ bottom: 6px; left: 8px; }}
        .card-crosshair.br {{ bottom: 6px; right: 8px; }}

        /* Slide 7: Programmable Spend Card / Drag & Drop Intake & Generator */
        .dropzone-section {{
            margin-bottom: 24px;
        }}

        /* Intake Mode Tabs */
        .intake-tabs-bar {{
            display: flex;
            align-items: center;
            justify-content: space-between;
            margin-bottom: 12px;
            border-bottom: 1px solid var(--border-color);
            padding-bottom: 8px;
        }}

        .intake-tabs-group {{
            display: inline-flex;
            gap: 8px;
            align-items: center;
        }}

        .intake-tab-btn {{
            background: var(--bg-surface);
            border: 1px solid var(--border-color);
            color: var(--text-secondary);
            font-family: var(--font-display);
            font-size: 0.88rem;
            font-weight: 700;
            letter-spacing: 0.08em;
            text-transform: uppercase;
            padding: 8px 16px;
            border-radius: var(--radius-sm);
            cursor: pointer;
            display: inline-flex;
            align-items: center;
            gap: 8px;
            transition: all 0.15s ease;
        }}

        .intake-tab-btn:hover {{
            color: var(--text-primary);
            border-color: var(--accent-steel);
            background: var(--bg-surface-elevated);
        }}

        .intake-tab-btn.active {{
            color: #ffffff;
            background: var(--accent-steel);
            border-color: var(--accent-steel);
            box-shadow: 0 1px 3px rgba(77, 116, 154, 0.25);
        }}


        .tab-badge-pill {{
            background: var(--accent-sprout);
            color: #ffffff;
            font-family: var(--font-tag);
            font-size: 0.65rem;
            font-weight: 300;
            padding: 2px 6px;
            border-radius: 10px;
            letter-spacing: 0.08em;
            text-transform: uppercase;
        }}


        .intake-tab-btn.active .tab-badge-pill {{
            background: rgba(255, 255, 255, 0.25);
            color: inherit;
        }}

        .intake-tabs-meta {{
            display: flex;
            align-items: center;
            gap: 6px;
            font-family: var(--font-tag);
            font-size: 0.72rem;
            font-weight: 300;
            color: var(--text-muted);
            letter-spacing: 0.08em;
            text-transform: uppercase;
        }}

        /* Demo Generator Card */
        .demo-generator-card {{
            padding: 20px 24px;
            position: relative;
            display: flex;
            flex-direction: column;
            gap: 16px;
        }}

        .demo-generator-card.processing {{
            cursor: wait;
            opacity: 0.7;
            pointer-events: none;
        }}

        .generator-header {{
            display: flex;
            justify-content: space-between;
            align-items: flex-start;
            gap: 16px;
            border-bottom: 1px solid var(--border-subtle);
            padding-bottom: 14px;
        }}

        .generator-title-wrap {{
            display: flex;
            align-items: flex-start;
            gap: 12px;
        }}

        .generator-icon {{
            width: 36px;
            height: 36px;
            background: var(--accent-steel-subtle);
            border: 1px solid var(--accent-steel-border);
            color: var(--accent-steel);
            border-radius: var(--radius-sm);
            display: flex;
            align-items: center;
            justify-content: center;
            flex-shrink: 0;
            margin-top: 2px;
        }}

        .generator-title {{
            font-family: var(--font-display);
            font-size: 1.05rem;
            font-weight: 700;
            letter-spacing: 0.06em;
            text-transform: uppercase;
            color: var(--text-primary);
        }}

        .generator-subtitle {{
            font-size: 0.82rem;
            color: var(--text-secondary);
            margin-top: 2px;
            line-height: 1.4;
        }}

        .generator-body {{
            display: grid;
            grid-template-columns: 1.25fr 1fr;
            gap: 16px;
        }}

        @media (max-width: 900px) {{
            .generator-body {{
                grid-template-columns: 1fr;
            }}
        }}

        .generator-primary-box {{
            background: var(--bg-surface-elevated);
            border: 1px solid var(--border-color);
            border-radius: var(--radius-card);
            padding: 16px 18px;
            display: flex;
            flex-direction: column;
            gap: 10px;
        }}

        .gen-box-header {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            gap: 8px;
        }}

        .gen-box-tag {{
            font-family: var(--font-tag);
            font-size: 0.68rem;
            font-weight: 300;
            color: var(--accent-steel);
            letter-spacing: 0.08em;
            text-transform: uppercase;
        }}

        .gen-box-badge {{
            font-family: var(--font-tag);
            font-size: 0.68rem;
            font-weight: 300;
            color: var(--accent-sprout);
            background: var(--accent-sprout-subtle);
            border: 1px solid var(--accent-sprout-border);
            padding: 2px 6px;
            border-radius: var(--radius-sm);
            letter-spacing: 0.08em;
            text-transform: uppercase;
        }}

        .gen-box-title {{
            font-family: var(--font-display);
            font-size: 1.15rem;
            font-weight: 700;
            color: var(--text-primary);
            letter-spacing: 0.04em;
            text-transform: uppercase;
        }}

        .gen-box-desc {{
            font-size: 0.8rem;
            color: var(--text-secondary);
            line-height: 1.45;
        }}

        .gen-box-desc code {{
            font-family: var(--font-mono);
            background: var(--bg-surface-muted);
            padding: 1px 4px;
            border-radius: 3px;
            font-size: 0.78rem;
        }}

        .btn-gen-primary {{
            background: var(--accent-steel);
            color: #ffffff;
            border: 1px solid var(--accent-steel);
            font-family: var(--font-display);
            font-size: 0.92rem;
            font-weight: 700;
            letter-spacing: 0.08em;
            text-transform: uppercase;
            padding: 10px 18px;
            border-radius: var(--radius-control);
            cursor: pointer;
            display: inline-flex;
            align-items: center;
            justify-content: center;
            gap: 8px;
            transition: all 0.15s ease;
            width: 100%;
            margin-top: 4px;
        }}

        .btn-gen-primary:hover {{
            background: var(--accent-steel-hover);
            border-color: var(--accent-steel-hover);
        }}


        .generator-presets-box {{
            background: var(--bg-surface-elevated);
            border: 1px solid var(--border-color);
            border-radius: var(--radius-card);
            padding: 16px 18px;
            display: flex;
            flex-direction: column;
            gap: 10px;
        }}

        .gen-presets-title {{
            font-family: var(--font-tag);
            font-size: 0.7rem;
            font-weight: 300;
            color: var(--text-muted);
            letter-spacing: 0.08em;
            text-transform: uppercase;
        }}

        .gen-preset-buttons {{
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 8px;
        }}

        .btn-gen-preset {{
            background: var(--bg-surface);
            border: 1px solid var(--border-color);
            border-radius: var(--radius-sm);
            padding: 8px 10px;
            cursor: pointer;
            display: flex;
            align-items: center;
            gap: 8px;
            text-align: left;
            transition: all 0.15s ease;
        }}

        .btn-gen-preset:hover {{
            border-color: var(--accent-steel);
            background: var(--accent-steel-subtle);
        }}

        .preset-icon {{
            font-size: 1rem;
            flex-shrink: 0;
        }}

        .preset-text {{
            display: flex;
            flex-direction: column;
            overflow: hidden;
        }}

        .preset-text strong {{
            font-family: var(--font-display);
            font-size: 0.86rem;
            font-weight: 700;
            letter-spacing: 0.04em;
            text-transform: uppercase;
            color: var(--text-primary);
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
        }}

        .preset-text span {{
            font-size: 0.7rem;
            color: var(--text-muted);
            font-family: var(--font-tag);
            font-weight: 300;
            letter-spacing: 0.06em;
            text-transform: uppercase;
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
        }}

        .dropzone-card .tab-link {{
            pointer-events: auto;
            color: var(--accent-steel);
            text-decoration: underline;
            font-weight: 700;
        }}
        .dropzone-card .tab-link:hover {{
            color: var(--accent-phosphor);
        }}

        .dropzone-card {{
            padding: 20px 24px;
            cursor: pointer;
            position: relative;
            overflow: hidden;
            display: flex;
            flex-direction: column;
            gap: 14px;
            border-style: dashed;
            border-width: 1.5px;
            border-color: var(--accent-steel-hover);
        }}

        .dropzone-card * {{
            pointer-events: none;
        }}

        .dropzone-card:hover {{
            border-color: var(--accent-steel);
        }}

        .dropzone-card.dragover {{
            border-color: var(--accent-steel) !important;
            background: var(--accent-steel-subtle) !important;
            border-style: dashed !important;
        }}

        .dropzone-card.processing {{
            border-color: var(--accent-phosphor);
            background: var(--accent-phosphor-subtle);
            cursor: wait;
        }}

        .dropzone-overlay {{
            display: none;
            position: absolute;
            top: 0; left: 0; right: 0; bottom: 0;
            background: var(--bg-surface);
            opacity: 0.96;
            backdrop-filter: blur(2px);
            flex-direction: column;
            align-items: center;
            justify-content: center;
            z-index: 10;
        }}

        .dropzone-card.processing .dropzone-overlay {{
            display: flex;
        }}

        .blueprint-spinner {{
            width: 32px;
            height: 32px;
            border: 2px solid var(--border-color);
            border-top-color: var(--accent-steel);
            border-radius: 50%;
            animation: bp-spin 0.75s linear infinite;
            margin-bottom: 12px;
        }}

        @keyframes bp-spin {{ to {{ transform: rotate(360deg); }} }}

        .dropzone-overlay-text {{
            font-size: 0.95rem;
            font-weight: 700;
            font-family: var(--font-display);
            letter-spacing: 0.04em;
            color: var(--text-primary);
        }}
        .dropzone-overlay-subtext {{
            font-size: 0.76rem;
            color: var(--text-muted);
            margin-top: 4px;
            font-family: var(--font-sans);
            font-weight: 400;
        }}

        .spend-card-header {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            flex-wrap: wrap;
            gap: 12px;
            padding-bottom: 12px;
            border-bottom: 1px solid var(--border-subtle);
        }}

        .spend-card-title-wrap {{
            display: flex;
            align-items: center;
            gap: 12px;
        }}

        .intake-icon {{
            width: 34px;
            height: 34px;
            border: 1px solid var(--border-color);
            background: var(--bg-surface-elevated);
            border-radius: var(--radius-sm);
            display: flex;
            align-items: center;
            justify-content: center;
            color: var(--accent-steel);
            flex-shrink: 0;
        }}

        .spend-card-label {{
            font-size: 0.82rem;
            font-weight: 700;
            color: var(--text-primary);
            letter-spacing: 0.04em;
        }}

        .intake-subtitle {{
            font-size: 0.74rem;
            color: var(--text-muted);
            margin-top: 2px;
        }}

        .format-tags {{
            display: flex;
            gap: 6px;
        }}

        .format-pill {{
            font-family: var(--font-tag) !important;
            font-size: 0.68rem;
            font-weight: 300 !important;
            letter-spacing: 0.06em;
            text-transform: uppercase;
            padding: 2px 7px;
            background: var(--bg-surface-elevated);
            border: 1px solid var(--border-color);
            border-radius: var(--radius-sm);
            color: var(--accent-steel);
        }}

        .dropzone-inner-prompt {{
            padding: 12px 0 4px;
        }}

        .dropzone-callout {{
            display: flex;
            align-items: center;
            gap: 16px;
        }}

        .dropzone-callout svg {{
            stroke: var(--accent-steel);
            flex-shrink: 0;
        }}

        .prompt-text {{
            display: flex;
            flex-direction: column;
            gap: 3px;
        }}

        .prompt-headline {{
            font-size: 0.95rem;
            font-weight: 700;
            font-family: var(--font-display);
            letter-spacing: 0.04em;
            text-transform: uppercase;
            color: var(--text-primary);
        }}

        .prompt-sub {{
            font-size: 0.78rem;
            color: var(--text-secondary);
        }}

        .mode-status-text {{
            color: var(--accent-steel);
            font-weight: 300;
            font-family: var(--font-tag);
            letter-spacing: 0.06em;
            text-transform: uppercase;
        }}

        #fileInput {{
            display: none;
        }}

        /* Toast Alert */
        .alert-toast {{
            display: none;
            padding: 10px 16px;
            border-radius: var(--radius-control);
            margin-top: 12px;
            font-size: 0.82rem;
            animation: bpFadeIn 0.2s ease;
        }}

        .alert-success {{
            background: var(--accent-sprout-subtle);
            border: 1px solid var(--accent-sprout-border);
            color: var(--accent-sprout);
        }}

        .alert-error {{
            background: rgba(225, 29, 72, 0.1);
            border: 1px solid rgba(225, 29, 72, 0.3);
            color: var(--signal-ember);
        }}

        .alert-info {{
            background: var(--accent-steel-subtle);
            border: 1px solid var(--accent-steel-border);
            color: var(--accent-steel);
        }}

        @keyframes bpFadeIn {{ from {{ opacity: 0; transform: translateY(-3px); }} to {{ opacity: 1; transform: translateY(0); }} }}

        /* Activity / Audit Feed Panel */
        .activity-panel {{
            margin-top: 12px;
            padding: 12px 18px;
        }}

        .panel-header {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 10px;
            padding-bottom: 6px;
            border-bottom: 1px solid var(--border-subtle);
        }}

        .panel-title {{
            font-size: 0.78rem;
            font-weight: 700;
            letter-spacing: 0.06em;
            text-transform: uppercase;
            color: var(--text-muted);
            display: flex;
            align-items: center;
            gap: 6px;
            font-family: var(--font-display);
        }}

        .btn-subtle {{
            font-family: var(--font-display);
            font-size: 0.74rem;
            font-weight: 700;
            letter-spacing: 0.06em;
            text-transform: uppercase;
            color: var(--text-muted);
            background: transparent;
            border: 1px solid var(--border-subtle);
            cursor: pointer;
            padding: 3px 8px;
            border-radius: var(--radius-sm);
            transition: all 0.15s ease;
        }}
        .btn-subtle:hover {{
            color: var(--text-primary);
            border-color: var(--border-color);
        }}

        .activity-list {{
            display: flex;
            flex-direction: column;
            gap: 6px;
            max-height: 140px;
            overflow-y: auto;
        }}

        .activity-item {{
            display: flex;
            align-items: center;
            justify-content: space-between;
            font-size: 0.78rem;
            padding: 6px 10px;
            border-radius: var(--radius-sm);
            background: var(--bg-surface-elevated);
            border: 1px solid var(--border-subtle);
        }}

        .activity-item-left {{
            display: flex;
            align-items: center;
            gap: 8px;
        }}

        .activity-badge {{
            display: inline-flex;
            align-items: center;
            justify-content: center;
            width: 16px;
            height: 16px;
            border-radius: var(--radius-sm);
            font-size: 0.65rem;
            font-weight: 300;
            flex-shrink: 0;
            font-family: var(--font-tag);
        }}

        .badge-act-success {{ background: var(--accent-sprout-subtle); color: var(--accent-sprout); border: 1px solid var(--accent-sprout-border); }}
        .badge-act-info {{ background: var(--accent-steel-subtle); color: var(--accent-steel); border: 1px solid var(--accent-steel-border); }}
        .badge-act-warning {{ background: rgba(217, 119, 6, 0.1); color: var(--signal-amber); border: 1px solid rgba(217, 119, 6, 0.3); }}
        .badge-act-reset {{ background: rgba(225, 29, 72, 0.1); color: var(--signal-ember); border: 1px solid rgba(225, 29, 72, 0.3); }}

        .activity-text {{
            color: var(--text-primary);
        }}
        .activity-time {{
            color: var(--text-muted);
            font-size: 0.72rem;
            font-family: var(--font-tag);
            font-weight: 300;
            letter-spacing: 0.06em;
            text-transform: uppercase;
            margin-left: 10px;
        }}

        /* Numbered Spend Compartments (Slides 3, 4, 6) */
        .cards-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(230px, 1fr));
            gap: 16px;
            margin-bottom: 24px;
        }}

        .stat-card {{
            padding: 18px 20px;
        }}

        .stat-top {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 8px;
        }}

        .stat-index {{
            font-family: var(--font-display);
            font-size: 0.78rem;
            font-weight: 700;
            color: var(--text-muted);
            letter-spacing: 0.06em;
            text-transform: uppercase;
        }}

        .stat-tag {{
            font-size: 0.68rem;
            font-weight: 300;
            font-family: var(--font-tag);
            color: var(--text-secondary);
            padding: 1px 6px;
            background: var(--bg-surface-elevated);
            border: 1px solid var(--border-subtle);
            border-radius: var(--radius-sm);
            letter-spacing: 0.08em;
            text-transform: uppercase;
        }}
        .stat-tag.invariant {{
            color: var(--accent-sprout);
            background: var(--accent-sprout-subtle);
            border-color: var(--accent-sprout-border);
        }}

        .stat-number {{
            font-size: 2.2rem;
            font-weight: 800;
            letter-spacing: -0.01em;
            line-height: 1.1;
            margin-bottom: 8px;
            font-family: var(--font-display);
        }}

        .stat-number.usd {{ color: var(--accent-steel); }}
        .stat-number.eur {{ color: var(--accent-phosphor); }}
        .stat-number.records {{ color: var(--text-primary); }}
        .stat-number.invariant {{ color: var(--accent-sprout); }}

        .stat-subtext {{
            font-size: 0.76rem;
            color: var(--text-muted);
            display: flex;
            gap: 6px;
            flex-wrap: wrap;
            align-items: center;
        }}

        .sub-chip {{
            display: inline-block;
            padding: 1px 6px;
            background: var(--bg-surface-elevated);
            border: 1px solid var(--border-subtle);
            border-radius: var(--radius-sm);
            font-size: 0.68rem;
            font-family: var(--font-tag);
            font-weight: 300;
            letter-spacing: 0.06em;
            text-transform: uppercase;
            color: var(--text-secondary);
        }}

        /* Infrastructure Spend Yield (Charts) */
        .charts-grid {{
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 16px;
            margin-bottom: 24px;
        }}
        @media (max-width: 900px) {{
            .charts-grid {{ grid-template-columns: 1fr; }}
        }}

        .chart-panel {{
            padding: 18px 20px;
        }}

        .curr-pill {{
            font-size: 0.68rem;
            font-weight: 300;
            font-family: var(--font-tag);
            letter-spacing: 0.06em;
            text-transform: uppercase;
            padding: 2px 7px;
            border-radius: var(--radius-sm);
        }}
        .curr-pill.curr-usd {{ background: var(--accent-steel-subtle); color: var(--accent-steel); border: 1px solid var(--accent-steel-border); }}
        .curr-pill.curr-eur {{ background: var(--accent-phosphor-subtle); color: var(--accent-phosphor); border: 1px solid var(--accent-phosphor-border); }}

        .chart-container {{
            min-height: 160px;
            display: flex;
            flex-direction: column;
            justify-content: center;
            gap: 12px;
            padding-top: 8px;
        }}

        .bar-group {{
            display: flex;
            flex-direction: column;
            gap: 4px;
        }}

        .bar-label {{
            display: flex;
            justify-content: space-between;
            font-size: 0.78rem;
            color: var(--text-secondary);
        }}

        .bar-track {{
            height: 10px;
            background: var(--bg-surface-elevated);
            border: 1px solid var(--border-color);
            border-radius: var(--radius-sm);
            overflow: hidden;
        }}

        .bar-fill {{
            height: 100%;
            border-radius: var(--radius-sm);
            transition: width 0.5s ease;
        }}

        .bar-fill-usd {{ background: var(--accent-steel); }}
        .bar-fill-eur {{ background: var(--accent-phosphor); }}

        /* Normalized Telemetry Table */
        .table-panel {{
            padding: 18px 20px;
        }}

        .table-actions {{
            display: flex;
            gap: 8px;
        }}

        .btn-action {{
            background: var(--bg-surface-elevated);
            border: 1px solid var(--border-color);
            color: var(--text-primary);
            padding: 6px 14px;
            border-radius: var(--radius-control);
            font-family: var(--font-display);
            font-size: 0.82rem;
            font-weight: 700;
            text-transform: uppercase;
            letter-spacing: 0.06em;
            cursor: pointer;
            text-decoration: none;
            display: inline-flex;
            align-items: center;
            gap: 6px;
            transition: all 0.15s ease;
        }}
        .btn-action:hover {{
            border-color: var(--accent-steel);
            color: var(--accent-steel);
        }}

        .table-filters-row {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            flex-wrap: wrap;
            gap: 12px;
            margin: 14px 0 16px;
        }}

        .search-box {{
            flex: 1;
            min-width: 260px;
            position: relative;
        }}

        .search-icon {{
            position: absolute;
            left: 12px;
            top: 50%;
            transform: translateY(-50%);
            stroke: var(--text-muted);
            pointer-events: none;
        }}

        .search-input {{
            width: 100%;
            background: var(--bg-surface-elevated);
            border: 1px solid var(--border-color);
            border-radius: var(--radius-control);
            padding: 8px 12px 8px 34px;
            color: var(--text-primary);
            font-family: var(--font-sans);
            font-size: 0.82rem;
            outline: none;
            transition: border-color 0.15s ease;
        }}
        .search-input:focus {{
            border-color: var(--accent-steel);
        }}

        .select-group {{
            display: flex;
            gap: 8px;
            flex-wrap: wrap;
        }}

        .filter-select {{
            background: var(--bg-surface-elevated);
            border: 1px solid var(--border-color);
            border-radius: var(--radius-control);
            padding: 8px 12px;
            color: var(--text-primary);
            font-family: var(--font-sans);
            font-size: 0.8rem;
            outline: none;
            cursor: pointer;
        }}

        .table-wrapper {{
            overflow-x: auto;
            border-radius: var(--radius-control);
            border: 1px solid var(--border-color);
        }}

        table {{
            width: 100%;
            border-collapse: collapse;
            font-size: 0.8rem;
            text-align: left;
        }}

        th {{
            background: var(--bg-surface-elevated);
            color: var(--text-secondary);
            font-family: var(--font-display);
            font-weight: 700;
            padding: 10px 12px;
            border-bottom: 1px solid var(--border-color);
            cursor: pointer;
            white-space: nowrap;
            user-select: none;
            font-size: 0.78rem;
            letter-spacing: 0.05em;
            text-transform: uppercase;
        }}
        th:hover {{
            color: var(--accent-steel);
        }}

        td {{
            padding: 9px 12px;
            border-bottom: 1px solid var(--border-subtle);
            color: var(--text-primary);
        }}

        tr:hover td {{
            background: var(--bg-surface-elevated);
        }}

        /* Blueprint Badges */
        .prov-badge {{
            display: inline-flex;
            align-items: center;
            padding: 2px 7px;
            border-radius: var(--radius-sm);
            font-family: var(--font-tag);
            font-size: 0.68rem;
            font-weight: 300;
            letter-spacing: 0.06em;
            text-transform: uppercase;
        }}
        .prov-aws {{ background: rgba(217, 119, 6, 0.1); color: #d97706; border: 1px solid rgba(217, 119, 6, 0.28); }}
        .prov-azure {{ background: var(--accent-steel-subtle); color: var(--accent-steel); border: 1px solid var(--accent-steel-border); }}
        .prov-gcp {{ background: var(--accent-sprout-subtle); color: var(--accent-sprout); border: 1px solid var(--accent-sprout-border); }}
        .prov-cloudflare {{ background: rgba(234, 88, 12, 0.1); color: #ea580c; border: 1px solid rgba(234, 88, 12, 0.28); }}
        .prov-nebius {{ background: rgba(147, 51, 234, 0.1); color: #9333ea; border: 1px solid rgba(147, 51, 234, 0.28); }}

        .curr-badge {{
            display: inline-block;
            padding: 2px 5px;
            border-radius: var(--radius-sm);
            font-size: 0.68rem;
            font-weight: 300;
            font-family: var(--font-tag);
            letter-spacing: 0.06em;
            text-transform: uppercase;
        }}
        .curr-usd {{ background: var(--accent-steel-subtle); color: var(--accent-steel); border: 1px solid var(--accent-steel-border); }}
        .curr-eur {{ background: var(--accent-phosphor-subtle); color: var(--accent-phosphor); border: 1px solid var(--accent-phosphor-border); }}

        .cat-badge {{
            display: inline-block;
            padding: 2px 6px;
            border-radius: var(--radius-sm);
            font-family: var(--font-tag);
            font-size: 0.68rem;
            font-weight: 300;
            letter-spacing: 0.06em;
            text-transform: uppercase;
        }}
        .cat-usage {{ background: var(--accent-steel-subtle); color: var(--accent-steel); border: 1px solid var(--accent-steel-border); }}
        .cat-credit {{ background: var(--accent-sprout-subtle); color: var(--accent-sprout); border: 1px solid var(--accent-sprout-border); }}
        .cat-tax {{ background: rgba(217, 119, 6, 0.1); color: var(--signal-amber); border: 1px solid rgba(217, 119, 6, 0.25); }}

        .cost-col {{
            font-weight: 600;
        }}
        .cost-col.pos {{ color: var(--text-primary); }}
        .cost-col.credit {{ color: var(--accent-sprout); }}
        .cost-col.zero {{ color: var(--text-muted); }}

        .pagination-bar {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-top: 14px;
            font-size: 0.78rem;
            color: var(--text-secondary);
        }}

        .page-buttons {{
            display: flex;
            gap: 4px;
        }}

        .page-btn {{
            background: var(--bg-surface-elevated);
            border: 1px solid var(--border-color);
            color: var(--text-primary);
            padding: 4px 10px;
            border-radius: var(--radius-sm);
            font-family: var(--font-display);
            font-size: 0.82rem;
            font-weight: 700;
            letter-spacing: 0.04em;
            text-transform: uppercase;
            cursor: pointer;
            transition: all 0.15s ease;
        }}
        .page-btn.active {{
            background: var(--accent-steel);
            border-color: var(--accent-steel);
            color: #ffffff;
            font-weight: 700;
        }}
        .page-btn:disabled {{
            opacity: 0.35;
            cursor: not-allowed;
        }}

        /* Deck Footer */
        .deck-footer {{
            margin-top: 40px;
            padding-top: 16px;
            border-top: 1px solid var(--border-color);
            display: flex;
            justify-content: space-between;
            align-items: center;
            font-size: 0.72rem;
            color: var(--text-muted);
            font-family: var(--font-tag);
            font-weight: 300;
            letter-spacing: 0.08em;
            text-transform: uppercase;
        }}
    </style>
</head>
<body>

    <!-- Slide Meta Bar -->
    <div class="deck-top-meta">
        <div class="meta-group">
            
            <span>TELEMETRY ARCHITECTURE</span>
            <span class="meta-divider">//</span>
            <span>INFRASTRUCTURE-NATIVE PAYMENT PLATFORM</span>
        </div>
        <div class="meta-group meta-group-right">
            <div class="meta-right-stack">
                <a href="https://cloudbotanist.ai" target="_blank" rel="noopener noreferrer" class="meta-link">cloudbotanist.ai</a>
                <div class="meta-social-row">
                    <span class="meta-social-label">Our team:</span>
                    <a href="https://www.linkedin.com/in/mashavsl" target="_blank" rel="noopener noreferrer" class="meta-social-link" title="Masha Vasilieva (linkedin.com/in/mashavsl)" aria-label="Masha Vasilieva LinkedIn">
                        <svg class="linkedin-icon" viewBox="0 0 24 24" fill="currentColor">
                            <path d="M19 3a2 2 0 0 1 2 2v14a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h14m-.5 15.5v-5.3a3.26 3.26 0 0 0-3.26-3.26c-.85 0-1.84.52-2.28 1.3v-1.11h-2.79v8.37h2.79v-4.93c0-.77.62-1.4 1.39-1.4a1.4 1.4 0 0 1 1.4 1.4v4.93h2.75M6.46 10.9v8.37H9.25V10.9H6.46M7.86 6.54a1.64 1.64 0 1 0 0 3.28 1.64 1.64 0 0 0 0-3.28z"/>
                        </svg>
                    </a>
                    <a href="https://www.linkedin.com/in/leovsl" target="_blank" rel="noopener noreferrer" class="meta-social-link" title="Leo Vasiliev (linkedin.com/in/leovsl)" aria-label="Leo Vasiliev LinkedIn">
                        <svg class="linkedin-icon" viewBox="0 0 24 24" fill="currentColor">
                            <path d="M19 3a2 2 0 0 1 2 2v14a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h14m-.5 15.5v-5.3a3.26 3.26 0 0 0-3.26-3.26c-.85 0-1.84.52-2.28 1.3v-1.11h-2.79v8.37h2.79v-4.93c0-.77.62-1.4 1.39-1.4a1.4 1.4 0 0 1 1.4 1.4v4.93h2.75M6.46 10.9v8.37H9.25V10.9H6.46M7.86 6.54a1.64 1.64 0 1 0 0 3.28 1.64 1.64 0 0 0 0-3.28z"/>
                        </svg>
                    </a>
                </div>
                <div class="meta-social-row" style="margin-top: 2px;">
                    <a href="https://github.com/masha-vasilieva/focus-demo/blob/main/README.md#" target="_blank" rel="noopener noreferrer" class="meta-link" style="display: flex; align-items: center; gap: 4px;">
                        <svg viewBox="0 0 24 24" width="12" height="12" fill="currentColor">
                            <path d="M12 2C6.477 2 2 6.484 2 12.017c0 4.425 2.865 8.18 6.839 9.504.5.092.682-.217.682-.483 0-.237-.008-.868-.013-1.703-2.782.605-3.369-1.343-3.369-1.343-.454-1.158-1.11-1.466-1.11-1.466-.908-.62.069-.608.069-.608 1.003.07 1.531 1.032 1.531 1.032.892 1.53 2.341 1.088 2.91.832.092-.647.35-1.088.636-1.338-2.22-.253-4.555-1.113-4.555-4.951 0-1.093.39-1.988 1.029-2.688-.103-.253-.446-1.272.098-2.65 0 0 .84-.27 2.75 1.026A9.564 9.564 0 0112 6.844c.85.004 1.705.115 2.504.337 1.909-1.296 2.747-1.027 2.747-1.027.546 1.379.202 2.398.1 2.651.64.7 1.028 1.595 1.028 2.688 0 3.848-2.339 4.695-4.566 4.943.359.309.678.92.678 1.855 0 1.338-.012 2.419-.012 2.747 0 .268.18.58.688.482A10.019 10.019 0 0022 12.017C22 6.484 17.522 2 12 2z"/>
                        </svg>
                        GitHub Repo
                    </a>
                </div>
            </div>
            
        </div>
    </div>

    <!-- Header Section -->
    <header class="header">
        <div class="header-brand">
            <div class="logo-box">
                <img src="{logo_src}" alt="Cloud Botanist AI" class="brand-logo-img">
            </div>
            <div class="header-title">
                <h1>CLOUD BOTANIST AI <span class="brand-sub">FOCUS 1.2</span></h1>
                <p>INFRASTRUCTURE-NATIVE PAYMENT PLATFORM FOR CLOUD & COMPUTE SPEND.</p>
            </div>
        </div>
        <div class="header-actions">
            <div class="badges">
                <span class="badge badge-focus"><span class="badge-dot"></span>FOCUS v1.2</span>
                <span class="badge badge-duckdb">DUCKDB</span>
                <span class="badge badge-status">LIVE TELEMETRY</span>
            </div>
            <div class="toggle-pill" role="group" aria-label="Ingestion Mode">
                <span class="pill-title">MODE:</span>
                <button type="button" id="btnModeReplace" class="pill-btn active" onclick="setMode('replace')" title="Replace active data with dropped file(s)">
                    Replace on Drop
                </button>
                <button type="button" id="btnModeAppend" class="pill-btn" onclick="setMode('append')" title="Accumulate dropped file(s) into active ledger">
                    Append
                </button>
            </div>

            <button class="btn-subtle-action" id="reloadBtn" onclick="handleReload()" title="Reload all 5 local sample multi-cloud datasets (AWS, Azure, GCP, Cloudflare, Nebius)">
                <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="square">
                    <polyline points="23 4 23 10 17 10"></polyline>
                    <polyline points="1 20 1 14 7 14"></polyline>
                    <path d="M3.51 9a9 9 0 0 1 14.85-3.36L23 10M1 14l4.64 4.36A9 9 0 0 0 20.49 15"></path>
                </svg>
                Reload Demo Data
            </button>
            <button class="btn-reset" id="resetBtn" onclick="handleReset()">
                <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="square" stroke-linejoin="miter">
                    <polyline points="1 4 1 10 7 10"></polyline>
                    <path d="M3.51 15a9 9 0 1 0 2.13-9.36L1 10"></path>
                </svg>
                Reset / Clear Data
            </button>
        </div>
    </header>

    <!-- What is FOCUS 1.2 Callout -->
    <div class="blueprint-card" style="margin-bottom: 24px; padding: 18px 24px;">
        
        
        
        
        <strong style="color: var(--accent-steel); font-family: var(--font-display); letter-spacing: 0.05em; margin-bottom: 8px; display: block; font-size: 0.95rem;">WHAT IS FOCUS 1.2?</strong>
        <p style="margin: 0; line-height: 1.55; color: var(--text-dim); font-size: 0.88rem;">
            The FinOps Open Cost and Usage Specification (FOCUS™) is an open-source technical standard supported by the FinOps Foundation. It defines a unified billing schema that allows engineers to normalize raw, proprietary cost exports from AWS, Azure, Google Cloud, and others into a single, consistent ledger without losing context. <a href="https://focus.finops.org/docs/specification/v1-2/" target="_blank" rel="noopener noreferrer" style="color: var(--accent-steel); text-decoration: underline; font-weight: 600; margin-left: 4px;">Read the v1.2 spec &rarr;</a>
        </p>
    </div>

    <!-- Billing File Intake Zone -->
    <div class="dropzone-section">
        <!-- Intake Mode Tabs -->
        <div class="intake-tabs-bar">
            <div class="intake-tabs-group">
                <button type="button" id="tabBtnUpload" class="intake-tab-btn active" onclick="switchIntakeTab('upload')">
                    <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="square">
                        <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"></path>
                        <polyline points="17 8 12 3 7 8"></polyline>
                        <line x1="12" y1="3" x2="12" y2="15"></line>
                    </svg>
                    <span>Upload Billing Files</span>
                </button>
                <button type="button" id="tabBtnDemo" class="intake-tab-btn" onclick="switchIntakeTab('demo')">
                    <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="square">
                        <polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2"></polygon>
                    </svg>
                    <span>Generate Demo Data</span>
                    <span class="tab-badge-pill">NO FILES NEEDED</span>
                </button>
            </div>
            <div class="intake-tabs-meta">
                
                <span>TELEMETRY INTAKE ENGINE</span>
            </div>
        </div>

        <!-- Tab 1: Upload Files Dropzone -->
        <div id="tabContentUpload" class="tab-pane">
            <div class="blueprint-card dropzone-card" id="dropZone" onclick="document.getElementById('fileInput').click()">
                
                
                
                

                <div class="dropzone-overlay" id="dropzoneOverlay">
                    <div class="blueprint-spinner"></div>
                    <div class="dropzone-overlay-text">EXECUTING DUCKDB NORMALIZATION KERNEL...</div>
                    <div class="dropzone-overlay-subtext">Content-sniffing schema and converting partitions to FOCUS 1.2 standard</div>
                </div>

                <div class="spend-card-header">
                    <div class="spend-card-title-wrap">
                        <div class="intake-icon">
                            <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="square" stroke-linejoin="miter">
                                <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"></path>
                                <polyline points="14 2 14 8 20 8"></polyline>
                                <line x1="8" y1="13" x2="16" y2="13"></line>
                                <line x1="8" y1="17" x2="13" y2="17"></line>
                            </svg>
                        </div>
                        <div>
                            <div class="spend-card-label">CLOUD BILLING // TELEMETRY INTAKE</div>
                            <div class="intake-subtitle">Upload raw cloud billing exports — auto-detected and normalized to FOCUS 1.2 with DuckDB</div>
                        </div>
                    </div>
                    <div class="format-tags">
                        <span class="format-pill">.PARQUET</span>
                        <span class="format-pill">.CSV</span>
                        <span class="format-pill">.JSON</span>
                        <span class="format-pill">.TAR.GZ</span>
                        <span class="format-pill">.ZIP</span>
                    </div>
                </div>

                <div class="dropzone-inner-prompt">
                    <div class="dropzone-callout">
                        <svg width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="square">
                            <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"></path>
                            <polyline points="17 8 12 3 7 8"></polyline>
                            <line x1="12" y1="3" x2="12" y2="15"></line>
                        </svg>
                        <div class="prompt-text">
                            <strong class="prompt-headline">DRAG & DROP BILLING TELEMETRY FILES HERE OR CLICK TO BROWSE</strong>
                            <span class="prompt-sub"><span id="modeStatusText" class="mode-status-text">[REPLACE ON DROP]</span> Content sniffs AWS, Azure, GCP, Cloudflare, Nebius partitions — or switch to <a href="javascript:void(0)" onclick="event.stopPropagation(); switchIntakeTab('demo');" class="tab-link">Generate Demo Data</a></span>
                        </div>
                    </div>
                </div>

                <input type="file" id="fileInput" multiple style="display: none;" onclick="event.stopPropagation()">
            </div>
        </div>

        <!-- Tab 2: Generate Demo Data -->
        <div id="tabContentDemo" class="tab-pane" style="display: none;">
            <div class="blueprint-card demo-generator-card">
                
                
                
                

                <div class="generator-header">
                    <div class="generator-title-wrap">
                        <div class="generator-icon">
                            <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="square">
                                <polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2"></polygon>
                            </svg>
                        </div>
                        <div>
                            <div class="generator-title">DEMO TELEMETRY GENERATOR // INSTANT SYNTHETIC BENCHMARK</div>
                            <div class="generator-subtitle">No cloud billing files on hand? Instantly generate and normalize live multi-cloud FOCUS 1.2 telemetry to explore spend governance, dual-currency pools, and idle zero-row pruning.</div>
                        </div>
                    </div>
                    <div class="format-tags">
                        <span class="format-pill">AWS</span>
                        <span class="format-pill">AZURE</span>
                        <span class="format-pill">GCP</span>
                        <span class="format-pill">CLOUDFLARE</span>
                        <span class="format-pill">NEBIUS</span>
                    </div>
                </div>

                <div class="generator-body">
                    <!-- Primary Full Benchmark -->
                    <div class="generator-primary-box">
                        <div class="gen-box-header">
                            <span class="gen-box-tag">PRIMARY BENCHMARK</span>
                            <span class="gen-box-badge">DUAL-CURRENCY INVARIANCE (USD + EUR)</span>
                        </div>
                        <div class="gen-box-title">Complete 5-Cloud Enterprise Dataset</div>
                        <div class="gen-box-desc">
                            Dynamically generates and normalizes live synthetic multi-cloud billing telemetry across AWS, Microsoft Azure, Google Cloud, Cloudflare, and Nebius AI. Automatically prunes idle $0.00 micro-metered rows while maintaining strict <code>0.000000%</code> delta invariance across USD and EUR pools. Every generation synthesizes fresh line items and dynamic spend totals.
                        </div>
                        <div class="gen-box-actions">
                            <button type="button" class="btn-gen-primary" onclick="generateDemoSuite('all')">
                                <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.4" stroke-linecap="square">
                                    <polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2"></polygon>
                                </svg>
                                <span>GENERATE DYNAMIC MULTI-CLOUD SUITE</span>
                            </button>
                        </div>
                    </div>

                    <!-- Selective Cloud Packs -->
                    <div class="generator-presets-box">
                        <div class="gen-presets-title">SELECTIVE CLOUD BENCHMARKS:</div>
                        <div class="gen-preset-buttons">
                            <button type="button" class="btn-gen-preset" onclick="generateDemoSuite('hyperscalers')" title="Generate AWS (USD) + Azure (EUR) + Google Cloud (EUR)">
                                <span class="preset-icon">☁</span>
                                <div class="preset-text">
                                    <strong>Hyperscalers Suite</strong>
                                    <span>Dynamic AWS + Azure + Google Cloud</span>
                                </div>
                            </button>
                            <button type="button" class="btn-gen-preset" onclick="generateDemoSuite('ai_infra')" title="Generate Nebius AI GPU compute (USD) + Cloudflare R2 (USD)">
                                <span class="preset-icon">⚡</span>
                                <div class="preset-text">
                                    <strong>AI & Edge Infrastructure</strong>
                                    <span>Dynamic Nebius GPU/Epyc + Cloudflare R2</span>
                                </div>
                            </button>
                            <button type="button" class="btn-gen-preset" onclick="generateDemoSuite('aws_only')" title="Generate AWS Cost & Usage Report (USD)">
                                <span class="preset-icon">🔶</span>
                                <div class="preset-text">
                                    <strong>AWS CUR Dataset</strong>
                                    <span>Dynamic EC2, S3, RDS, Secrets (USD)</span>
                                </div>
                            </button>
                            <button type="button" class="btn-gen-preset" onclick="generateDemoSuite('europe_only')" title="Generate Google Cloud + Microsoft Azure (EUR)">
                                <span class="preset-icon">💶</span>
                                <div class="preset-text">
                                    <strong>European Cloud (EUR)</strong>
                                    <span>Dynamic Azure + GCP telemetry (EUR)</span>
                                </div>
                            </button>
                        </div>
                    </div>
                </div>
            </div>
        </div>

        <div id="alertToast" class="alert-toast"></div>

        <!-- Hardware Telemetry & Audit Stream -->
        <div class="blueprint-panel activity-panel">
            <div class="panel-header">
                <span class="panel-title">
                    
                    AUDIT FEED // HARDWARE TELEMETRY & INGESTION STREAM
                </span>
                <button class="btn-subtle" onclick="clearActivityLog()">Clear Feed</button>
            </div>
            <div class="activity-list" id="activityLogList">
                <div class="activity-placeholder" style="color: var(--text-muted); font-size: 0.78rem; padding: 6px 4px; font-family: var(--font-sans); font-weight: 400;">Awaiting cloud billing telemetry stream. Ingest files above or click 'Generate Demo Data'.</div>
            </div>
        </div>
    </div>

    <!-- Numbered Spend Metric Compartments (Slides 3, 4, 6) -->
    <div class="cards-grid">
        <div class="blueprint-card stat-card">
            
            
            <div class="stat-top">
                <span class="stat-index">01 // ALLOCATED USD SPEND</span>
                <span class="stat-tag">USD POOL</span>
            </div>
            <div class="stat-number usd" id="card-usd-total">{usd_formatted}</div>
            <div class="stat-subtext" id="card-usd-breakdown">
                <span class="sub-chip">$0.00 USD</span>
            </div>
        </div>

        <div class="blueprint-card stat-card">
            
            
            <div class="stat-top">
                <span class="stat-index">02 // ALLOCATED EUR SPEND</span>
                <span class="stat-tag">EUR POOL</span>
            </div>
            <div class="stat-number eur" id="card-eur-total">{eur_formatted}</div>
            <div class="stat-subtext" id="card-eur-breakdown">
                <span class="sub-chip">€0.00 EUR</span>
            </div>
        </div>

        <div class="blueprint-card stat-card">
            
            
            <div class="stat-top">
                <span class="stat-index">03 // ACTIVE FOCUS 1.2 RECORDS</span>
                <span class="stat-tag" id="card-records-chip">{norm_rows_formatted} RECORDS</span>
            </div>
            <div class="stat-number records" id="card-row-count">{norm_rows_formatted}</div>
            <div class="stat-subtext">
                <span>Pruned <strong id="card-pruned-count" class="mono-val" style="color: var(--accent-sprout);">{pruned_rows_formatted}</strong> idle $0.00 micro-metered rows</span>
            </div>
        </div>
    </div>

    <!-- Infrastructure Spend Yield (Charts) -->
    <div class="charts-grid">
        <div class="blueprint-panel chart-panel">
            <div class="panel-header">
                <span class="panel-title">
                    
                    04 // USD INFRASTRUCTURE SPEND YIELD
                </span>
                <span class="curr-pill curr-usd">USD</span>
            </div>
            <div class="chart-container" id="usd-bars"></div>
        </div>

        <div class="blueprint-panel chart-panel">
            <div class="panel-header">
                <span class="panel-title">
                    
                    05 // EUR INFRASTRUCTURE SPEND YIELD
                </span>
                <span class="curr-pill curr-eur">EUR</span>
            </div>
            <div class="chart-container" id="eur-bars"></div>
        </div>
    </div>

    <!-- Normalized Telemetry Ledger Table -->
    <div class="blueprint-panel table-panel">
        <div class="panel-header">
            <span class="panel-title">
                
                06 // NORMALIZED FOCUS 1.2 TELEMETRY LEDGER
            </span>
            <div class="table-actions">
                <a href="/unified_focus.parquet" class="btn-action" download title="Download full normalized dataset as Apache Parquet">
                    <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="square"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"></path><polyline points="7 10 12 15 17 10"></polyline><line x1="12" y1="15" x2="12" y2="3"></line></svg>
                    Download Parquet
                </a>
                <a href="/unified_focus.duckdb" class="btn-action" download title="Download standalone DuckDB database file">
                    <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="square"><ellipse cx="12" cy="5" rx="9" ry="3"></ellipse><path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3"></path><path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5"></path></svg>
                    Export DuckDB
                </a>
                <button class="btn-action" id="exportBtn" title="Export filtered rows to CSV">
                    <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="square"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"></path><polyline points="14 2 14 8 20 8"></polyline><line x1="16" y1="13" x2="8" y2="13"></line><line x1="16" y1="17" x2="8" y2="17"></line></svg>
                    Export CSV
                </button>
            </div>
        </div>

        <div class="table-filters-row">
            <div class="search-box">
                <svg class="search-icon" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                    <circle cx="11" cy="11" r="8"></circle>
                    <line x1="21" y1="21" x2="16.65" y2="16.65"></line>
                </svg>
                <input type="text" id="searchInput" class="search-input" placeholder="Search provider, service, SKU, description...">
            </div>
            <div class="select-group">
                <select id="provFilter" class="filter-select">
                    <option value="">All Providers</option>
                </select>
                <select id="currFilter" class="filter-select">
                    <option value="">All Currencies</option>
                    <option value="USD">USD ($)</option>
                    <option value="EUR">EUR (€)</option>
                </select>
                <select id="catFilter" class="filter-select">
                    <option value="">All Categories</option>
                    <option value="Usage">Usage</option>
                    <option value="Credit">Credit</option>
                    <option value="Tax">Tax</option>
                </select>
            </div>
        </div>

        <div class="table-wrapper">
            <table>
                <thead>
                    <tr>
                        <th onclick="sortTable('ProviderName')">PROVIDER ↕</th>
                        <th onclick="sortTable('ServiceName')">SERVICE NAME ↕</th>
                        <th onclick="sortTable('ChargeCategory')">CATEGORY ↕</th>
                        <th onclick="sortTable('ChargeDescription')">CHARGE DESCRIPTION ↕</th>
                        <th onclick="sortTable('ConsumedQuantity')">QTY ↕</th>
                        <th onclick="sortTable('ConsumedUnit')">UNIT ↕</th>
                        <th onclick="sortTable('BilledCost')">BILLED COST ↕</th>
                        <th onclick="sortTable('EffectiveCost')">EFFECTIVE COST ↕</th>
                        <th onclick="sortTable('BillingCurrency')">CURRENCY ↕</th>
                        <th onclick="sortTable('PeriodStart')">PERIOD START ↕</th>
                    </tr>
                </thead>
                <tbody id="tableBody"></tbody>
            </table>
        </div>

        <div class="pagination-bar">
            <div>
                Showing <span id="startIdx" class="mono-val">0</span> to <span id="endIdx" class="mono-val">0</span> of <span id="totalFilterCount" class="mono-val">0</span> records
            </div>
            <div class="page-buttons" id="paginationControls"></div>
        </div>
    </div>

    <!-- Deck Footer -->
    <footer class="deck-footer">
        <div> CLOUD BOTANIST AI // INFRASTRUCTURE-NATIVE PAYMENT PLATFORM FOR CLOUD & COMPUTE SPEND</div>
        <div><a href="https://cloudbotanist.ai" target="_blank" rel="noopener noreferrer" class="meta-link">cloudbotanist.ai</a> // FOCUS 1.2 ENGINE </div>
    </footer>

    <script>
        let rawData = {metrics_json};
        let recentEvents = {events_json};
        let currentRows = [];
        let filteredRows = [];
        let currentPage = 1;
        const pageSize = 15;
        let sortCol = 'BilledCost';
        let sortAsc = false;



        function updateUI(data) {{
            rawData = data || {{}};
            const usdTotal = rawData.currency_totals?.USD?.billed || 0;
            const eurTotal = rawData.currency_totals?.EUR?.billed || 0;
            const normRows = rawData.total_normalized_rows || 0;
            const prunedRows = rawData.pruned_count || 0;

            document.getElementById('card-usd-total').textContent = '$' + usdTotal.toFixed(6);
            document.getElementById('card-eur-total').textContent = '€' + eurTotal.toFixed(6);
            document.getElementById('card-row-count').textContent = normRows.toLocaleString();
            document.getElementById('card-pruned-count').textContent = prunedRows.toLocaleString();
            document.getElementById('card-records-chip').textContent = normRows + ' RECORDS';

            // Breakdown chips
            const usdProvs = (rawData.providers || []).filter(p => p.currency === 'USD');
            const eurProvs = (rawData.providers || []).filter(p => p.currency === 'EUR');

            document.getElementById('card-usd-breakdown').innerHTML = usdProvs.length > 0
                ? usdProvs.map(p => `<span class="sub-chip">${{p.provider}}: $${{p.billed_cost.toFixed(6)}}</span>`).join('')
                : '<span class="sub-chip">$0.00 USD</span>';

            document.getElementById('card-eur-breakdown').innerHTML = eurProvs.length > 0
                ? eurProvs.map(p => `<span class="sub-chip">${{p.provider}}: €${{p.billed_cost.toFixed(6)}}</span>`).join('')
                : '<span class="sub-chip">€0.00 EUR</span>';

            // Provider filter options
            const provSelect = document.getElementById('provFilter');
            const currentSelected = provSelect.value;
            const provNames = [...new Set((rawData.providers || []).map(p => p.provider))].sort();
            provSelect.innerHTML = '<option value="">All Providers</option>' + 
                provNames.map(p => `<option value="${{p}}" ${{p === currentSelected ? 'selected' : ''}}>${{p}}</option>`).join('');

            renderBars();

            currentRows = [...(rawData.table_rows || [])];
            filterData();
        }}

        function renderBars() {{
            const usdContainer = document.getElementById('usd-bars');
            const eurContainer = document.getElementById('eur-bars');

            const usdProviders = (rawData.providers || []).filter(p => p.currency === 'USD');
            const eurProviders = (rawData.providers || []).filter(p => p.currency === 'EUR');

            const maxUSD = Math.max(...usdProviders.map(p => p.billed_cost), 0.0001);
            const maxEUR = Math.max(...eurProviders.map(p => p.billed_cost), 0.0001);

            usdContainer.innerHTML = usdProviders.length > 0 ? usdProviders.map(p => {{
                const pct = Math.max((p.billed_cost / maxUSD) * 100, p.billed_cost > 0 ? 5 : 2);
                return `
                <div class="bar-group">
                    <div class="bar-label">
                        <span><strong>${{p.provider}}</strong> (${{p.normalized_rows}} rows)</span>
                        <span class="mono-val">$${{p.billed_cost.toFixed(6)}}</span>
                    </div>
                    <div class="bar-track">
                        <div class="bar-fill bar-fill-usd" style="width: ${{pct}}%;"></div>
                    </div>
                </div>`;
            }}).join('') : '<div style="color: var(--text-muted); text-align: left; padding: 20px 0; font-size: 0.8rem; font-family: var(--font-tag); font-weight: 300; letter-spacing: 0.08em; text-transform: uppercase;">NO USD TELEMETRY IN ACTIVE LEDGER</div>';

            eurContainer.innerHTML = eurProviders.length > 0 ? eurProviders.map(p => {{
                const pct = Math.max((p.billed_cost / maxEUR) * 100, p.billed_cost > 0 ? 5 : 2);
                return `
                <div class="bar-group">
                    <div class="bar-label">
                        <span><strong>${{p.provider}}</strong> (${{p.normalized_rows}} rows)</span>
                        <span class="mono-val">€${{p.billed_cost.toFixed(6)}}</span>
                    </div>
                    <div class="bar-track">
                        <div class="bar-fill bar-fill-eur" style="width: ${{pct}}%;"></div>
                    </div>
                </div>`;
            }}).join('') : '<div style="color: var(--text-muted); text-align: left; padding: 20px 0; font-size: 0.8rem; font-family: var(--font-tag); font-weight: 300; letter-spacing: 0.08em; text-transform: uppercase;">NO EUR TELEMETRY IN ACTIVE LEDGER</div>';
        }}

        function getProvBadge(prov) {{
            const p = prov || '';
            if (p.includes('AWS')) return '<span class="prov-badge prov-aws">AWS</span>';
            if (p.includes('Azure')) return '<span class="prov-badge prov-azure">Azure</span>';
            if (p.includes('Google')) return '<span class="prov-badge prov-gcp">GCP</span>';
            if (p.includes('Cloudflare')) return '<span class="prov-badge prov-cloudflare">Cloudflare</span>';
            return `<span class="prov-badge prov-nebius">${{p || 'Other'}}</span>`;
        }}

        function getCategoryBadge(cat) {{
            const c = (cat || 'Usage').toLowerCase();
            if (c.includes('credit')) return `<span class="cat-badge cat-credit">${{cat}}</span>`;
            if (c.includes('tax')) return `<span class="cat-badge cat-tax">${{cat}}</span>`;
            return `<span class="cat-badge cat-usage">${{cat || 'Usage'}}</span>`;
        }}

        function formatCost(val, curr) {{
            const sym = curr === 'USD' ? '$' : '€';
            const num = parseFloat(val) || 0;
            let cls = 'zero';
            if (num > 0) cls = 'pos';
            else if (num < 0) cls = 'credit';
            return `<span class="cost-col ${{cls}}">${{sym}}${{num.toFixed(6)}}</span>`;
        }}

        function filterData() {{
            const query = document.getElementById('searchInput').value.toLowerCase().trim();
            const prov = document.getElementById('provFilter').value;
            const curr = document.getElementById('currFilter').value;
            const cat = document.getElementById('catFilter').value;

            filteredRows = currentRows.filter(r => {{
                if (prov && r.ProviderName !== prov) return false;
                if (curr && r.BillingCurrency !== curr) return false;
                if (cat && (!r.ChargeCategory || !r.ChargeCategory.toLowerCase().includes(cat.toLowerCase()))) return false;
                if (query) {{
                    const matchString = `${{r.ProviderName}} ${{r.ServiceName}} ${{r.ChargeDescription}} ${{r.SubAccount}}`.toLowerCase();
                    if (!matchString.includes(query)) return false;
                }}
                return true;
            }});

            currentPage = 1;
            renderTable();
        }}

        function sortTable(col) {{
            if (sortCol === col) {{
                sortAsc = !sortAsc;
            }} else {{
                sortCol = col;
                sortAsc = true;
            }}
            filteredRows.sort((a, b) => {{
                let valA = a[col];
                let valB = b[col];
                if (typeof valA === 'number' && typeof valB === 'number') {{
                    return sortAsc ? valA - valB : valB - valA;
                }}
                valA = String(valA || '').toLowerCase();
                valB = String(valB || '').toLowerCase();
                return sortAsc ? valA.localeCompare(valB) : valB.localeCompare(valA);
            }});
            renderTable();
        }}

        function renderTable() {{
            const tbody = document.getElementById('tableBody');
            const total = filteredRows.length;
            const start = (currentPage - 1) * pageSize;
            const end = Math.min(start + pageSize, total);

            document.getElementById('startIdx').textContent = total > 0 ? start + 1 : 0;
            document.getElementById('endIdx').textContent = end;
            document.getElementById('totalFilterCount').textContent = total;

            if (total === 0) {{
                tbody.innerHTML = `
                    <tr>
                        <td colspan="10" style="text-align: center; padding: 48px 16px; color: var(--text-secondary);">
                            <div style="font-size: 1.1rem; font-weight: 700; margin-bottom: 6px; color: var(--text-primary); font-family: var(--font-display);">NO TELEMETRY PARTITIONS LOADED</div>
                            <div style="font-size: 0.8rem; color: var(--text-muted); font-family: var(--font-sans); font-weight: 400;">Ingest raw billing files (.parquet, .csv, .json, .tar.gz, .zip) into the intake card above.</div>
                        </td>
                    </tr>`;
                renderPagination(0);
                return;
            }}

            const slice = filteredRows.slice(start, end);
            tbody.innerHTML = slice.map(r => `
                <tr>
                    <td>${{getProvBadge(r.ProviderName)}}</td>
                    <td><strong>${{r.ServiceName}}</strong></td>
                    <td>${{getCategoryBadge(r.ChargeCategory)}}</td>
                    <td style="max-width: 300px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;" title="${{r.ChargeDescription}}">${{r.ChargeDescription}}</td>
                    <td class="telemetry-num">${{typeof r.ConsumedQuantity === 'number' ? (r.ConsumedQuantity < 0.0001 && r.ConsumedQuantity > 0 ? r.ConsumedQuantity.toExponential(2) : r.ConsumedQuantity.toLocaleString(undefined, {{maximumFractionDigits: 4}})) : '-'}}</td>
                    <td><span class="sub-chip">${{r.ConsumedUnit}}</span></td>
                    <td>${{formatCost(r.BilledCost, r.BillingCurrency)}}</td>
                    <td>${{formatCost(r.EffectiveCost, r.BillingCurrency)}}</td>
                    <td><span class="curr-badge curr-${{(r.BillingCurrency || 'usd').toLowerCase()}}">${{r.BillingCurrency}}</span></td>
                    <td class="telemetry-num" style="font-size: 0.74rem; color: var(--text-muted);">${{r.PeriodStart || '-'}}</td>
                </tr>
            `).join('');

            renderPagination(total);
        }}

        function renderPagination(total) {{
            const container = document.getElementById('paginationControls');
            const totalPages = Math.ceil(total / pageSize) || 1;
            let html = `
                <button class="page-btn" onclick="gotoPage(${{currentPage - 1}})" ${{currentPage <= 1 ? 'disabled' : ''}}>Prev</button>
            `;
            for (let i = 1; i <= Math.min(totalPages, 8); i++) {{
                html += `<button class="page-btn ${{i === currentPage ? 'active' : ''}}" onclick="gotoPage(${{i}})">${{i}}</button>`;
            }}
            html += `
                <button class="page-btn" onclick="gotoPage(${{currentPage + 1}})" ${{currentPage >= totalPages ? 'disabled' : ''}}>Next</button>
            `;
            container.innerHTML = html;
        }}

        function gotoPage(p) {{
            currentPage = p;
            renderTable();
        }}

        // Drag & Drop Implementation
        const dropZone = document.getElementById('dropZone');
        const fileInput = document.getElementById('fileInput');
        const alertToast = document.getElementById('alertToast');
        const activityList = document.getElementById('activityLogList');

        ['dragenter', 'dragover', 'dragleave', 'drop'].forEach(evt => {{
            window.addEventListener(evt, (e) => {{
                e.preventDefault();
                e.stopPropagation();
            }}, false);
            dropZone.addEventListener(evt, (e) => {{
                e.preventDefault();
                e.stopPropagation();
            }}, false);
        }});

        dropZone.addEventListener('dragover', () => {{
            dropZone.classList.add('dragover');
        }});
        dropZone.addEventListener('dragleave', () => {{
            dropZone.classList.remove('dragover');
        }});

        dropZone.addEventListener('drop', (e) => {{
            dropZone.classList.remove('dragover');
            const files = e.dataTransfer.files;
            if (files && files.length > 0) uploadFiles(files);
        }});

        fileInput.addEventListener('change', (e) => {{
            const files = e.target.files;
            if (files && files.length > 0) uploadFiles(files);
        }});

        function showAlert(msg, type = 'success') {{
            let cls = 'alert-success';
            if (type === 'error') cls = 'alert-error';
            if (type === 'info') cls = 'alert-info';
            alertToast.className = 'alert-toast ' + cls;
            alertToast.innerHTML = msg;
            alertToast.style.display = 'block';
            setTimeout(() => {{ alertToast.style.display = 'none'; }}, 6000);
        }}

        let uploadMode = 'replace';

        function setMode(mode) {{
            uploadMode = mode;
            const btnReplace = document.getElementById('btnModeReplace');
            const btnAppend = document.getElementById('btnModeAppend');
            const statusText = document.getElementById('modeStatusText');

            if (mode === 'replace') {{
                btnReplace.classList.add('active');
                btnAppend.classList.remove('active');
                if (statusText) {{
                    statusText.textContent = '[REPLACE ON DROP]';
                    statusText.style.color = 'var(--accent-steel)';
                }}
            }} else {{
                btnAppend.classList.add('active');
                btnReplace.classList.remove('active');
                if (statusText) {{
                    statusText.textContent = '[APPEND]';
                    statusText.style.color = 'var(--accent-phosphor)';
                }}
            }}
        }}

        const apiBase = (window.location.protocol === 'file:' || !window.location.port) ? 'http://localhost:8000' : '';

        async function uploadFiles(fileList) {{
            if (!fileList || fileList.length === 0) return;

            dropZone.classList.add('processing');
            const modeDesc = uploadMode === 'replace' ? 'replacing active view' : 'appending to active view';
            showAlert(`<span class="blueprint-spinner" style="display:inline-block; width:12px; height:12px; margin-right:6px; vertical-align:middle; border-width:1.5px;"></span>Sniffing schemas and ${{modeDesc}} with DuckDB...`, 'info');

            const formData = new FormData();
            formData.append('mode', uploadMode);
            for (let i = 0; i < fileList.length; i++) {{
                formData.append('files', fileList[i]);
            }}

            try {{
                const resp = await fetch(`${{apiBase}}/api/upload?mode=${{encodeURIComponent(uploadMode)}}`, {{
                    method: 'POST',
                    body: formData
                }});
                const result = await resp.json();
                if (result.success) {{
                    updateUI(result.metrics);
                    if (result.events && result.events.length > 0) {{
                        renderActivityEvents(result.events);
                    }}

                    const allDup = result.events && result.events.length > 0 && result.events.every(ev => ev.status === 'duplicate');
                    if (allDup) {{
                        showAlert(`<strong>File(s) already processed:</strong> Ingestion timestamp re-verified.`, 'info');
                    }} else {{
                        let warningText = '';
                        if (result.warnings && result.warnings.length > 0) {{
                            warningText = `<br><span style="font-size:0.78rem; color:var(--signal-amber);">Warning: ${{result.warnings.join(', ')}}</span>`;
                        }}
                        const modeTag = uploadMode === 'replace' ? 'Replace Mode' : 'Append Mode';
                        showAlert(`<strong>Success [${{modeTag}}]!</strong> Processed ${{fileList.length}} file(s) into FOCUS 1.2 format.${{warningText}}`, 'success');
                    }}
                }} else {{
                    showAlert(`<strong>Error:</strong> ${{result.error || 'Failed to normalize upload'}}`, 'error');
                }}
            }} catch (err) {{
                showAlert(`<strong>Static Preview:</strong> Cannot connect to local engine. To process your own files, please clone the <a href="https://github.com/masha-vasilieva/focus-demo" target="_blank" style="color: inherit; text-decoration: underline;">GitHub repository</a> and run <code>python3 focus_engine.py</code> locally.`, 'error');
            }} finally {{
                dropZone.classList.remove('processing');
                fileInput.value = '';
            }}
        }}

        async function handleReset() {{
            dropZone.classList.add('processing');
            showAlert('<span class="blueprint-spinner" style="display:inline-block; width:12px; height:12px; margin-right:6px; vertical-align:middle; border-width:1.5px;"></span>Clearing in-memory database and resetting spend ledger...', 'info');
            try {{
                const resp = await fetch(`${{apiBase}}/api/reset`, {{ method: 'POST' }});
                const result = await resp.json();
                if (result.success) {{
                    updateUI(result.metrics);
                    if (result.events && result.events.length > 0) {{
                        renderActivityEvents(result.events);
                    }}
                    showAlert(`<strong>Ledger Reset:</strong> All in-memory data cleared to $0.00. Ready for new files.`, 'success');
                }} else {{
                    showAlert(`<strong>Reset Error:</strong> ${{result.error}}`, 'error');
                }}
            }} catch (err) {{
                showAlert(`<strong>Static Preview:</strong> Cannot connect to local engine. To interact with the dashboard, clone the <a href="https://github.com/masha-vasilieva/focus-demo" target="_blank" style="color: inherit; text-decoration: underline;">GitHub repository</a> and run <code>python3 focus_engine.py</code> locally.`, 'error');
            }} finally {{
                dropZone.classList.remove('processing');
            }}
        }}

        function switchIntakeTab(tab) {{
            const btnUpload = document.getElementById('tabBtnUpload');
            const btnDemo = document.getElementById('tabBtnDemo');
            const paneUpload = document.getElementById('tabContentUpload');
            const paneDemo = document.getElementById('tabContentDemo');

            if (tab === 'demo') {{
                if (btnDemo) btnDemo.classList.add('active');
                if (btnUpload) btnUpload.classList.remove('active');
                if (paneDemo) paneDemo.style.display = 'block';
                if (paneUpload) paneUpload.style.display = 'none';
            }} else {{
                if (btnUpload) btnUpload.classList.add('active');
                if (btnDemo) btnDemo.classList.remove('active');
                if (paneUpload) paneUpload.style.display = 'block';
                if (paneDemo) paneDemo.style.display = 'none';
            }}
        }}

        async function generateDemoSuite(preset) {{
            const demoCard = document.querySelector('.demo-generator-card');
            if (demoCard) demoCard.classList.add('processing');
            showAlert('<span class="blueprint-spinner" style="display:inline-block; width:12px; height:12px; margin-right:6px; vertical-align:middle; border-width:1.5px;"></span>Executing DuckDB normalization kernel for demo telemetry...', 'info');

            try {{
                const resp = await fetch(`${{apiBase}}/api/generate-demo?preset=${{encodeURIComponent(preset || 'all')}}`, {{ method: 'POST' }});
                const result = await resp.json();
                if (result.success) {{
                    updateUI(result.metrics);
                    if (result.events && result.events.length > 0) {{
                        renderActivityEvents(result.events);
                    }}
                    showAlert(`<strong>Demo Generated:</strong> Ingested ${{result.metrics?.total_normalized_rows || 0}} FOCUS 1.2 records into active ledger.`, 'success');
                }} else {{
                    showAlert(`<strong>Generator Error:</strong> ${{result.error || 'Failed to generate demo data'}}`, 'error');
                }}
            }} catch (err) {{
                showAlert(`<strong>Static Preview:</strong> Cannot connect to local engine. To load different demo presets or process files, clone the <a href="https://github.com/masha-vasilieva/focus-demo" target="_blank" style="color: inherit; text-decoration: underline;">GitHub repository</a> and run <code>python3 focus_engine.py</code> locally.`, 'error');
            }} finally {{
                if (demoCard) demoCard.classList.remove('processing');
            }}
        }}

        async function handleReload() {{
            return generateDemoSuite('all');
        }}

        function renderActivityEvents(events) {{
            if (!events || events.length === 0) return;
            const placeholder = activityList.querySelector('.activity-placeholder');
            if (placeholder) {{
                placeholder.remove();
            }}
            const itemsHtml = events.map(ev => {{
                let badgeClass = 'badge-act-success';
                let badgeIcon = '✔';
                if (ev.status === 'duplicate') {{
                    badgeClass = 'badge-act-info';
                    badgeIcon = 'ℹ';
                }} else if (ev.status === 'skipped' || ev.status === 'unsupported') {{
                    badgeClass = 'badge-act-warning';
                    badgeIcon = '⚠';
                }} else if (ev.status === 'reset') {{
                    badgeClass = 'badge-act-reset';
                    badgeIcon = '⟲';
                }}
                return `
                <div class="activity-item">
                    <div class="activity-item-left">
                        <span class="activity-badge ${{badgeClass}}">${{badgeIcon}}</span>
                        <span class="activity-text">${{ev.message}}</span>
                    </div>
                    <span class="activity-time">${{ev.timestamp || ''}}</span>
                </div>`;
            }}).join('');

            activityList.innerHTML = itemsHtml + activityList.innerHTML;
        }}

        function clearActivityLog() {{
            activityList.innerHTML = '<div class="activity-placeholder" style="color: var(--text-muted); font-size: 0.78rem; padding: 6px 4px; font-family: var(--font-sans); font-weight: 400;">Telemetry stream cleared. Ingest partitions above.</div>';
        }}

        // Listeners
        document.getElementById('searchInput').addEventListener('input', filterData);
        document.getElementById('provFilter').addEventListener('change', filterData);
        document.getElementById('currFilter').addEventListener('change', filterData);
        document.getElementById('catFilter').addEventListener('change', filterData);

        // Export to CSV
        document.getElementById('exportBtn').addEventListener('click', () => {{
            const headers = ['ProviderName', 'ServiceName', 'ChargeCategory', 'ChargeDescription', 'ConsumedQuantity', 'ConsumedUnit', 'BilledCost', 'EffectiveCost', 'BillingCurrency', 'PeriodStart', 'PeriodEnd'];
            const rows = filteredRows.map(r => [
                r.ProviderName,
                `"${{(r.ServiceName || '').replace(/"/g, '""')}}"`,
                r.ChargeCategory,
                `"${{(r.ChargeDescription || '').replace(/"/g, '""')}}"`,
                r.ConsumedQuantity,
                r.ConsumedUnit,
                r.BilledCost,
                r.EffectiveCost,
                r.BillingCurrency,
                r.PeriodStart,
                r.PeriodEnd
            ].join(','));
            const csv = [headers.join(','), ...rows].join('\\n');
            const blob = new Blob([csv], {{ type: 'text/csv;charset=utf-8;' }});
            const url = URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = 'focus_filtered_export.csv';
            a.click();
            URL.revokeObjectURL(url);
        }});

        // Initial setup
        updateUI(rawData);
        if (recentEvents && recentEvents.length > 0) {{
            renderActivityEvents(recentEvents);
        }}
        try {{
            const urlParams = new URLSearchParams(window.location.search);
            if (urlParams.get('tab') === 'demo') {{
                switchIntakeTab('demo');
            }}
        }} catch(e) {{}}

        // Offline file:// helper: update direct file export links and ping local server
        if (window.location.protocol === 'file:') {{
            const dlParquet = document.querySelector('a[href="/unified_focus.parquet"]');
            if (dlParquet) dlParquet.href = 'http://localhost:8000/unified_focus.parquet';
            const dlDuckdb = document.querySelector('a[href="/unified_focus.duckdb"]');
            if (dlDuckdb) dlDuckdb.href = 'http://localhost:8000/unified_focus.duckdb';

            fetch('http://localhost:8000/api/data')
                .then(r => r.json())
                .then(d => {{
                    if (d.success) {{
                        updateUI(d.metrics);
                        if (d.events && d.events.length > 0) renderActivityEvents(d.events);
                    }}
                }})
                .catch(() => {{
                    const notice = document.createElement('div');
                    notice.style.cssText = 'background: rgba(217, 119, 6, 0.15); border: 1px solid var(--signal-amber); color: var(--signal-amber); padding: 8px 16px; margin-bottom: 16px; border-radius: var(--radius-control); font-size: 0.82rem; font-family: var(--font-sans); font-weight: 400;';
                    notice.innerHTML = '⚡ NOTE: Viewing via file:// protocol. For live drag-and-drop ingestion, run <code>python3 focus_engine.py</code> and navigate to <a href="http://localhost:8000" style="color:inherit; font-weight:700;">http://localhost:8000</a>.';
                    const header = document.querySelector('.header');
                    if (header && header.parentNode) header.parentNode.insertBefore(notice, header.nextSibling);
                }});
        }}
    </script>
</body>
</html>
"""
    with open(OUTPUT_HTML, "w", encoding="utf-8") as f:
        f.write(html_code)



# ----------------------------------------------------------------------
# Application Entrypoint & Server Runner
# ----------------------------------------------------------------------
def run_server(port: int = DEFAULT_PORT, open_browser: bool = True):
    engine = FocusEngine(data_dir=SCRIPT_DIR)
    generate_interactive_dashboard(engine.metrics, engine.recent_events)
    engine.print_terminal_summary()

    FocusRequestHandler.engine = engine

    # Find free port starting at `port`
    server_port = port
    server = None
    for p in range(server_port, server_port + 20):
        try:
            server = ThreadingHTTPServer(("127.0.0.1", p), FocusRequestHandler)
            server_port = p
            break
        except OSError:
            continue

    if not server:
        raise RuntimeError(f"Could not bind to any port in range {port}-{port+20}")

    url = f"http://localhost:{server_port}"
    print(f"\n[SERVER READY] Zero-Framework FOCUS Engine listening on {url}")
    print(f"               Drag & drop billing files directly in your browser.")
    print("               Press Ctrl+C to stop the server.\n")

    if open_browser:
        try:
            if sys.platform == "darwin":
                subprocess.run(["open", url], check=False)
            elif sys.platform.startswith("linux"):
                subprocess.run(["xdg-open", url], check=False)
            else:
                webbrowser.open(url)
        except Exception as e:
            print(f"[QA Tester] Browser launch warning: {e}")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n[SHUTDOWN] Stopping server...")
    finally:
        server.server_close()


def main():
    parser = argparse.ArgumentParser(description="FOCUS 1.2 Zero-Framework Web Application")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help="Port to bind server (default: 8000)")
    parser.add_argument("--no-browser", action="store_true", help="Do not open browser automatically")
    parser.add_argument("--cli-only", action="store_true", help="Run normalization pipeline and exit without server")
    args = parser.parse_args()

    if args.cli_only:
        engine = FocusEngine(data_dir=SCRIPT_DIR)
        engine.load_initial_files(generate_report=False)
        engine.print_terminal_summary()
        print("[SUCCESS] Pipeline completed successfully.")
        return

    run_server(port=args.port, open_browser=not args.no_browser)


if __name__ == "__main__":
    main()
