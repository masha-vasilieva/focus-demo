# Cloud Botanist AI — FOCUS 1.2 Telemetry & Spend Governance Demo

> Infrastructure-Native Payment Platform for Cloud & Compute Spend.

A zero-framework, standalone FinOps normalization engine built with **DuckDB** and Python's standard library. It ingests multi-cloud billing partitions (AWS, Azure, GCP, Cloudflare, Nebius), normalizes them into **FinOps Open Cost & Usage Specification (FOCUS™) 1.2** schema, and serves an interactive architectural blueprint dashboard with real-time drag-and-drop ingestion, dual-currency isolation (USD & EUR), and columnar data exports.

---

## Key Features

- **Multi-Cloud Content & Schema Sniffing**: Automatically identifies and ingests raw billing files:
  - AWS CUR (`.snappy.parquet`)
  - Azure Cost Details (`.snappy.parquet`, defensively skipping 0-row ghost partitions)
  - Google Cloud UI Export (`.csv`, handling credit negations and consumed units)
  - Cloudflare GraphQL JSON (`.json`, unnesting nested usage nodes)
  - Nebius Archive (`.tar.gz`, unpacking and reading internal billing CSVs)
- **FOCUS 1.2 Format Normalization**:
  - Full dimension alignment (`ProviderName`, `ServiceName`, `ChargeCategory`, `ChargeDescription`, `ConsumedQuantity`, `ConsumedUnit`, `BilledCost`, `EffectiveCost`, `BillingCurrency`, `PeriodStart`, `PeriodEnd`, etc.).
  - Automatic zero-spend micro-metered pruning ($0.00 idle rows).
  - Strict dual-currency isolation and invariant enforcement ($0.000000% delta across currency boundaries).
- **Interactive Pitchdeck Blueprint Dashboard**:
  - Designed in the visual language of the Cloud Botanist pitch deck (architectural grid, Barlow Condensed typography, corner crosshairs `+`, and symmetrical botanical antenna glyph).
  - Dual theme support: Blueprint Light (default) & Blueprint Dark.
  - Drag-and-drop intake zone with **Replace on Drop** (default) and **Append** modes.
  - Reset / Clear Data workflow for live demonstrations.
  - Live hardware telemetry audit feed.
- **Columnar Export Capabilities**:
  - Export to **Apache Parquet** (`unified_focus.parquet`).
  - Export to standalone **DuckDB** database file (`unified_focus.duckdb`).
  - Export filtered rows to **CSV**.

---

## Quickstart

### Prerequisites
- Python 3.9+
- DuckDB (`pip install duckdb`)

### Run the Engine & Dashboard

```bash
# Start the local server and launch the interactive dashboard
python3 focus_engine.py

# Or start on a custom port without auto-opening the browser
python3 focus_engine.py --port 8000 --no-browser

# Run batch normalization in CLI-only mode (generates unified_focus.parquet and report.html without server)
python3 focus_engine.py --cli-only
```

Once started, open `http://localhost:8000` in your browser. Drag and drop any raw cloud billing files directly onto the intake card to inspect normalized spend in real time.

---

## Repository Structure

```
focus-demo/
├── focus_engine.py                  # Standalone zero-framework engine & HTTP server
├── report.html                      # Interactive self-contained HTML dashboard
├── DESIGN.md                        # Visual design system specifications
├── PITCHDECK.pdf                    # Pitch deck reference
├── unified_focus.parquet            # Exported normalized FOCUS 1.2 dataset (Parquet)
├── unified_focus.duckdb             # Exported normalized FOCUS 1.2 database (DuckDB)
├── AWSDemoReport-00001.snappy.parquet # Sample AWS billing partition
├── AZUREpart_0_0001.snappy.parquet  # Sample Azure billing partition
├── GCP_cost_table.csv               # Sample GCP billing table
├── NEBIUSnbs.tar.gz                 # Sample Nebius billing archive
└── cloudflare.json                  # Sample Cloudflare GraphQL billing payload
```

---

## License

Apache-2.0
