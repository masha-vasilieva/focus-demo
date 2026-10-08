# Cloud Botanist AI — FOCUS 1.2 Telemetry & Spend Governance Demo

![AWS](https://img.shields.io/badge/AWS-%23232F3E.svg?style=for-the-badge&logo=amazon-webservices&logoColor=white)
![Azure](https://img.shields.io/badge/azure-%230072C6.svg?style=for-the-badge&logo=microsoftazure&logoColor=white)
![Google Cloud](https://img.shields.io/badge/GoogleCloud-%234285F4.svg?style=for-the-badge&logo=google-cloud&logoColor=white)
![Cloudflare](https://img.shields.io/badge/Cloudflare-F38020?style=for-the-badge&logo=Cloudflare&logoColor=white)
![Nebius](https://img.shields.io/badge/Nebius_AI-black?style=for-the-badge)

> Infrastructure-Native Payment Platform for Cloud & Compute Spend.
> 📚 **[View the Cloud Botanist Pitch Deck (PDF)](PITCHDECK.pdf)**

A lightweight, framework-free tool built with Python and DuckDB that cleans and normalizes multi-cloud billing data. It takes raw cost exports from AWS, Azure, GCP, Cloudflare, and Nebius, maps them to the FOCUS 1.2 standard, and features an interactive dashboard with drag-and-drop file uploads, dual-currency support (USD/EUR), and fast data exports.
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

> For complete step-by-step installation instructions for any new environment (macOS, Linux, Windows, or Docker), see **[SETUP.md](SETUP.md)**.

### Prerequisites
- Python 3.9+
- Install dependencies: `pip install -r requirements.txt` (DuckDB)

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
├── LICENSE                          # MIT open source license
├── SETUP.md                         # Clean environment installation & run guide
├── requirements.txt                 # Dependencies (DuckDB)
├── DESIGN.md                        # Visual design system specifications
├── PITCHDECK.pdf                    # Pitch deck reference
├── logo.png                         # Cloud Botanist AI branding asset
└── sample_data/                     # Sample datasets for multi-cloud normalization
    ├── AWSDemoReport-00001.snappy.parquet # Sample AWS billing partition
    ├── AZUREpart_0_0001.snappy.parquet  # Sample Azure billing partition
    ├── GCP_cost_table.csv               # Sample GCP billing table
    ├── NEBIUSnbs.tar.gz                 # Sample Nebius billing archive
    └── cloudflare.json                  # Sample Cloudflare GraphQL billing payload
```

---

## License

MIT License — see [LICENSE](LICENSE) for details.
