# Setup & Environment Guide — Cloud Botanist AI (FOCUS Engine)

This guide walks you through setting up and running **Cloud Botanist AI / FOCUS Engine** in a completely new, clean environment from scratch.

---

## 1. System Requirements

- **Operating System**: macOS, Linux (Ubuntu, Debian, Fedora, Arch, CentOS), or Windows (WSL2 or native PowerShell).
- **Python**: Version `3.9` or higher (`python3 --version`).
- **Dependencies**: Only 1 third-party dependency: **DuckDB** (`duckdb>=1.0.0`).
- **No External Web Frameworks Required**: Pure Python standard library (`http.server`, `urllib`, `json`, `argparse`). No Flask, FastAPI, or Node.js required.

---

## 2. Step-by-Step Installation

### Step 1: Clone the Repository
Open your terminal and clone the repository:

```bash
git clone https://github.com/masha-vasilieva/focus-demo.git
cd focus-demo
```

### Step 2: Create a Dedicated Python Virtual Environment (Recommended)
Isolate dependencies using Python's built-in `venv` module:

**macOS / Linux:**
```bash
python3 -m venv venv
source venv/bin/activate
```

**Windows (PowerShell):**
```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
```

**Windows (Command Prompt):**
```cmd
python -m venv venv
venv\Scripts\activate.bat
```

### Step 3: Install Required Dependencies
Install DuckDB using `pip`:

```bash
pip install -r requirements.txt
```

*Or install DuckDB directly:*
```bash
pip install duckdb
```

---

## 3. Running the Application

### Option A: Interactive Web Dashboard (Default)
Starts the local zero-framework HTTP server and automatically opens the dashboard in your default web browser:

```bash
python3 focus_engine.py
```

- **URL**: `http://localhost:8000`
- **Features**: Live drag-and-drop file ingestion, replace vs. append modes, live audit stream, spend yield charts, and columnar exports (DuckDB, Parquet, CSV).

### Option B: Headless / Remote Server Mode
If running on a remote server, EC2/VM, or cloud workspace where you do not want an automatic browser window opened:

```bash
python3 focus_engine.py --no-browser --port 8000
```

### Option C: Custom Port
If port `8000` is already in use by another application:

```bash
python3 focus_engine.py --port 8080
```

### Option D: Batch CLI-Only Mode (No Server)
To run the normalization pipeline, verify dual-currency invariance, and export `unified_focus.parquet`, `unified_focus.duckdb`, and `report.html` without running an HTTP server:

```bash
python3 focus_engine.py --cli-only
```

---

## 4. Verification & Testing

To confirm that your environment is properly configured and all normalization rules pass:

```bash
# 1. Run the engine verification
python3 focus_engine.py --cli-only

# 2. Inspect the generated outputs
ls -lh unified_focus.parquet unified_focus.duckdb report.html
```

Expected output:
- **`[PASSED] Invariance check for EUR: Delta = 0.000000000000`**
- **`[PASSED] Invariance check for USD: Delta = 0.000000000000`**
- **`[SUCCESS] Pipeline completed successfully.`**

---

## 5. Ingesting Billing Files in the Browser

Once the server is running on `http://localhost:8000`:

1. **Drag and Drop**: Drag raw cloud billing partitions directly into the top intake zone:
   - AWS CUR: `AWSDemoReport-00001.snappy.parquet`
   - Azure Cost Details: `AZUREpart_0_0001.snappy.parquet`
   - Google Cloud UI Export: `GCP_cost_table.csv`
   - Nebius Billing Archive: `NEBIUSnbs.tar.gz`
   - Cloudflare GraphQL JSON: `cloudflare.json`
2. **Ingestion Modes**:
   - **`[ Replace on Drop ]`** (Default): Replaces the active dataset with only the dropped file(s) for quick single-provider audits.
   - **`[ Append ]`**: Accumulates newly dropped files into the active spend ledger.
3. **Data Exports**:
   - **Download Parquet**: Download the active FOCUS 1.2 columnar dataset.
   - **Export DuckDB**: Export a standalone `.duckdb` relational database.
   - **Export CSV**: Export filtered table rows.
4. **Theme Toggle**:
   - Switch between **Blueprint Light** (default) and **Blueprint Dark** themes in the top bar.

---

## 6. Docker Container Deployment (Optional)

If you prefer running in a containerized environment, create a `Dockerfile`:

```dockerfile
FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8000
CMD ["python3", "focus_engine.py", "--no-browser", "--port", "8000"]
```

Build and run:
```bash
docker build -t focus-engine .
docker run -p 8000:8000 focus-engine
```

---

## 7. Troubleshooting

| Issue | Cause | Solution |
| :--- | :--- | :--- |
| `Address already in use` | Another process is listening on port 8000. | Run with `--port 8080` or kill the existing process: `lsof -ti :8000 \| xargs kill -9`. |
| `ModuleNotFoundError: No module named 'duckdb'` | DuckDB is not installed in the active Python environment. | Run `pip install duckdb` or check active virtual environment (`which python3`). |
| `0-row ghost partitions` | Azure export partitions contain metadata without row data. | Handled automatically: the engine inspects metadata row counts before querying. |
| `Tar archive decompression error` | Archive was renamed or corrupted. | The engine content-sniffs gzip headers and extracts files into an isolated temporary directory. |

---

## Need Assistance?

For questions, issues, or custom multi-cloud schema normalizers, visit [cloudbotanist.ai](https://cloudbotanist.ai).
