# Project Instructions & Operating Rules

## Git & GitHub Version Control Workflow

### Mandatory Protocol: Commit & Push After Every Major Change
The agent must commit and push all changes to GitHub after completing every major change, feature implementation, refactoring, or bugfix.

#### Execution Steps:
1. **Verify Integrity**:
   - Run verification checks (e.g. `python3 focus_engine.py --cli-only` and any test suites) to ensure code compiles and all invariance tests pass before committing.
2. **Stage Targeted Files**:
   - Stage modified and newly created files using `git add <files>`.
   - Never stage temporary test artifacts, `.DS_Store`, or `__pycache__` (enforce `.gitignore`).
3. **Commit with Clear Rationale**:
   - Write a descriptive, conventional commit message detailing what changed and why (e.g., `feat: ...`, `fix: ...`, `refactor: ...`).
4. **Push to Remote**:
   - Push commits immediately to the tracking branch:
     ```bash
     git push origin main
     ```
   - Use the authenticated `gh` CLI session.
5. **Report to User**:
   - Mention the commit SHA and provide the direct GitHub commit link in the response.

---

## FOCUS 1.2 Spend Governance & Engine Invariants

1. **Dual-Currency Invariance**:
   - USD and EUR totals must NEVER be combined, converted, or conflated.
   - Maintain 0.000000% delta across currency boundaries.
2. **Zero-Framework Architecture**:
   - Pure Python standard library (`http.server`, `urllib`, `json`, `argparse`) + `duckdb`.
   - No external web frameworks (no Flask, FastAPI, Node.js).
3. **Dynamic Content Sniffing**:
   - Content and schema-based detection for `.parquet`, `.csv`, `.json`, `.tar.gz`, `.zip`.
4. **Visual Style**:
   - Adhere strictly to the `PITCHDECK.pdf` design system defined in `DESIGN.md` (Barlow Condensed typography, Denim Steel Blue `#4d749a`, architectural grid, `+` alignment crosshairs, programmable spend card dropzone).
