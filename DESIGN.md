# Cloud Botanist AI — Pitchdeck Design System Specification (DESIGN.md)

## Brand Essence & Visual Archetype
Derived directly from **PITCHDECK.pdf** (V0.1; Sept; 2026), Cloud Botanist AI visualizes the junction of AI infrastructure orchestration and financial spend governance:
*"Infrastructure-Native Payment Platform for Cloud & Compute Spend."*

The visual language merges high-performance compute telemetry with clean Swiss architectural blueprint principles:
- Structural grid lines with `+` coordinate crosshairs at margins and intersections.
- The iconic geometric branching botanical tree / data antenna logo.
- High-contrast, condensed technical typography (`Barlow Condensed`) paired with monospaced telemetry (`JetBrains Mono`).
- Compartmentalized numbered metric blocks (`01`, `02`, `03`, `04`).
- Programmable payment card layout for compute spend rule governance (`rule: GPU burn <= budget`).

---

## Color Palette Architecture (Extracted from PITCHDECK.pdf)

### 1. Light Blueprint Palette (Dominant Deck Theme — Slides 1, 2, 3, 5, 6, 7)
- **Blueprint Paper Ground**: `#f3f4f2` (Warm Technical Slate / Chalk Gray)
- **Container Surfaces**: `#ffffff` (Crisp Pure White)
- **Elevated Surfaces**: `#f8f9f7`
- **Architectural Grid Lines**: `#dbe0e6` (Hairline blueprint rule)
- **Primary Type**: `#12161c` (Deep Technical Charcoal / Carbon)
- **Secondary Type**: `#687787` (Muted Technical Slate)
- **Signature Deck Accent**: `#4d749a` / `#5780a3` (Denim Steel Blue — exact color of the tree logo and TAM figures)
- **Sprout Accent**: `#10b981` (Chlorophyll Emerald for healthy invariant ledger state)
- **Signals**: `#f59e0b` (Amber Warning), `#f43f5e` (Critical Kill-Switch Ember)
- **Crosshairs**: `#8fa0b0` (`+` coordinate markers)
- **Perimeter Borders**: `1px solid #d8dde3`

### 2. Dark Blueprint Palette (Deck Problem & Contact Theme — Slides 4, 8, 9)
- **Blueprint Slate Ground**: `#182432` / `#16222f` (Deep Petrol Navy)
- **Container Surfaces**: `#1c2a38` (Deep Blueprint Surface)
- **Elevated Surfaces**: `#141e2a`
- **Architectural Grid Lines**: `#233446`
- **Primary Type**: `#f4f7fa`
- **Secondary Type**: `#8ca1b5`
- **Signature Deck Accent**: `#7da7cb` / `#8cb2d4` (Ice Steel Blue)
- **Perimeter Borders**: `1px solid #2a3c50`
- **Crosshairs**: `#4d6780` (`+` coordinate markers)

---

## Typography Hierarchy

| Role | Font Family | Weights | Usage & Characteristics |
| :--- | :--- | :--- | :--- |
| **Display & Headlines** | `Barlow Condensed`, sans-serif | `600`, `700`, `800` | High-impact condensed grotesque for deck titles, major KPI numbers, and card headings. |
| **Section Meta Labels** | `Inter`, `JetBrains Mono` | `700` | Uppercase, tracked (`letter-spacing: 0.12em`), e.g. `TELEMETRY // V0.1; SEPT; 2026`. |
| **Telemetry & Numbers** | `JetBrains Mono`, monospace | `500`, `700` | `font-feature-settings: 'tnum' 1, 'zero' 1;` for all live currency values, row counts, and dates. |
| **Narrative & Labels** | `Inter`, -apple-system, sans-serif | `400`, `500`, `600` | Clean, legible body copy, descriptions, and dropdown controls. |

---

## Key Design Patterns & Layout Constraints

1. **Architectural Grid & Crosshairs (`+`)**:
   - Backgrounds feature a subtle 80px technical blueprint grid.
   - Headers, section labels, and card corners display discrete `+` alignment crosshairs.
2. **Iconic Tree Logo**:
   - Symmetrical geometric tree glyph featuring a central vertical trunk with 3 tiers of branching limbs (45° diagonals, 90° uprights).
3. **Numbered Metric Compartments**:
   - Stat cards feature top-left index indicators (`01`, `02`, `03`, `04`) reflecting the structured slides of the deck.
4. **Programmable Compute Spend Card (Slide 7)**:
   - The dropzone is styled as the programmable card interface: chip outline, masked card number `•••• •••• •••• 2026`, and rule governance tag `rule: GPU burn <= budget`.
5. **Anti-Patterns**:
   - No diffuse drop shadows (clean 1px perimeter borders only).
   - No multi-color gradient fills (solid steel blue / ice blue tracks).
   - No proportional sans-serif for currency figures (strictly monospaced tabular figures).
   - No centered body text (left-aligned technical layout).
