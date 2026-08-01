# HDB Resale Flat Prices — ETL Pipeline

An end-to-end data pipeline that extracts, cleans, validates, transforms, and exports HDB resale flat transaction data (January 2012 – December 2016). Built in Python, designed to run as a single Jupyter Notebook.

---

## Project Structure

```
HDB/
├── HDB_analysis.ipynb                  # Main ETL notebook (run cells sequentially)
├── README.md                           # This file
│
├── raw_data/ResaleFlatPrices/          # [GROUP 1: RAW] 3 source CSVs
│   ├── Resale Flat Prices (Based on Approval Date), 2000 - Feb 2012.csv
│   ├── Resale Flat Prices (Based on Registration Date), From Mar 2012 to Dec 2014.csv
│   └── Resale Flat Prices (Based on Registration Date), From Jan 2015 to Dec 2016.csv
│
├── manipulated_data/                   # Pipeline output files
│   ├── master_dataset.csv              # Combined master dataset (Step 1)
│   ├── cleaned_dataset.csv             # [GROUP 2: CLEANED]
│   ├── transformed_dataset.csv         # [GROUP 3: TRANSFORMED]
│   ├── failed_records.csv              # [GROUP 4: FAILED]
│   └── hashed_dataset.csv              # [GROUP 5: HASHED]
│
├── profiling_outputs/                  # Reports and visualisations
│   ├── profiling_summary.txt
│   ├── profiling_visualisations.png
│   ├── validation_rules.json
│   └── anomaly_summary.json
│
├── docs/                               # Deployable AWS architecture documentation
│   ├── architecture.html               # Interactive architecture diagram
│   ├── index.html
│   └── icons/                          # AWS service icons (SVG)
│
└── architecture/                       # Architecture design documents
    ├── ARCHITECTURE.md                 # Design decisions, security, cost estimates
    └── diagrams/
        ├── architecture.html
        └── icons/
```

The `docs/` and `architecture/` folders document the AWS architecture that this pipeline maps onto (data.gov.sg → S3 → AWS Glue → Amazon Athena → Tableau). Open `docs/architecture.html` for an interactive diagram, or `architecture/ARCHITECTURE.md` for the design rationale, network segmentation, and cost estimates.

---

## Data Sources

The pipeline consumes three CSV files published on [data.gov.sg](https://data.gov.sg/collections/189/view). Each file covers a different date range and pricing basis.

| File | Date Range | Pricing Basis | Has Remaining Lease |
|------|-----------|---------------|---------------------|
| Resale Flat Prices (Based on Approval Date), 2000 - Feb 2012 | 2000 – Feb 2012 | Approval date | No |
| Resale Flat Prices (Based on Registration Date), From Mar 2012 to Dec 2014 | Mar 2012 – Dec 2014 | Registration date | No |
| Resale Flat Prices (Based on Registration Date), From Jan 2015 to Dec 2016 | Jan 2015 – Dec 2016 | Registration date | Yes |

Only records within the **January 2012 – December 2016** window are retained after loading.

---

## Pipeline Steps

### Step 1 — Combine Datasets

Load all three source CSVs, align their schemas, and merge into a single master dataset. The pre-2015 files lack a `remaining_lease` column, which is back-filled with `NaN`. The `flat_model` column is standardised to title case, and rows are sorted chronologically. A `source` column is added to track which file each row originated from.

**Output:** `manipulated_data/master_dataset.csv`

### Step 2 — Data Profiling

Generate a plain-text profiling report and a six-panel matplotlib visualisation covering the master dataset. The report includes a dataset overview (shape, memory usage, date range, cardinalities), per-column profiles (dtype, null counts, unique values), descriptive statistics for numeric columns, top-15 frequency tables for categorical columns, records per source file, missing value summary, duplicate analysis, IQR-based outlier detection on resale price, and year-over-year record counts.

**Output:** `profiling_outputs/profiling_summary.txt`, `profiling_outputs/profiling_visualisations.png`

### Step 3 — Validation Rules

Derive data validation rules for the following fields based on the statistical properties of the master dataset:

| Field | Rule Type | What Is Derived |
|-------|-----------|-----------------|
| `month` | Date | Min/max date range, unique count, null constraint |
| `town` | Categorical | Set of valid values, frequency stats, null constraint |
| `flat_type` | Categorical | Set of valid values, frequency stats, null constraint |
| `flat_model` | Categorical | Set of valid values, frequency stats, null constraint |
| `storey_range` | Categorical | Numerically sorted valid values, detected regex pattern, null constraint |

All rules are inferred from the data with nothing being hard-coded. The rules are then tested in two ways: first against the master dataset to confirm all records pass, and then against a copy injected with known-bad values (out-of-range dates, fake town names, invalid flat types) to confirm violations are caught.

**Output:** `profiling_outputs/validation_rules.json`

### Step 4 — Recompute Remaining Lease

Compute the remaining lease for every record as of today's date, assuming a standard 99-year HDB lease starting on 1 January of the `lease_commence_date` year. The calculation is fully vectorised (no row-by-row iteration). Results are rounded down to whole years and months.

Three columns are added: `remaining_lease_years` (integer), `remaining_lease_months` (integer, 0–11), and `remaining_lease_computed` (human-readable string, e.g. "61 years 04 months"). Expired leases are set to 0 years 0 months. Rows with missing `lease_commence_date` are set to `NA`.

### Step 5 — Composite-Key Deduplication

The composite key is defined as all columns except `resale_price`. Where multiple rows share the same key, the row with the highest resale price is kept. All lower-priced duplicates are moved to the failed dataset with the reason `key_duplicate`.

### Step 6 — Anomaly Detection

Three independent heuristics flag potentially anomalous resale prices. A row is considered anomalous if any heuristic triggers.

| Heuristic | Method | Threshold |
|-----------|--------|-----------|
| Global IQR | Resale price outside 1.5x IQR from Q1/Q3 | 1.5x IQR |
| Peer z-score (town + flat type) | Price-per-sqm z-score within same town and flat type | z > 3.0 |
| Peer z-score (flat model) | Price-per-sqm z-score within same flat model | z > 3.0 |

**Assumptions:** Price per square metre is used to normalise for unit size variation. The z-score threshold of 3.0 captures approximately 0.3% of tail events under a normal distribution. Peer groups are defined independently by town + flat type and by flat model to catch different kinds of outliers.

Flagged rows are moved to the failed dataset with the reason `anomaly`. A JSON summary is saved with per-heuristic counts and documentation of assumptions.

**Output:** `profiling_outputs/anomaly_summary.json`

### Step 7 — Additional Validation Checks

Three supplementary checks provide deeper insight into data quality beyond the core requirements.

**Benford's Law** compares the leading-digit distribution of resale prices against the expected Benford distribution. HDB prices cluster in a narrow band, so deviations from Benford's Law are expected and do not indicate a quality issue.

**Isolation Forest** uses an unsupervised machine learning model (200 estimators, 2% contamination rate) to flag multivariate outliers across four features: resale price, floor area, lease commencement year, and price per square metre. These are rows that are unusual when all four dimensions are considered together.

**Year-over-Year Price Jumps** compares the median resale price for each town + flat type group against the previous year. Groups with a change exceeding 20% are flagged for review, as they may indicate market shifts, data errors, or small sample sizes.

### Step 8 — Record ID, Hashing, and Export

**Resale Identifier** — A composite ID is generated for each record with the following format:

| Position | Component | Derivation |
|----------|-----------|------------|
| 1 | Prefix | Always `S` |
| 2–4 | Block digits | First 3 digits of block number, zero-padded (e.g. block "19" becomes "019") |
| 5–6 | Average prefix | First 2 digits of the mean resale price for the same year-month, town, and flat type |
| 7–8 | Month | Two-digit transaction month (e.g. "01" for January) |
| 9 | Town initial | First character of the town name, uppercased |

**Hashing** — The identifier is hashed using SHA-256, an irreversible cryptographic hash function that produces a fixed 64-character hexadecimal digest. SHA-256 is chosen because it is collision-resistant (the probability of two different inputs producing the same hash is negligible), irreversible (the original identifier cannot be recovered from the hash), and deterministic (the same input always produces the same hash). The pipeline verifies that the number of unique hashes equals the number of unique identifiers to confirm no collisions occurred.

**ID-based Deduplication** — Where multiple rows share the same record ID, the row with the highest resale price is kept. Discarded rows are moved to the failed dataset with the reason `id_duplicate`.

**Final Outputs:**

| Group | File | Description |
|-------|------|-------------|
| 1 — Raw | `raw_data/ResaleFlatPrices/` | Source CSVs, unmodified |
| 2 — Cleaned | `manipulated_data/cleaned_dataset.csv` | Records that passed all quality checks |
| 3 — Transformed | `manipulated_data/transformed_dataset.csv` | Cleaned data with computed lease and record IDs |
| 4 — Failed | `manipulated_data/failed_records.csv` | All rejected records with a `failure_reason` column (key_duplicate, anomaly, id_duplicate) |
| 5 — Hashed | `manipulated_data/hashed_dataset.csv` | Cleaned data with record IDs and SHA-256 hashes |

---

## How to Run

1. Clone this repository.
2. Place the three source CSV files in `raw_data/ResaleFlatPrices/`.
3. Open `HDB_analysis.ipynb` and run all cells sequentially.
4. Outputs will be written to `manipulated_data/` and `profiling_outputs/`.

**Dependencies:** Python 3.10+, pandas, numpy, matplotlib, scikit-learn.

---
