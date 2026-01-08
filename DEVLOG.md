# DEVLOG: Cognitive Capital Analysis - Brazil

**Project:** Cognitive Capital Analysis - Brazil
**Researchers:** Dr. José Aparecido da Silva | Me. Cássio Dalbem Barth
**Last Update:** 2026-01-06
**Status:** Data Pipeline Complete and Validated

---

## Development History

### 1. Configuration and Standardization
- Defined directory structure (`data/raw`, `src/cog`, `reports/varcog`).
- Established **Standard Header** for all scripts (Metadata, Data Source Citations, Authors).
- Standardized code language and outputs to **English** (for international academic compatibility).
- Adopted **Seed 42** for scientific reproducibility.

### 2. SAEB 2023 Processing (National Census)
- **Challenge:** The original `TS_ESCOLA` file contained only IBGE codes (`ID_UF`) without region names.
- **Solution:** Implemented a robust mapping dictionary (`IBGE_CODE_MAP` and `UF_REGION_MAP`).
- **Technical Fix:** Forced conversion of `ID_UF` to `int` to avoid mapping errors (`NaN`) during CSV reading.
- **Output:** Generation of state and regional rankings for Math and Portuguese.

### 3. PISA Processing (The International Anchor)
- **PISA 2015 & 2018:** Direct processing via `.sav` (SPSS) files.
- **PISA 2022 (Critical):**
    - *Issue:* The public 2022 file does not provide clear numeric codes for Brazilian states, using descriptive strings in the `STRATUM` column instead.
    - *Correction:* Developed a **Text Matching** function (`get_region`) to extract "North", "South", "Northeast", etc., directly from descriptive strings.
    - *Correction 2:* Adjusted country filter to accept both `BRA` and `Brazil`.
- **Temporal Analysis:** Created comparison script (Delta 2018 vs 2022), revealing the rise of the South Region and the decline of the Center-West.

### 4. The ENEM Triennium (Big Data)
- **Strategy:** Utilization of a 3-year mean (2022, 2023, 2024) to eliminate annual noise and create a robust indicator.
- **Data Engineering:**
    - Implemented **Chunk** reading (blocks of 500k rows) to process giant files inside ZIPs without exceeding RAM limits.
    - Automatic selection of the largest `.csv` file inside the ZIP (ignoring manuals and metadata).
- **Consolidation:** Unified script that generates annual reports and, at the end, merges them to create `enem_consolidated_states_triennium.xlsx`.

### 5. Statistical Validation (Correlation)
- **Hypothesis:** Are ENEM and SAEB valid proxies for Cognitive Capital (PISA)?
- **Granularity Issue:** PISA 2022 (N=5 Regions) vs. ENEM/SAEB (N=27 States).
- **Methodological Solution:** Mathematical aggregation of state-level ENEM/SAEB data to Regional level (Weighted Mean).
- **Result:** Pearson Correlation ($r > 0.95$) and Spearman Correlation confirm the robustness of national indicators against the international standard.

### 6. Orchestration (Pipeline)
- Creation of `00_run_pipeline.py` using `subprocess`.
- Ensures secure sequential execution, clearing memory between each intensive step.

---

## Code Architecture (`src/cog/`)

| Script | Main Function |
| :--- | :--- |
| **`00_run_pipeline.py`** | **Master Orchestrator.** Runs all scripts below in order. |
| `01_process_saeb_2023.py` | Extracts SAEB data, maps UF/Region, and generates rankings. |
| `02_process_pisa_*.py` | Series of scripts to clean PISA data (2015, 2018, 2022). |
| `03_compare_years.py` | Generates the variation table (Delta) for PISA pre- and post-pandemic. |
| `04_process_enem_triennium.py` | Processes 3 years of ENEM and consolidates into a single robust indicator. |
| `05_correlate_indicators.py` | Crosses PISA, SAEB, and ENEM. Generates Correlation Matrices and Scatter Plots. |

---

## Key Data Insights (To Date)

1.  **The South Anomaly:** The only region that grew or maintained resilience in PISA 2022, assuming the national lead.
2.  **ENEM Validity:** The ENEM Triennium proved to be an almost perfect predictor of PISA performance ($r=0.95$), validating its use for more granular state-level analyses where PISA data is unavailable.
3.  **Center-West Decline:** Data shows a sharp drop (-19 points) in PISA 2022 compared to 2018.

---

## Next Steps
1.  Draft qualitative analysis based on the generated charts.
2.  Investigate socioeconomic factors (if data becomes available) that explain the divergence of the South Region.