# src/config.py
from __future__ import annotations
from pathlib import Path

# -------------------------
# Project paths
# -------------------------
ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
OUTPUT_DIR = ROOT / "output"
FIGURES_DIR = OUTPUT_DIR / "figures"
FIGURES_DIR.mkdir(parents=True, exist_ok=True)
PROCESSED_DIR = DATA_DIR / "processed"
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

# -------------------------
# Raw input files
# -------------------------
BANK_FILE = RAW_DIR / "Stoxx Europe Banks_data.xlsx"
OIL_FILE  = RAW_DIR / "Stoxx Europe Oil&Gas_data.xlsx"

WORKSHEET_NAME = "Worksheet"   # sheet to exclude

# -------------------------
# Excel parsing heuristics
# -------------------------
HEADER_SCAN_START_ROW = 2000
HEADER_SCAN_MAX_ROWS  = 3000
DATE_FORMAT = "%m/%d/%Y"       # adjust if needed

# -------------------------
# Panel columns
# -------------------------
EXPECTED_HEADERS = [
    "date",
    "rendite_1_quartal",
    "volatility 260 day calc",
    "return on common equity",
    "price to book ratio",
    "book to market ratio",
    "bloomberg_esg",
    "refinitiv_esg",
    "rendite_2_quartal",
]

Y_VARS = [
    "rendite_1_quartal",
    "volatility 260 day calc",
    "return on common equity",
    "price to book ratio",
    "book to market ratio",
    "rendite_2_quartal",
]

X_VARS = [
    "bloomberg_esg",
    "refinitiv_esg",
]

ENTITY_COL = "firm"
TIME_COL = "date"
SECTOR_COL = "sector"

SECTOR_BANKS = "banks"
SECTOR_OILGAS = "oil_gas"

# -------------------------
# Output files (processed)
# -------------------------
BANK_PANEL_PARQUET = PROCESSED_DIR / "bank_panel.parquet"
OIL_PANEL_PARQUET  = PROCESSED_DIR / "oil_panel.parquet"
