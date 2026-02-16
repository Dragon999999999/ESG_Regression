# src/prep_data.py
from __future__ import annotations

import re
import numpy as np
import pandas as pd

from config import (
    EXPECTED_HEADERS,
    WORKSHEET_NAME,
    HEADER_SCAN_START_ROW,
    HEADER_SCAN_MAX_ROWS,
    DATE_FORMAT,
    SECTOR_BANKS,
    SECTOR_OILGAS,
    BANK_FILE,
    OIL_FILE,
    BANK_PANEL_PARQUET,
    OIL_PANEL_PARQUET,
)

def _norm(s: str) -> str:
    """Normalize header strings for robust matching."""
    if s is None or (isinstance(s, float) and np.isnan(s)):
        return ""
    s = str(s).strip().lower()
    s = re.sub(r"\s+", " ", s)
    s = s.replace("\n", " ").strip()
    return s

def find_header_row(
    file_path: str | "os.PathLike[str]",
    sheet_name: str,
    start_row: int = HEADER_SCAN_START_ROW,
    max_scan_rows: int = HEADER_SCAN_MAX_ROWS,
) -> int:
    """Return 0-based row index of the header row."""
    chunk = pd.read_excel(
        file_path,
        sheet_name=sheet_name,
        header=None,
        skiprows=start_row,
        nrows=max_scan_rows,
        engine=None,
    )

    expected = set(_norm(h) for h in EXPECTED_HEADERS)

    for i in range(len(chunk)):
        row_vals = [_norm(v) for v in chunk.iloc[i].tolist()]
        row_set = set(row_vals)

        hits = sum((h in row_set) for h in expected)
        if ("date" in row_set) and (hits >= 7):
            return start_row + i

    raise ValueError(
        f"Header row not found in '{sheet_name}' of '{file_path}' "
        f"(scanned rows {start_row}..{start_row+max_scan_rows-1})."
    )

def read_firm_sheet(
    file_path: str | "os.PathLike[str]",
    sheet_name: str,
    start_row: int = HEADER_SCAN_START_ROW,
    max_scan_rows: int = HEADER_SCAN_MAX_ROWS,
) -> pd.DataFrame:
    """Read and clean one firm sheet to a date-indexed DataFrame."""
    hdr = find_header_row(file_path, sheet_name, start_row, max_scan_rows)

    df = pd.read_excel(
        file_path,
        sheet_name=sheet_name,
        header=hdr,
        engine=None,
    )

    df.columns = [_norm(c) for c in df.columns]

    keep = [c for c in EXPECTED_HEADERS if c in df.columns]
    df = df[keep].copy()

    if "date" not in df.columns:
        raise ValueError(f"No 'date' column after reading '{sheet_name}' in '{file_path}'.")

    df["date"] = pd.to_datetime(df["date"], format=DATE_FORMAT, errors="coerce")
    df = df[df["date"].notna()].copy()

    for c in df.columns:
        if c != "date":
            df[c] = pd.to_numeric(df[c], errors="coerce")

    return df.set_index("date").sort_index()

def list_firm_sheets(xlsx_path: str | "os.PathLike[str]") -> list[str]:
    """All sheet names except the worksheet metadata sheet."""
    xls = pd.ExcelFile(xlsx_path)
    return [s for s in xls.sheet_names if s != WORKSHEET_NAME]

def build_panel(xlsx_path, sector_label: str) -> pd.DataFrame:
    """Concatenate all firm sheets into a (firm, date) indexed panel."""
    frames: list[pd.DataFrame] = []
    for firm in list_firm_sheets(xlsx_path):
        df_firm = read_firm_sheet(xlsx_path, firm)
        df_firm["firm"] = firm
        df_firm["sector"] = sector_label
        frames.append(df_firm.reset_index())

    panel = pd.concat(frames, ignore_index=True)
    panel = panel.set_index(["firm", "date"]).sort_index()
    return panel

def add_esg_zscores(
    df: pd.DataFrame,
    esg_cols: list[str],
    group_col: str = "sector",
) -> pd.DataFrame:
    """
    Add z-score columns for ESG variables.
    Z-scoring is performed within group_col (e.g. sector).
    """
    df = df.copy()

    for col in esg_cols:
        z_col = f"{col}_z"
        df[z_col] = (
            df.groupby(group_col)[col]
              .transform(lambda x: (x - x.mean()) / x.std())
        )

    return df

def main(save: bool = True) -> tuple[pd.DataFrame, pd.DataFrame]:
    df_bank_panel = build_panel(BANK_FILE, SECTOR_BANKS)
    df_oil_panel  = build_panel(OIL_FILE, SECTOR_OILGAS)

    df_bank_panel = add_esg_zscores(
        df_bank_panel,
        esg_cols=["bloomberg_esg", "refinitiv_esg"],
    )
    df_oil_panel = add_esg_zscores(
        df_oil_panel,
        esg_cols=["bloomberg_esg", "refinitiv_esg"],
    )


    if save:
        # Store as Parquet for fast reload
        df_bank_panel.reset_index().to_parquet(BANK_PANEL_PARQUET, index=False)
        df_oil_panel.reset_index().to_parquet(OIL_PANEL_PARQUET, index=False)

    return df_bank_panel, df_oil_panel

if __name__ == "__main__":
    main(save=True)
