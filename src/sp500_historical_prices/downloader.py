from __future__ import annotations

import argparse
from dataclasses import dataclass
from io import StringIO
from pathlib import Path

import pandas as pd
import requests
import yfinance as yf

WIKIPEDIA_SP500_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36"
    )
}


@dataclass
class DownloadResult:
    constituents: pd.DataFrame
    prices: pd.DataFrame
    dropped_symbols: list[str]
    note: str


def normalize_symbol(symbol: str) -> str:
    """Convert Wikipedia-style symbols to the Yahoo Finance format."""
    return symbol.strip().replace(".", "-")


def fetch_sp500_constituents(
    url: str = WIKIPEDIA_SP500_URL,
    session: requests.Session | None = None,
) -> pd.DataFrame:
    """Fetch the current S&P 500 constituents table from Wikipedia."""
    session = session or requests.Session()
    response = session.get(url, headers=REQUEST_HEADERS, timeout=30)
    response.raise_for_status()

    tables = pd.read_html(StringIO(response.text))
    if not tables:
        raise ValueError("No HTML tables were found on the Wikipedia page.")

    constituents = tables[0].copy()
    required_columns = {"Symbol", "Security"}
    missing_columns = required_columns.difference(constituents.columns)
    if missing_columns:
        missing_display = ", ".join(sorted(missing_columns))
        raise ValueError(
            "The constituents table is missing expected columns: "
            f"{missing_display}."
        )

    constituents["Wikipedia Symbol"] = constituents["Symbol"]
    constituents["Yahoo Symbol"] = constituents["Symbol"].map(normalize_symbol)
    constituents = constituents.drop_duplicates(subset=["Yahoo Symbol"]).reset_index(
        drop=True
    )
    return constituents


def extract_price_frame(raw_download: pd.DataFrame, field: str = "Adj Close") -> pd.DataFrame:
    """Extract a single price field from yfinance's varying column layouts."""
    if raw_download.empty:
        return raw_download.copy()

    if isinstance(raw_download.columns, pd.MultiIndex):
        first_level = raw_download.columns.get_level_values(0)
        last_level = raw_download.columns.get_level_values(-1)

        if field in first_level:
            selected = raw_download[field]
        elif field in last_level:
            selected = raw_download.xs(field, axis=1, level=-1)
        else:
            available = ", ".join(sorted({str(value) for value in first_level}))
            raise KeyError(
                f"'{field}' was not returned by yfinance. Available fields: {available}"
            )

        if isinstance(selected, pd.Series):
            return selected.to_frame()
        return selected.copy()

    if field not in raw_download.columns:
        available = ", ".join(str(column) for column in raw_download.columns)
        raise KeyError(
            f"'{field}' was not returned by yfinance. Available fields: {available}"
        )

    selected = raw_download[field]
    if isinstance(selected, pd.Series):
        return selected.to_frame()
    return selected.copy()


def download_adjusted_close(
    tickers: list[str],
    start: str,
    end: str,
    *,
    keep_all_nan: bool = False,
    show_progress: bool = True,
) -> tuple[pd.DataFrame, list[str]]:
    """Download adjusted close prices and optionally drop symbols with no data."""
    normalized_tickers = [normalize_symbol(ticker) for ticker in tickers]
    raw_download = yf.download(
        tickers=normalized_tickers,
        start=start,
        end=end,
        auto_adjust=False,
        progress=show_progress,
        group_by="column",
        threads=True,
    )
    adjusted_close = extract_price_frame(raw_download, field="Adj Close")
    adjusted_close = adjusted_close.sort_index(axis=1)

    dropped_symbols = adjusted_close.columns[adjusted_close.isna().all()].tolist()
    if dropped_symbols and not keep_all_nan:
        adjusted_close = adjusted_close.drop(columns=dropped_symbols)

    return adjusted_close, dropped_symbols


def export_dataset(
    start: str,
    end: str,
    *,
    output_csv: Path,
    constituents_csv: Path | None = None,
    keep_all_nan: bool = False,
    show_progress: bool = True,
) -> DownloadResult:
    """Build and export the adjusted close dataset for current constituents."""
    constituents = fetch_sp500_constituents()
    prices, dropped_symbols = download_adjusted_close(
        tickers=constituents["Yahoo Symbol"].tolist(),
        start=start,
        end=end,
        keep_all_nan=keep_all_nan,
        show_progress=show_progress,
    )

    if dropped_symbols and not keep_all_nan:
        constituents = constituents[
            ~constituents["Yahoo Symbol"].isin(dropped_symbols)
        ].reset_index(drop=True)

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    prices.to_csv(output_csv, index=True)

    if constituents_csv is not None:
        constituents_csv.parent.mkdir(parents=True, exist_ok=True)
        constituents.to_csv(constituents_csv, index=False)

    note = (
        "This dataset uses the current S&P 500 constituents table from Wikipedia "
        "and then downloads the selected date window from Yahoo Finance. It does "
        "not reconstruct historical index membership."
    )
    return DownloadResult(
        constituents=constituents,
        prices=prices,
        dropped_symbols=dropped_symbols,
        note=note,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Download adjusted close prices for the current S&P 500 constituents."
    )
    parser.add_argument("--start", required=True, help="Start date in YYYY-MM-DD format.")
    parser.add_argument("--end", required=True, help="End date in YYYY-MM-DD format.")
    parser.add_argument(
        "--output-csv",
        default="outputs/sp500_adjusted_close.csv",
        help="Path for the adjusted close CSV output.",
    )
    parser.add_argument(
        "--constituents-csv",
        default="outputs/sp500_constituents.csv",
        help="Path for the cleaned constituents CSV output.",
    )
    parser.add_argument(
        "--keep-all-nan",
        action="store_true",
        help="Keep symbols with no data over the requested date range.",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Disable yfinance progress output.",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    result = export_dataset(
        start=args.start,
        end=args.end,
        output_csv=Path(args.output_csv),
        constituents_csv=Path(args.constituents_csv),
        keep_all_nan=args.keep_all_nan,
        show_progress=not args.quiet,
    )

    print(result.note)
    print(f"Wrote prices to: {Path(args.output_csv).resolve()}")
    print(f"Wrote constituents to: {Path(args.constituents_csv).resolve()}")
    print(f"Rows: {len(result.prices):,}")
    print(f"Symbols: {len(result.prices.columns):,}")

    if result.dropped_symbols and not args.keep_all_nan:
        print(
            "Dropped symbols with no data in the requested window: "
            + ", ".join(result.dropped_symbols)
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
