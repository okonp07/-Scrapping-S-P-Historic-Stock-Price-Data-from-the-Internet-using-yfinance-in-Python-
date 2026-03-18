# S&P 500 Historical Prices with `yfinance`

This project downloads daily adjusted closing prices for the companies that are in the S&P 500 today, using the current Wikipedia constituents table and Yahoo Finance price history.

## Important Caveat

This is **not** a true historical S&P 500 membership dataset.

The workflow uses the **current** S&P 500 constituents list from Wikipedia, then pulls prices for your selected date range. That is useful for many exploratory analyses, but it introduces survivorship bias if you want to study the exact index membership that existed in earlier years.

## What Changed

The repository now includes a maintained Python workflow alongside the original notebook-style project:

- a reusable downloader module in `src/sp500_historical_prices`
- a CLI script in `scripts/download_sp500_prices.py`
- compatibility fixes for current `yfinance`
- a more robust Wikipedia fetch that sends a browser-like user agent
- tests for symbol normalization, table parsing, and price extraction

## Quick Start

```bash
python3 -m pip install -r requirements.txt
python3 scripts/download_sp500_prices.py --start 2017-01-01 --end 2023-01-01
```

That command writes:

- `outputs/sp500_adjusted_close.csv`
- `outputs/sp500_constituents.csv`

## Command Options

```bash
python3 scripts/download_sp500_prices.py \
  --start 2017-01-01 \
  --end 2023-01-01 \
  --output-csv outputs/sp500_adjusted_close.csv \
  --constituents-csv outputs/sp500_constituents.csv \
  --keep-all-nan
```

## Why The Workflow Is More Robust Now

- Wikipedia requests use `requests` with a user agent before parsing tables.
- `pandas.read_html` reads from the fetched HTML body instead of hitting Wikipedia directly.
- Yahoo Finance downloads explicitly use `auto_adjust=False`, so `Adj Close` is available with current `yfinance`.
- S&P class-share tickers such as `BRK.B` and `BF.B` are normalized to Yahoo Finance's dash format.
- Columns that are entirely missing over the requested date window can be dropped automatically and reported clearly.

## Project Structure

- `src/sp500_historical_prices/`: maintained source code
- `scripts/download_sp500_prices.py`: CLI entrypoint
- `tests/`: unit tests
- `S&P_500_data_Scrapping_Project.ipynb`: refreshed walkthrough notebook
- `Blackfaces_filtered.ipynb` and `Optimizing_Euro_Stoxx_50_Investments_A_UCB_Algorithm_Based_Stock_Selection_Strategy_with_Correlation_Driven_Hedging.ipynb`: older exploratory notebooks kept as archival material

## Running Tests

```bash
python3 -m pytest
```
