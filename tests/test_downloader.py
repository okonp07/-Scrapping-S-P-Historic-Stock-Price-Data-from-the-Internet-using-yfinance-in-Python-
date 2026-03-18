from __future__ import annotations

import pandas as pd

from sp500_historical_prices.downloader import (
    download_adjusted_close,
    extract_price_frame,
    fetch_sp500_constituents,
    normalize_symbol,
)


class DummyResponse:
    def __init__(self, text: str) -> None:
        self.text = text

    def raise_for_status(self) -> None:
        return None


class DummySession:
    def __init__(self, text: str) -> None:
        self.text = text
        self.calls: list[tuple[str, dict[str, str], int]] = []

    def get(self, url: str, headers: dict[str, str], timeout: int) -> DummyResponse:
        self.calls.append((url, headers, timeout))
        return DummyResponse(self.text)


def test_normalize_symbol_replaces_dot_with_dash() -> None:
    assert normalize_symbol("BRK.B") == "BRK-B"
    assert normalize_symbol("BF.B") == "BF-B"
    assert normalize_symbol("AAPL") == "AAPL"


def test_fetch_sp500_constituents_adds_yahoo_symbol_column() -> None:
    html = """
    <table>
      <thead>
        <tr>
          <th>Symbol</th>
          <th>Security</th>
          <th>GICS Sector</th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <td>BRK.B</td>
          <td>Berkshire Hathaway</td>
          <td>Financials</td>
        </tr>
        <tr>
          <td>AAPL</td>
          <td>Apple</td>
          <td>Information Technology</td>
        </tr>
      </tbody>
    </table>
    """
    session = DummySession(html)

    constituents = fetch_sp500_constituents(
        url="https://example.com/sp500",
        session=session,
    )

    assert constituents["Wikipedia Symbol"].tolist() == ["BRK.B", "AAPL"]
    assert constituents["Yahoo Symbol"].tolist() == ["BRK-B", "AAPL"]
    assert session.calls[0][0] == "https://example.com/sp500"
    assert "Mozilla/5.0" in session.calls[0][1]["User-Agent"]


def test_extract_price_frame_handles_multiindex_columns() -> None:
    columns = pd.MultiIndex.from_product([["Adj Close", "Close"], ["AAPL", "MSFT"]])
    frame = pd.DataFrame(
        [
            [100.0, 200.0, 101.0, 201.0],
            [102.0, 202.0, 103.0, 203.0],
        ],
        columns=columns,
    )

    extracted = extract_price_frame(frame, field="Adj Close")

    assert list(extracted.columns) == ["AAPL", "MSFT"]
    assert extracted.iloc[0, 0] == 100.0
    assert extracted.iloc[1, 1] == 202.0


def test_download_adjusted_close_drops_all_nan_columns(monkeypatch) -> None:
    index = pd.date_range("2022-01-01", periods=2, freq="D")
    columns = pd.MultiIndex.from_tuples(
        [
            ("Adj Close", "AAPL"),
            ("Adj Close", "GEHC"),
            ("Close", "AAPL"),
            ("Close", "GEHC"),
        ]
    )
    fake_download = pd.DataFrame(
        [
            [150.0, float("nan"), 151.0, float("nan")],
            [152.0, float("nan"), 153.0, float("nan")],
        ],
        index=index,
        columns=columns,
    )

    def fake_yf_download(**kwargs):
        assert kwargs["auto_adjust"] is False
        assert kwargs["tickers"] == ["AAPL", "GEHC"]
        return fake_download

    monkeypatch.setattr("sp500_historical_prices.downloader.yf.download", fake_yf_download)

    prices, dropped = download_adjusted_close(
        tickers=["AAPL", "GEHC"],
        start="2022-01-01",
        end="2022-01-03",
        show_progress=False,
    )

    assert list(prices.columns) == ["AAPL"]
    assert dropped == ["GEHC"]
