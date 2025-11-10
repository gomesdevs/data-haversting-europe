"""
Multi API Client
================

Wrapper that attempts to fetch historical data from multiple providers in order:
1. Yahoo Finance (fast, preferred)
2. Twelve Data (requires key, fallback)
3. Alpha Vantage (existing client)

Exposes: get_historical_data(symbol, period, interval) and download_multiple(symbols,...)
"""

from typing import List, Dict, Any, Optional
from core.yahoo_finance_client import default_yahoo_client
from core.twelvedata_client import default_twelvedata_client
from core.alphavantage_client import default_alphavantage_client
from core.logger import setup_logger
import time


class MultiApiClient:
    def __init__(self):
        self.yahoo = default_yahoo_client
        self.td = default_twelvedata_client
        self.av = default_alphavantage_client
        self.logger = setup_logger('scraper.multi_api_client')

    def get_historical_data(self, symbol: str, period: str = 'max', interval: str = '1d'):
        """Try providers in order and return first successful DataFrame or None."""
        # 1) Yahoo
        try:
            df = self.yahoo.get_historical_data(symbol=symbol, period=period, interval=interval)
            if df is not None and len(df) > 0:
                self.logger.info('Using Yahoo for %s', symbol)
                return df
        except Exception:
            self.logger.exception('Yahoo failed for %s', symbol)

        # 2) Twelve Data (if key configured)
        try:
            df = None
            if self.td and getattr(self.td, 'api_key', None):
                # Twelve Data uses outputsize instead of period; map simple periods
                df = self.td.get_time_series(symbol=symbol, interval='1day')
            if df is not None and len(df) > 0:
                self.logger.info('Using Twelve Data for %s', symbol)
                return df
        except Exception:
            self.logger.exception('Twelve Data failed for %s', symbol)

        # 3) Alpha Vantage
        try:
            data = self.av.get_data_for_interval(symbol=symbol, interval=interval, outputsize='full')
            if data:
                # Attempt to convert AlphaVantage JSON to DataFrame
                df = self._alphavantage_json_to_df(data)
                if df is not None:
                    self.logger.info('Using AlphaVantage for %s', symbol)
                    return df
                else:
                    return None
        except Exception:
            self.logger.exception('AlphaVantage failed for %s', symbol)

        self.logger.warning('All providers failed for %s', symbol)
        return None

    def _alphavantage_json_to_df(self, data: dict):
        """Try to coerce AlphaVantage JSON into a pandas DataFrame similar to other clients.

        Returns DataFrame or None on failure.
        """
        try:
            # Common keys: 'Time Series (Daily)', 'Time Series (60min)', etc.
            for k in data.keys():
                if k.lower().startswith('time series') and isinstance(data[k], dict):
                    ts = data[k]
                    # ts: {date_str: { '1. open': '...', '2. high': '...', ... }}
                    import pandas as pd
                    rows = []
                    for dt, vals in ts.items():
                        row = {'datetime': pd.to_datetime(dt)}
                        # Extract numeric fields
                        for field_key, field_val in vals.items():
                            # field_key like '1. open' -> 'open'
                            name = field_key.split('. ', 1)[-1].strip() if '. ' in field_key else field_key
                            row[name] = float(field_val) if field_val not in (None, '') else None
                        rows.append(row)
                    df = pd.DataFrame(rows)
                    # Normalize column names to match other clients
                    rename_map = {}
                    if 'open' in df.columns:
                        rename_map['open'] = 'open'
                    if 'high' in df.columns:
                        rename_map['high'] = 'high'
                    if 'low' in df.columns:
                        rename_map['low'] = 'low'
                    if 'close' in df.columns:
                        rename_map['close'] = 'close'
                    if 'volume' in df.columns:
                        rename_map['volume'] = 'volume'

                    # Ensure datetime and date
                    df['date'] = df['datetime'].dt.strftime('%Y-%m-%d')
                    column_order = ['datetime', 'date', 'open', 'high', 'low', 'close', 'volume']
                    df = df[[c for c in column_order if c in df.columns]]
                    # Sort ascending by datetime
                    df = df.sort_values('datetime').reset_index(drop=True)
                    return df
        except Exception:
            self.logger.exception('Failed to convert AlphaVantage JSON to DataFrame')
            return None

    def download_multiple(self, symbols: List[str], period: str = 'max', interval: str = '1d') -> Dict[str, Any]:
        """Attempt batch download: prefer Yahoo's batch, then fallback per-symbol."""
        results: Dict[str, Any] = {}

        # Try Yahoo batch
        try:
            batch = self.yahoo.download_multiple(symbols=symbols, period=period, interval=interval)
            if batch:
                # batch is dict {symbol: df}
                results.update(batch)
                # For symbols not included, fall back per-symbol
                missing = [s for s in symbols if s not in results]
            else:
                missing = symbols
        except Exception:
            self.logger.exception('Yahoo batch failed, will fallback per-symbol')
            missing = symbols

        # Per-symbol fallback for missing
        for s in missing:
            df = None
            try:
                df = self.get_historical_data(symbol=s, period=period, interval=interval)
            except Exception:
                self.logger.exception('Per-symbol fallback failed for %s', s)

            if df is not None:
                results[s] = df

        return results


# Default instance
default_multi_client = MultiApiClient()
