"""
Twelve Data HTTP Client
=======================

Lightweight client using the Twelve Data REST API as a fallback provider.
Reads TWELVEDATA_API_KEY from environment when available.

Docs: https://twelvedata.com/docs
"""

import os
import time
import requests
import pandas as pd
from typing import Dict, Any, List, Optional
from core.logger import setup_logger


class TwelveDataClient:
    """Simple client for Twelve Data API.

    This client implements a minimal subset used by the pipelines:
    - get_time_series(symbol)
    - download_multiple(symbols)
    """

    BASE_URL = "https://api.twelvedata.com"

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.environ.get('TWELVEDATA_API_KEY')
        self.session = requests.Session()
        self.logger = setup_logger('scraper.twelvedata_client')

        if not self.api_key:
            self.logger.warning('Twelve Data API key not found in environment (TWELVEDATA_API_KEY)')
        else:
            self.logger.info('Twelve Data client initialized')

    def _request(self, path: str, params: Dict[str, Any]) -> Dict[str, Any]:
        params = params.copy()
        params['apikey'] = self.api_key
        url = f"{self.BASE_URL}{path}"

        start = time.time()
        r = self.session.get(url, params=params, timeout=30)
        elapsed = time.time() - start

        try:
            data = r.json()
        except ValueError:
            self.logger.error('Invalid JSON from Twelve Data', extra={'url': url, 'status': r.status_code})
            raise

        if r.status_code != 200 or 'status' in data and data.get('status') == 'error':
            self.logger.warning('Twelve Data returned error', extra={'status_code': r.status_code, 'body': data})
            raise ValueError(f"Twelve Data error: {data}")

        self.logger.info('Twelve Data request successful', extra={'path': path, 'elapsed': round(elapsed, 2)})
        return data

    def get_time_series(self, symbol: str, interval: str = '1day', outputsize: int = 5000) -> Optional[pd.DataFrame]:
        """Get historical time series for a single symbol.

        Returns a pandas.DataFrame like Yahoo client or None on error.
        """
        if not self.api_key:
            return None

        params = {
            'symbol': symbol,
            'interval': interval,
            'outputsize': outputsize,
            'format': 'JSON'
        }

        try:
            data = self._request('/time_series', params)
            values = data.get('values') or []

            if not values:
                return None

            df = pd.DataFrame(values)
            # Twelve Data returns strings; convert types
            df = df.rename(columns={
                'datetime': 'datetime',
                'open': 'open',
                'high': 'high',
                'low': 'low',
                'close': 'close',
                'volume': 'volume'
            })

            # Convert types
            for col in ['open', 'high', 'low', 'close']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            if 'volume' in df.columns:
                df['volume'] = pd.to_numeric(df['volume'], errors='coerce').fillna(0).astype('Int64')

            df['datetime'] = pd.to_datetime(df['datetime'])
            df['date'] = df['datetime'].dt.strftime('%Y-%m-%d')

            # Reorder
            column_order = ['datetime', 'date', 'open', 'high', 'low', 'close', 'volume']
            df = df[[c for c in column_order if c in df.columns]]
            return df

        except Exception:
            self.logger.exception('Failed to fetch time series from Twelve Data for %s', symbol)
            return None

    def download_multiple(self, symbols: List[str], interval: str = '1day', outputsize: int = 5000) -> Dict[str, pd.DataFrame]:
        """Download multiple symbols sequentially (Twelve Data doesn't support large batch endpoints on free tier).

        This implementation is intentionally simple: it iterates symbols and calls get_time_series.
        """
        results: Dict[str, pd.DataFrame] = {}
        for s in symbols:
            try:
                df = self.get_time_series(s, interval=interval, outputsize=outputsize)
                if df is not None:
                    results[s] = df
            except Exception:
                self.logger.exception('Error downloading symbol %s from Twelve Data', s)
        return results


# Default instance
default_twelvedata_client = TwelveDataClient()
