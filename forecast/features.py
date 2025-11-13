"""
Feature Engineering - Indicadores Técnicos
===========================================

Cria features para modelos de forecast baseadas em análise técnica:
- Médias Móveis (SMA, EMA)
- Indicadores de Momentum (RSI, MACD)
- Volatilidade (Bollinger Bands, ATR)
- Volume (OBV, VWAP)
"""

import pandas as pd
import numpy as np
from typing import Optional, List


class FeatureEngineering:
    """Classe para criar features de análise técnica"""

    def __init__(self, df: pd.DataFrame):
        """
        Inicializa com DataFrame de preços

        Args:
            df: DataFrame com colunas [datetime, open, high, low, close, volume]
        """
        self.df = df.copy()
        self._validate_columns()

    def _validate_columns(self):
        """Valida se o DataFrame tem as colunas necessárias"""
        required = ['datetime', 'close']
        missing = [col for col in required if col not in self.df.columns]
        if missing:
            raise ValueError(f"Colunas faltando: {missing}")

        # Garantir datetime como index
        if self.df.index.name != 'datetime':
            self.df = self.df.set_index('datetime')

    # ===== MÉDIAS MÓVEIS =====

    def add_sma(self, periods: List[int] = [5, 10, 20, 50, 200]) -> 'FeatureEngineering':
        """
        Adiciona Simple Moving Average (SMA)

        Args:
            periods: Lista de períodos para calcular SMA
        """
        for period in periods:
            self.df[f'sma_{period}'] = self.df['close'].rolling(window=period).mean()
        return self

    def add_ema(self, periods: List[int] = [12, 26]) -> 'FeatureEngineering':
        """
        Adiciona Exponential Moving Average (EMA)

        Args:
            periods: Lista de períodos para calcular EMA
        """
        for period in periods:
            self.df[f'ema_{period}'] = self.df['close'].ewm(span=period, adjust=False).mean()
        return self

    # ===== MOMENTUM =====

    def add_rsi(self, period: int = 14) -> 'FeatureEngineering':
        """
        Adiciona Relative Strength Index (RSI)

        Args:
            period: Período para cálculo (padrão: 14)
        """
        delta = self.df['close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()

        rs = gain / loss
        self.df['rsi'] = 100 - (100 / (1 + rs))
        return self

    def add_macd(self, fast: int = 12, slow: int = 26, signal: int = 9) -> 'FeatureEngineering':
        """
        Adiciona Moving Average Convergence Divergence (MACD)

        Args:
            fast: Período EMA rápida (padrão: 12)
            slow: Período EMA lenta (padrão: 26)
            signal: Período linha de sinal (padrão: 9)
        """
        ema_fast = self.df['close'].ewm(span=fast, adjust=False).mean()
        ema_slow = self.df['close'].ewm(span=slow, adjust=False).mean()

        self.df['macd'] = ema_fast - ema_slow
        self.df['macd_signal'] = self.df['macd'].ewm(span=signal, adjust=False).mean()
        self.df['macd_hist'] = self.df['macd'] - self.df['macd_signal']
        return self

    def add_roc(self, period: int = 12) -> 'FeatureEngineering':
        """
        Adiciona Rate of Change (ROC)

        Args:
            period: Período para cálculo (padrão: 12)
        """
        self.df['roc'] = ((self.df['close'] - self.df['close'].shift(period)) /
                          self.df['close'].shift(period) * 100)
        return self

    # ===== VOLATILIDADE =====

    def add_bollinger_bands(self, period: int = 20, std_dev: int = 2) -> 'FeatureEngineering':
        """
        Adiciona Bollinger Bands

        Args:
            period: Período para SMA (padrão: 20)
            std_dev: Número de desvios padrão (padrão: 2)
        """
        sma = self.df['close'].rolling(window=period).mean()
        std = self.df['close'].rolling(window=period).std()

        self.df['bb_middle'] = sma
        self.df['bb_upper'] = sma + (std * std_dev)
        self.df['bb_lower'] = sma - (std * std_dev)
        self.df['bb_width'] = (self.df['bb_upper'] - self.df['bb_lower']) / self.df['bb_middle']
        return self

    def add_atr(self, period: int = 14) -> 'FeatureEngineering':
        """
        Adiciona Average True Range (ATR)

        Args:
            period: Período para cálculo (padrão: 14)
        """
        if not all(col in self.df.columns for col in ['high', 'low']):
            raise ValueError("ATR requer colunas 'high' e 'low'")

        high_low = self.df['high'] - self.df['low']
        high_close = abs(self.df['high'] - self.df['close'].shift())
        low_close = abs(self.df['low'] - self.df['close'].shift())

        true_range = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
        self.df['atr'] = true_range.rolling(window=period).mean()
        return self

    # ===== VOLUME =====

    def add_obv(self) -> 'FeatureEngineering':
        """Adiciona On-Balance Volume (OBV)"""
        if 'volume' not in self.df.columns:
            raise ValueError("OBV requer coluna 'volume'")

        obv = [0]
        for i in range(1, len(self.df)):
            if self.df['close'].iloc[i] > self.df['close'].iloc[i-1]:
                obv.append(obv[-1] + self.df['volume'].iloc[i])
            elif self.df['close'].iloc[i] < self.df['close'].iloc[i-1]:
                obv.append(obv[-1] - self.df['volume'].iloc[i])
            else:
                obv.append(obv[-1])

        self.df['obv'] = obv
        return self

    def add_vwap(self) -> 'FeatureEngineering':
        """Adiciona Volume Weighted Average Price (VWAP)"""
        required = ['high', 'low', 'close', 'volume']
        if not all(col in self.df.columns for col in required):
            raise ValueError(f"VWAP requer colunas: {required}")

        typical_price = (self.df['high'] + self.df['low'] + self.df['close']) / 3
        self.df['vwap'] = (typical_price * self.df['volume']).cumsum() / self.df['volume'].cumsum()
        return self

    # ===== PRICE TRANSFORMATIONS =====

    def add_returns(self, periods: List[int] = [1, 5, 10]) -> 'FeatureEngineering':
        """
        Adiciona retornos percentuais

        Args:
            periods: Lista de períodos para calcular retornos
        """
        for period in periods:
            self.df[f'return_{period}d'] = self.df['close'].pct_change(periods=period) * 100
        return self

    def add_log_returns(self) -> 'FeatureEngineering':
        """Adiciona log returns"""
        self.df['log_return'] = np.log(self.df['close'] / self.df['close'].shift(1))
        return self

    # ===== TREND =====

    def add_trend_strength(self, period: int = 14) -> 'FeatureEngineering':
        """
        Adiciona indicador de força da tendência

        Args:
            period: Período para cálculo
        """
        # ADX simplificado
        if not all(col in self.df.columns for col in ['high', 'low']):
            raise ValueError("Trend strength requer 'high' e 'low'")

        plus_dm = self.df['high'].diff()
        minus_dm = -self.df['low'].diff()

        plus_dm[plus_dm < 0] = 0
        minus_dm[minus_dm < 0] = 0

        self.df['trend_strength'] = (plus_dm.rolling(window=period).mean() -
                                     minus_dm.rolling(window=period).mean())
        return self

    # ===== PIPELINE COMPLETO =====

    def add_all_features(self) -> 'FeatureEngineering':
        """Adiciona todas as features disponíveis"""
        # Médias móveis
        self.add_sma([5, 10, 20, 50])
        self.add_ema([12, 26])

        # Momentum
        self.add_rsi()
        self.add_macd()
        self.add_roc()

        # Volatilidade
        self.add_bollinger_bands()
        if 'high' in self.df.columns and 'low' in self.df.columns:
            self.add_atr()

        # Volume
        if 'volume' in self.df.columns:
            self.add_obv()
            if 'high' in self.df.columns and 'low' in self.df.columns:
                self.add_vwap()

        # Returns
        self.add_returns([1, 5, 10])
        self.add_log_returns()

        # Trend
        if 'high' in self.df.columns and 'low' in self.df.columns:
            self.add_trend_strength()

        return self

    def get_dataframe(self) -> pd.DataFrame:
        """Retorna DataFrame com features"""
        return self.df.reset_index()

    def summary(self) -> dict:
        """Retorna sumário das features criadas"""
        feature_cols = [col for col in self.df.columns
                       if col not in ['open', 'high', 'low', 'close', 'volume', 'date']]

        return {
            'total_features': len(feature_cols),
            'features': feature_cols,
            'original_columns': ['datetime', 'open', 'high', 'low', 'close', 'volume'],
            'shape': self.df.shape,
            'date_range': f"{self.df.index[0]} → {self.df.index[-1]}"
        }
