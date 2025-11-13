"""
Módulo de Forecast
==================

Implementa modelos de previsão de séries temporais financeiras:
- Prophet (Facebook): Baseline robusto
- ARIMA/SARIMA: Modelos estatísticos clássicos
- LSTM: Deep Learning (opcional)

Features:
- Feature engineering (indicadores técnicos)
- Backtesting framework
- Métricas de performance
- Pipeline automatizado
"""

__version__ = '1.0.0'
__author__ = 'Data Harvesting Europe'

from .features import FeatureEngineering
from .metrics import ForecastMetrics

__all__ = [
    'FeatureEngineering',
    'ForecastMetrics',
]
