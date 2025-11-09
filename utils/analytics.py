"""
Analytics engine usando DuckDB + Pandas para análise de dados financeiros.

Features:
- Análise de qualidade de dados (completude, gaps, issues)
- Métricas financeiras (retornos, volatilidade, drawdown)
- Análise de volume
- Comparações (original vs corrigido, símbolos)
- Queries SQL otimizadas via DuckDB
"""

import pandas as pd
import numpy as np
import duckdb
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union
from datetime import datetime, timedelta
import json

from storage.layout import StorageLayout
from storage.writer_parquet import ParquetWriter
from core.logger import setup_logger


class DataAnalyzer:
    """Engine de análise para dados financeiros armazenados"""
    
    def __init__(self, base_path: str = "data"):
        """
        Args:
            base_path: Diretório base onde dados estão armazenados
        """
        self.layout = StorageLayout(base_path)
        self.writer = ParquetWriter(base_path)
        self.logger = setup_logger('scraper.analytics')
        self.conn = duckdb.connect(':memory:')  # DuckDB in-memory
        
        self.logger.info(
            "DataAnalyzer inicializado",
            extra={"base_path": base_path}
        )
    
    # ========================================================================
    # QUALIDADE DE DADOS
    # ========================================================================
    
    def analyze_data_quality(
        self, 
        symbol: str, 
        year: int, 
        month: int
    ) -> Dict[str, any]:
        """
        Analisa qualidade dos dados salvos.
        
        Returns:
            Dict com métricas de qualidade:
            - completeness: % de dados completos
            - gaps_filled: número de gaps preenchidos pela validação
            - issues_summary: resumo de issues encontradas
            - date_range: período coberto
        """
        # Carregar dados
        df_original = self.writer.read(symbol, year, month, 'original')
        df_corrected = self.writer.read(symbol, year, month, 'corrected')
        metadata = self.writer.get_metadata(symbol, year, month)
        
        if df_original is None or metadata is None:
            return {"error": "Dados não encontrados"}
        
        # Calcular completude temporal
        start_date = pd.to_datetime(df_original['datetime'].min())
        end_date = pd.to_datetime(df_original['datetime'].max())
        
        # Dias de mercado esperados (seg-sex, excluindo feriados aproximado)
        total_days = (end_date - start_date).days + 1
        expected_trading_days = total_days * 5 / 7  # Aproximação
        
        completeness_original = (len(df_original) / expected_trading_days) * 100
        completeness_corrected = (len(df_corrected) / expected_trading_days) * 100 if df_corrected is not None else completeness_original
        
        # Gaps preenchidos
        gaps_filled = metadata['records'].get('added_by_correction', 0)
        
        # Análise de gaps
        gaps_info = self._analyze_gaps(df_original)
        
        quality_report = {
            "symbol": symbol,
            "period": f"{year}-{month:02d}",
            "date_range": {
                "start": str(start_date.date()),
                "end": str(end_date.date()),
                "total_days": total_days
            },
            "completeness": {
                "original_records": len(df_original),
                "corrected_records": len(df_corrected) if df_corrected is not None else len(df_original),
                "expected_trading_days": int(expected_trading_days),
                "completeness_original_pct": round(completeness_original, 2),
                "completeness_corrected_pct": round(completeness_corrected, 2)
            },
            "gaps": {
                "total_gaps": gaps_info['total_gaps'],
                "gaps_filled_by_validation": gaps_filled,
                "gap_distribution": gaps_info['gap_distribution'],
                "largest_gap_days": gaps_info['largest_gap']
            },
            "validation_issues": metadata['validation'],
            "data_integrity": {
                "has_nulls": self._check_nulls(df_corrected if df_corrected is not None else df_original),
                "has_duplicates": self._check_duplicates(df_corrected if df_corrected is not None else df_original),
                "is_sorted": self._check_sorted(df_corrected if df_corrected is not None else df_original)
            }
        }
        
        return quality_report
    
    def compare_original_vs_corrected(
        self, 
        symbol: str, 
        year: int, 
        month: int
    ) -> Dict[str, any]:
        """
        Compara dados originais vs corrigidos para mostrar impacto da validação.
        
        Returns:
            Dict com comparações detalhadas
        """
        df_original = self.writer.read(symbol, year, month, 'original')
        df_corrected = self.writer.read(symbol, year, month, 'corrected')
        
        if df_original is None or df_corrected is None:
            return {"error": "Dados não encontrados"}
        
        # Estatísticas básicas
        stats_comparison = {
            "records": {
                "original": len(df_original),
                "corrected": len(df_corrected),
                "added": len(df_corrected) - len(df_original)
            },
            "price_statistics": {
                "original": {
                    "mean_close": float(df_original['close'].mean()),
                    "std_close": float(df_original['close'].std()),
                    "min_close": float(df_original['close'].min()),
                    "max_close": float(df_original['close'].max())
                },
                "corrected": {
                    "mean_close": float(df_corrected['close'].mean()),
                    "std_close": float(df_corrected['close'].std()),
                    "min_close": float(df_corrected['close'].min()),
                    "max_close": float(df_corrected['close'].max())
                }
            },
            "volume_statistics": {
                "original": {
                    "mean_volume": int(df_original['volume'].mean()),
                    "total_volume": int(df_original['volume'].sum())
                },
                "corrected": {
                    "mean_volume": int(df_corrected['volume'].mean()),
                    "total_volume": int(df_corrected['volume'].sum())
                }
            }
        }
        
        return stats_comparison
    
    # ========================================================================
    # ANÁLISE FINANCEIRA
    # ========================================================================
    
    def calculate_returns(
        self, 
        symbol: str, 
        year: int, 
        month: int,
        period: str = 'daily'
    ) -> pd.DataFrame:
        """
        Calcula retornos (diário, semanal, mensal).
        
        Args:
            period: 'daily', 'weekly', 'monthly'
            
        Returns:
            DataFrame com retornos calculados
        """
        df = self.writer.read(symbol, year, month, 'corrected')
        
        if df is None:
            return pd.DataFrame()
        
        df = df.copy()
        df['datetime'] = pd.to_datetime(df['datetime'])
        df = df.sort_values('datetime')
        
        # Retorno diário
        df['daily_return'] = df['close'].pct_change()
        
        if period == 'weekly':
            df = df.resample('W', on='datetime').last()
            df['weekly_return'] = df['close'].pct_change()
        elif period == 'monthly':
            df = df.resample('M', on='datetime').last()
            df['monthly_return'] = df['close'].pct_change()
        
        return df
    
    def calculate_volatility(
        self, 
        symbol: str, 
        year: int, 
        month: int,
        window: int = 30,
        annualized: bool = True
    ) -> Dict[str, float]:
        """
        Calcula volatilidade.
        
        Args:
            window: Janela para rolling volatility
            annualized: Se True, anualiza a volatilidade
            
        Returns:
            Dict com métricas de volatilidade
        """
        df = self.calculate_returns(symbol, year, month, 'daily')
        
        if df.empty:
            return {}
        
        # Volatilidade histórica
        daily_vol = df['daily_return'].std()
        
        if annualized:
            # Anualizar: vol_diária * sqrt(252 dias de trading)
            annual_vol = daily_vol * np.sqrt(252)
        else:
            annual_vol = daily_vol
        
        # Rolling volatility
        df['rolling_vol'] = df['daily_return'].rolling(window=window).std()
        
        return {
            "daily_volatility": float(daily_vol),
            "annualized_volatility_pct": float(annual_vol * 100),
            "current_rolling_vol": float(df['rolling_vol'].iloc[-1]) if not df['rolling_vol'].isna().all() else None,
            "max_rolling_vol": float(df['rolling_vol'].max()) if not df['rolling_vol'].isna().all() else None,
            "min_rolling_vol": float(df['rolling_vol'].min()) if not df['rolling_vol'].isna().all() else None
        }
    
    def calculate_drawdown(
        self, 
        symbol: str, 
        year: int, 
        month: int
    ) -> Dict[str, any]:
        """
        Calcula maximum drawdown e drawdown atual.
        
        Returns:
            Dict com métricas de drawdown
        """
        df = self.writer.read(symbol, year, month, 'corrected')
        
        if df is None or len(df) == 0:
            return {}
        
        df = df.copy()
        df = df.sort_values('datetime')
        
        # Calcular running maximum
        df['running_max'] = df['close'].cummax()
        
        # Drawdown = (preço atual - máximo anterior) / máximo anterior
        df['drawdown'] = (df['close'] - df['running_max']) / df['running_max']
        
        # Maximum drawdown
        max_dd = df['drawdown'].min()
        max_dd_idx = df['drawdown'].idxmin()
        max_dd_date = df.loc[max_dd_idx, 'datetime']
        
        # Drawdown atual
        current_dd = df['drawdown'].iloc[-1]
        
        return {
            "maximum_drawdown_pct": float(max_dd * 100),
            "max_drawdown_date": str(max_dd_date),
            "current_drawdown_pct": float(current_dd * 100),
            "days_in_drawdown": self._count_drawdown_days(df)
        }
    
    def get_summary_statistics(
        self, 
        symbol: str, 
        year: int, 
        month: int
    ) -> Dict[str, any]:
        """
        Retorna estatísticas resumidas completas.
        
        Returns:
            Dict com todas as métricas principais
        """
        df = self.writer.read(symbol, year, month, 'corrected')
        
        if df is None:
            return {"error": "Dados não encontrados"}
        
        df = df.sort_values('datetime')
        
        # Retorno total
        total_return = ((df['close'].iloc[-1] - df['close'].iloc[0]) / df['close'].iloc[0]) * 100
        
        # Maiores alta/baixa
        max_price = df['high'].max()
        min_price = df['low'].min()
        max_price_date = df.loc[df['high'].idxmax(), 'datetime']
        min_price_date = df.loc[df['low'].idxmin(), 'datetime']
        
        # Volume
        avg_volume = df['volume'].mean()
        max_volume = df['volume'].max()
        max_volume_date = df.loc[df['volume'].idxmax(), 'datetime']
        
        summary = {
            "symbol": symbol,
            "period": f"{year}-{month:02d}",
            "date_range": {
                "start": str(df['datetime'].min()),
                "end": str(df['datetime'].max()),
                "trading_days": len(df)
            },
            "price_metrics": {
                "start_price": float(df['close'].iloc[0]),
                "end_price": float(df['close'].iloc[-1]),
                "total_return_pct": float(total_return),
                "highest_price": float(max_price),
                "highest_price_date": str(max_price_date),
                "lowest_price": float(min_price),
                "lowest_price_date": str(min_price_date),
                "price_change": float(df['close'].iloc[-1] - df['close'].iloc[0])
            },
            "volume_metrics": {
                "average_volume": int(avg_volume),
                "max_volume": int(max_volume),
                "max_volume_date": str(max_volume_date),
                "total_volume": int(df['volume'].sum())
            }
        }
        
        # Adicionar volatilidade
        vol_metrics = self.calculate_volatility(symbol, year, month)
        summary["volatility"] = vol_metrics
        
        # Adicionar drawdown
        dd_metrics = self.calculate_drawdown(symbol, year, month)
        summary["drawdown"] = dd_metrics
        
        return summary
    
    # ========================================================================
    # ANÁLISE DE VOLUME
    # ========================================================================
    
    def analyze_volume(
        self, 
        symbol: str, 
        year: int, 
        month: int
    ) -> Dict[str, any]:
        """
        Analisa padrões de volume.
        
        Returns:
            Dict com análise de volume
        """
        df = self.writer.read(symbol, year, month, 'corrected')
        
        if df is None:
            return {}
        
        # Estatísticas de volume
        mean_vol = df['volume'].mean()
        std_vol = df['volume'].std()
        
        # Dias com volume anômalo (> 2 desvios padrão)
        df['volume_zscore'] = (df['volume'] - mean_vol) / std_vol
        anomalous_volume_days = df[abs(df['volume_zscore']) > 2]
        
        # Dias com volume zero
        zero_volume_days = df[df['volume'] == 0]
        
        return {
            "average_volume": int(mean_vol),
            "std_volume": int(std_vol),
            "median_volume": int(df['volume'].median()),
            "max_volume": int(df['volume'].max()),
            "min_volume": int(df['volume'].min()),
            "zero_volume_days": len(zero_volume_days),
            "anomalous_volume_days": len(anomalous_volume_days),
            "volume_concentration": {
                "top_10_days_volume_pct": float((df.nlargest(10, 'volume')['volume'].sum() / df['volume'].sum()) * 100)
            }
        }
    
    # ========================================================================
    # COMPARAÇÕES E CORRELAÇÕES
    # ========================================================================
    
    def compare_symbols(
        self, 
        symbols: List[str], 
        year: int, 
        month: int,
        metric: str = 'close'
    ) -> pd.DataFrame:
        """
        Compara múltiplos símbolos.
        
        Args:
            symbols: Lista de símbolos
            metric: Métrica a comparar ('close', 'volume', 'returns')
            
        Returns:
            DataFrame com comparação
        """
        dfs = []
        
        for symbol in symbols:
            df = self.writer.read(symbol, year, month, 'corrected')
            if df is not None:
                df = df[['datetime', metric]].copy()
                df = df.rename(columns={metric: symbol})
                dfs.append(df)
        
        if not dfs:
            return pd.DataFrame()
        
        # Merge all dataframes
        result = dfs[0]
        for df in dfs[1:]:
            result = result.merge(df, on='datetime', how='outer')
        
        result = result.sort_values('datetime')
        
        return result
    
    def correlation_matrix(
        self, 
        symbols: List[str], 
        year: int, 
        month: int
    ) -> pd.DataFrame:
        """
        Calcula matriz de correlação entre símbolos.
        
        Returns:
            DataFrame com matriz de correlação
        """
        # Pegar retornos diários de todos os símbolos
        returns_dict = {}
        
        for symbol in symbols:
            df = self.calculate_returns(symbol, year, month, 'daily')
            if not df.empty:
                returns_dict[symbol] = df.set_index('datetime')['daily_return']
        
        if not returns_dict:
            return pd.DataFrame()
        
        # Criar DataFrame de retornos
        returns_df = pd.DataFrame(returns_dict)
        
        # Calcular correlação
        corr_matrix = returns_df.corr()
        
        return corr_matrix
    
    # ========================================================================
    # ANÁLISE TEMPORAL (Períodos)
    # ========================================================================
    
    def compare_periods(
        self,
        symbol: str,
        periods: List[Tuple[int, int]],  # [(year, month), ...]
        metric: str = 'return'
    ) -> Dict[str, any]:
        """
        Compara métricas entre diferentes períodos.
        
        Args:
            periods: Lista de tuplas (ano, mês)
            metric: 'return', 'volatility', 'volume'
            
        Returns:
            Dict com comparação entre períodos
        """
        results = {}
        
        for year, month in periods:
            period_key = f"{year}-{month:02d}"
            
            if metric == 'return':
                summary = self.get_summary_statistics(symbol, year, month)
                if 'error' not in summary:
                    results[period_key] = summary['price_metrics']['total_return_pct']
            
            elif metric == 'volatility':
                vol = self.calculate_volatility(symbol, year, month)
                if vol:
                    results[period_key] = vol['annualized_volatility_pct']
            
            elif metric == 'volume':
                vol_analysis = self.analyze_volume(symbol, year, month)
                if vol_analysis:
                    results[period_key] = vol_analysis['average_volume']
        
        return results
    
    # ========================================================================
    # HELPERS INTERNOS
    # ========================================================================
    
    def _analyze_gaps(self, df: pd.DataFrame) -> Dict[str, any]:
        """Analisa gaps temporais nos dados"""
        df = df.sort_values('datetime')
        df['datetime'] = pd.to_datetime(df['datetime'])
        
        # Calcular diferenças entre datas consecutivas
        date_diffs = df['datetime'].diff().dt.days
        
        # Gaps (diferença > 1 dia)
        gaps = date_diffs[date_diffs > 1]
        
        # Distribuição de gaps
        gap_distribution = {}
        for gap_size in range(2, int(gaps.max()) + 1 if len(gaps) > 0 else 2):
            count = len(gaps[gaps == gap_size])
            if count > 0:
                gap_distribution[f"{gap_size}_days"] = count
        
        return {
            "total_gaps": len(gaps),
            "gap_distribution": gap_distribution,
            "largest_gap": int(gaps.max()) if len(gaps) > 0 else 0
        }
    
    def _check_nulls(self, df: pd.DataFrame) -> Dict[str, int]:
        """Verifica nulls em colunas críticas"""
        critical_cols = ['open', 'high', 'low', 'close', 'volume']
        nulls = {}
        for col in critical_cols:
            if col in df.columns:
                nulls[col] = int(df[col].isna().sum())
        return nulls
    
    def _check_duplicates(self, df: pd.DataFrame) -> int:
        """Conta duplicatas por data"""
        return int(df.duplicated(subset=['date']).sum())
    
    def _check_sorted(self, df: pd.DataFrame) -> bool:
        """Verifica se dados estão ordenados"""
        return bool(pd.to_datetime(df['datetime']).is_monotonic_increasing)
    
    def _count_drawdown_days(self, df: pd.DataFrame) -> int:
        """Conta dias consecutivos em drawdown"""
        # Contar quantos dias estamos abaixo do máximo
        in_drawdown = df['drawdown'] < 0
        return int(in_drawdown.sum())
