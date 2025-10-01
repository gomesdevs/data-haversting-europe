"""
Chart Data Collector - Alpha Vantage
====================================

Módulo especializado em coletar dados históricos de preços para forecasting.
Migrado do Yahoo Finance para Alpha Vantage para maior confiabilidade.
Mantém exatamente a mesma interface externa.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Union, Tuple
from datetime import datetime, timedelta
import time

from core.alphavantage_client import AlphaVantageClient, default_alphavantage_client
from core.logger import setup_logger
from utils.validation import FinancialDataValidator, ValidationResult, Severity


class ChartDataCollector:
    """
    Coletor especializado em dados de charts/preços usando Alpha Vantage.
    Otimizado para projetos de forecasting com validação robusta.
    Mantém mesma interface do Yahoo Finance para compatibilidade.
    """

    # Intervalos suportados (para forecasting)
    VALID_INTERVALS = {
        '1d': 'Diário',
        '1wk': 'Semanal',
        '1mo': 'Mensal'
    }

    # Períodos suportados (Alpha Vantage específicos)
    VALID_PERIODS = {
        '5d': '5 dias',
        '1mo': '1 mês',
        '3mo': '3 meses',
        '6mo': '6 meses',
        '1y': '1 ano',
        '2y': '2 anos',
        '5y': '5 anos',
        'max': 'Máximo disponível (20+ anos)'
    }

    def __init__(self, client: AlphaVantageClient = None):
        """
        Inicializa o coletor de dados de chart.

        Args:
            client: Cliente Alpha Vantage personalizado (usa padrão se None)
        """
        self.client = client or default_alphavantage_client
        self.logger = setup_logger("scraper.endpoints.chart")
        self.validator = FinancialDataValidator(auto_correct=True)

        self.logger.info(
            "Chart data collector inicializado (Alpha Vantage)",
            extra={
                "supported_intervals": list(self.VALID_INTERVALS.keys()),
                "supported_periods": list(self.VALID_PERIODS.keys()),
                "default_period": "5y",
                "default_interval": "1d",
                "data_provider": "AlphaVantage"
            }
        )

    def _validate_parameters(self, symbol: str, period: str, interval: str) -> None:
        """
        Valida parâmetros de entrada.

        Args:
            symbol: Símbolo da ação
            period: Período solicitado
            interval: Intervalo solicitado

        Raises:
            ValueError: Se algum parâmetro for inválido
        """
        if not symbol or not isinstance(symbol, str):
            raise ValueError("Símbolo deve ser uma string não vazia")

        if interval not in self.VALID_INTERVALS:
            valid_intervals = ', '.join(self.VALID_INTERVALS.keys())
            raise ValueError(f"Intervalo '{interval}' inválido. Válidos: {valid_intervals}")

        if period not in self.VALID_PERIODS:
            valid_periods = ', '.join(self.VALID_PERIODS.keys())
            raise ValueError(f"Período '{period}' inválido. Válidos: {valid_periods}")

    def _determine_outputsize(self, period: str) -> str:
        """
        Determina outputsize do Alpha Vantage baseado no período.

        Args:
            period: Período solicitado

        Returns:
            'compact' ou 'full'
        """
        # Alpha Vantage compact = últimos 100 pontos de dados
        # Alpha Vantage full = até 20+ anos de dados

        if period in ['5d', '1mo', '3mo']:
            return 'compact'  # 100 pontos são suficientes
        else:
            return 'full'     # Precisamos de mais dados históricos

    def _fetch_raw_data(self, symbol: str, period: str, interval: str) -> Dict:
        """
        Busca dados brutos da Alpha Vantage.

        Args:
            symbol: Símbolo da ação
            period: Período dos dados
            interval: Intervalo dos dados

        Returns:
            Dados brutos da Alpha Vantage

        Raises:
            Exception: Se falhar na coleta ou dados inválidos
        """
        # Determinar outputsize baseado no período
        outputsize = self._determine_outputsize(period)

        self.logger.debug(
            f"Buscando dados Alpha Vantage para {symbol}",
            extra={
                "symbol": symbol,
                "period": period,
                "interval": interval,
                "outputsize": outputsize,
                "data_provider": "AlphaVantage"
            }
        )

        # Usar o cliente Alpha Vantage para buscar dados
        data = self.client.get_data_for_interval(symbol, interval, outputsize)

        # Validar estrutura da resposta Alpha Vantage
        if 'Error Message' in data:
            raise ValueError(f"Erro Alpha Vantage para {symbol}: {data['Error Message']}")

        # Verificar se temos dados de série temporal
        time_series_key = None
        for key in data.keys():
            if 'Time Series' in key:
                time_series_key = key
                break

        if not time_series_key or not data.get(time_series_key):
            available_keys = list(data.keys())
            raise ValueError(f"Nenhum dado de série temporal encontrado para {symbol}. Chaves disponíveis: {available_keys}")

        return data

    def _parse_to_dataframe(self, raw_data: Dict, symbol: str) -> pd.DataFrame:
        """
        Converte dados brutos da Alpha Vantage em DataFrame estruturado.

        Args:
            raw_data: Dados brutos da Alpha Vantage
            symbol: Símbolo da ação

        Returns:
            DataFrame com dados de preços (mesma estrutura do Yahoo Finance)
        """
        # Encontrar a chave de série temporal
        time_series_key = None
        for key in raw_data.keys():
            if 'Time Series' in key:
                time_series_key = key
                break

        if not time_series_key:
            raise ValueError(f"Chave de série temporal não encontrada para {symbol}")

        time_series_data = raw_data[time_series_key]

        # Extrair metadados se disponíveis
        metadata = raw_data.get('Meta Data', {})

        # Converter dados Alpha Vantage para formato DataFrame
        records = []

        for date_str, price_data in time_series_data.items():
            # Alpha Vantage usa diferentes formatos de chave dependendo do endpoint
            # Mapeamento para formato consistente

            if '1. open' in price_data:  # Formato daily/weekly/monthly
                record = {
                    'date': date_str,
                    'open': float(price_data.get('1. open', 0)),
                    'high': float(price_data.get('2. high', 0)),
                    'low': float(price_data.get('3. low', 0)),
                    'close': float(price_data.get('4. close', 0)),
                    'volume': int(float(price_data.get('5. volume', 0))),
                    'adj_close': float(price_data.get('5. adjusted close', price_data.get('4. close', 0)))  # Fallback para close se não tiver adjusted
                }
            elif 'open' in price_data:  # Formato alternativo
                record = {
                    'date': date_str,
                    'open': float(price_data.get('open', 0)),
                    'high': float(price_data.get('high', 0)),
                    'low': float(price_data.get('low', 0)),
                    'close': float(price_data.get('close', 0)),
                    'volume': int(float(price_data.get('volume', 0))),
                    'adj_close': float(price_data.get('adjusted close', price_data.get('close', 0)))
                }
            else:
                # Log das chaves disponíveis para debug
                available_keys = list(price_data.keys())
                self.logger.warning(
                    f"Formato inesperado de dados Alpha Vantage",
                    extra={
                        "symbol": symbol,
                        "date": date_str,
                        "available_keys": available_keys
                    }
                )
                continue

            records.append(record)

        if not records:
            raise ValueError(f"Nenhum registro válido encontrado para {symbol}")

        # Criar DataFrame
        df = pd.DataFrame(records)

        # Converter date string para datetime
        df['datetime'] = pd.to_datetime(df['date'])
        df['date'] = df['datetime'].dt.date

        # Adicionar metadados
        df['symbol'] = symbol
        df['currency'] = 'USD'  # Alpha Vantage padrão para TIME_SERIES_DAILY

        # Extrair exchange do símbolo se disponível (ex: ASML.AS = Amsterdam)
        if '.' in symbol:
            exchange_code = symbol.split('.')[1]
            exchange_map = {
                'AS': 'Amsterdam',
                'L': 'London',
                'DE': 'Frankfurt',
                'PA': 'Paris',
                'MI': 'Milan'
            }
            df['exchange'] = exchange_map.get(exchange_code, exchange_code)
        else:
            df['exchange'] = 'US'  # Símbolos sem sufixo são geralmente US

        # Reordenar colunas para mesma estrutura do Yahoo Finance
        column_order = [
            'datetime', 'date', 'symbol', 'open', 'high',
            'low', 'close', 'adj_close', 'volume', 'currency', 'exchange'
        ]
        df = df[column_order]

        # Remover linhas com dados nulos
        df = df.dropna(subset=['open', 'high', 'low', 'close'])

        # Ordenar por data (Alpha Vantage vem em ordem reversa)
        df = df.sort_values('datetime').reset_index(drop=True)

        return df

    def get_historical_data(
        self,
        symbol: str,
        period: str = "5y",
        interval: str = "1d",
        validate: bool = True
    ) -> pd.DataFrame:
        """
        Coleta dados históricos de preços para forecasting.

        Args:
            symbol: Símbolo da ação (ex: 'ASML.AS', 'INGA.AS')
            period: Período dos dados ('5y', '2y', '1y', '6mo', '3mo', '1mo', '5d')
            interval: Intervalo dos dados ('1d', '1wk', '1mo')
            validate: Se deve validar a qualidade dos dados

        Returns:
            DataFrame com dados históricos estruturados

        Raises:
            ValueError: Se parâmetros inválidos ou dados de baixa qualidade
            Exception: Se falhar na coleta
        """
        # Validar parâmetros
        self._validate_parameters(symbol, period, interval)

        start_time = time.time()

        self.logger.info(
            f"Iniciando coleta de dados históricos",
            extra={
                "symbol": symbol,
                "period": period,
                "interval": interval,
                "validate_data": validate
            }
        )

        try:
            # Buscar dados brutos
            raw_data = self._fetch_raw_data(symbol, period, interval)

            # Converter para DataFrame
            df = self._parse_to_dataframe(raw_data, symbol)

            # Validar dados se solicitado
            if validate:
                validation_result = self.validator.validate(df, symbol)

                # Log detalhado dos resultados da validação
                self.logger.info(
                    f"Validação de dados concluída para {symbol}",
                    extra={
                        "symbol": symbol,
                        "is_valid": validation_result.is_valid,
                        "total_issues": len(validation_result.issues),
                        "critical_issues": len(validation_result.critical_issues),
                        "warning_issues": len(validation_result.warning_issues),
                        "has_corrections": validation_result.corrected_data is not None
                    }
                )

                # Rejeitar se há issues críticas
                if not validation_result.is_valid:
                    critical_descriptions = [issue.description for issue in validation_result.critical_issues]
                    self.logger.error(
                        f"Dados rejeitados para {symbol} - issues críticas encontradas",
                        extra={
                            "symbol": symbol,
                            "critical_issues": critical_descriptions,
                            "total_records": len(df)
                        }
                    )
                    raise ValueError(f"Dados inválidos para {symbol}: {'; '.join(critical_descriptions)}")

                # Usar dados corrigidos se disponíveis
                if validation_result.corrected_data is not None:
                    df = validation_result.corrected_data
                    self.logger.info(
                        f"Usando dados corrigidos para {symbol}",
                        extra={
                            "symbol": symbol,
                            "corrections_applied": True,
                            "final_records": len(df)
                        }
                    )

                # Log warnings para monitoramento
                for warning_issue in validation_result.warning_issues:
                    self.logger.warning(
                        f"Issue de qualidade detectada: {warning_issue.description}",
                        extra=warning_issue.to_dict()
                    )

            collection_time = time.time() - start_time

            self.logger.info(
                f"Dados históricos coletados com sucesso",
                extra={
                    "symbol": symbol,
                    "period": period,
                    "interval": interval,
                    "records_collected": len(df),
                    "date_range": f"{df['datetime'].min()} to {df['datetime'].max()}" if 'datetime' in df.columns else "unknown",
                    "collection_time_seconds": round(collection_time, 2),
                    "data_quality": "validated" if validate else "not_validated"
                }
            )

            return df

        except Exception as e:
            self.logger.error(
                f"Falha na coleta de dados históricos",
                extra={
                    "symbol": symbol,
                    "period": period,
                    "interval": interval,
                    "error_type": type(e).__name__,
                    "error_message": str(e)
                }
            )
            raise

    def get_latest_price(self, symbol: str) -> Dict[str, Union[float, str]]:
        """
        Coleta o preço mais recente de uma ação.

        Args:
            symbol: Símbolo da ação

        Returns:
            Dict com preço atual e metadados
        """
        try:
            # Buscar dados dos últimos 5 dias para garantir dados recentes
            df = self.get_historical_data(symbol, period="5d", interval="1d", validate=False)

            if len(df) == 0:
                raise ValueError(f"Nenhum dado encontrado para {symbol}")

            # Pegar o registro mais recente
            latest = df.iloc[-1]

            result = {
                'symbol': symbol,
                'price': float(latest['close']),
                'adj_price': float(latest['adj_close']),
                'volume': int(latest['volume']) if not pd.isna(latest['volume']) else 0,
                'date': latest['date'].isoformat(),
                'currency': latest['currency'],
                'exchange': latest['exchange']
            }

            self.logger.info(
                f"Preço atual coletado",
                extra={
                    "symbol": symbol,
                    "price": result['price'],
                    "date": result['date']
                }
            )

            return result

        except Exception as e:
            self.logger.error(
                f"Falha na coleta de preço atual",
                extra={
                    "symbol": symbol,
                    "error_type": type(e).__name__,
                    "error_message": str(e)
                }
            )
            raise

    def bulk_collect(
        self,
        symbols: List[str],
        period: str = "5y",
        interval: str = "1d"
    ) -> Dict[str, pd.DataFrame]:
        """
        Coleta dados para múltiplos símbolos.

        Args:
            symbols: Lista de símbolos
            period: Período dos dados
            interval: Intervalo dos dados

        Returns:
            Dict {symbol: DataFrame}
        """
        results = {}
        failed_symbols = []

        self.logger.info(
            f"Iniciando coleta em lote",
            extra={
                "symbols_count": len(symbols),
                "symbols": symbols,
                "period": period,
                "interval": interval
            }
        )

        for i, symbol in enumerate(symbols, 1):
            try:
                self.logger.debug(f"Coletando {symbol} ({i}/{len(symbols)})")

                df = self.get_historical_data(symbol, period, interval)
                results[symbol] = df

            except Exception as e:
                failed_symbols.append(symbol)
                self.logger.warning(
                    f"Falha na coleta do símbolo {symbol}",
                    extra={
                        "symbol": symbol,
                        "error": str(e),
                        "position": f"{i}/{len(symbols)}"
                    }
                )

        self.logger.info(
            f"Coleta em lote concluída",
            extra={
                "total_symbols": len(symbols),
                "successful": len(results),
                "failed": len(failed_symbols),
                "failed_symbols": failed_symbols,
                "success_rate": round(len(results) / len(symbols) * 100, 1)
            }
        )

        return results


# Instância global padrão
default_chart_collector = ChartDataCollector()
