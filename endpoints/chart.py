"""
Módulo especializado em coletar dados históricos de preços.
Suporta diferentes intervalos e períodos com validação robusta de dados.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Union, Tuple
from datetime import datetime, timedelta
import time

from core.http_client import YahooFinanceClient, default_client
from core.logger import setup_logger


class ChartDataCollector:
    """
    Coletor especializado em dados de charts/preços do Yahoo Finance.
    Otimizado para projetos de forecasting com validação robusta.
    """

    # Intervalos suportados
    VALID_INTERVALS = {
        '1d': 'Diário',
        '1wk': 'Semanal',
        '1mo': 'Mensal'
    }

    # Períodos suportados
    VALID_PERIODS = {
        '5d': '5 dias',
        '1mo': '1 mês',
        '3mo': '3 meses',
        '6mo': '6 meses',
        '1y': '1 ano',
        '2y': '2 anos',
        '5y': '5 anos',
        'max': 'Máximo disponível'
    }

    def __init__(self, client: YahooFinanceClient = None):
        """
        Inicializa o coletor de dados de chart.

        Args:
            client: Cliente HTTP personalizado (usa padrão se None)
        """
        self.client = client or default_client
        self.logger = setup_logger("scraper.endpoints.chart")

        self.logger.info(
            "Chart data collector inicializado",
            extra={
                "supported_intervals": list(self.VALID_INTERVALS.keys()),
                "supported_periods": list(self.VALID_PERIODS.keys()),
                "default_period": "5y",
                "default_interval": "1d"
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

    def _fetch_raw_data(self, symbol: str, period: str, interval: str) -> Dict:
        """
        Busca dados brutos do Yahoo Finance.

        Args:
            symbol: Símbolo da ação
            period: Período dos dados
            interval: Intervalo dos dados

        Returns:
            Dados brutos do Yahoo Finance

        Raises:
            Exception: Se falhar na coleta ou dados inválidos
        """
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"

        params = {
            'period1': None,  # Será calculado baseado no period
            'period2': int(time.time()),  # Até agora
            'interval': interval,
            'includePrePost': 'false',
            'events': 'div,splits'  # Incluir dividendos e splits
        }

        # Calcular period1 baseado no período solicitado
        now = datetime.now()
        if period == '5d':
            start_date = now - timedelta(days=5)
        elif period == '1mo':
            start_date = now - timedelta(days=30)
        elif period == '3mo':
            start_date = now - timedelta(days=90)
        elif period == '6mo':
            start_date = now - timedelta(days=180)
        elif period == '1y':
            start_date = now - timedelta(days=365)
        elif period == '2y':
            start_date = now - timedelta(days=730)
        elif period == '5y':
            start_date = now - timedelta(days=1825)
        else:  # max
            start_date = datetime(1970, 1, 1)  # Epoch

        params['period1'] = int(start_date.timestamp())

        self.logger.debug(
            f"Buscando dados para {symbol}",
            extra={
                "symbol": symbol,
                "period": period,
                "interval": interval,
                "start_date": start_date.isoformat(),
                "end_date": now.isoformat()
            }
        )

        response = self.client.get(url, params=params)
        data = response.json()

        # Validar estrutura da resposta
        if 'chart' not in data:
            raise ValueError(f"Resposta inválida do Yahoo Finance para {symbol}")

        chart = data['chart']

        if 'error' in chart and chart['error']:
            error_msg = chart['error'].get('description', 'Erro desconhecido')
            raise ValueError(f"Erro do Yahoo Finance para {symbol}: {error_msg}")

        if not chart.get('result') or len(chart['result']) == 0:
            raise ValueError(f"Nenhum dado encontrado para símbolo {symbol}")

        return data

    def _parse_to_dataframe(self, raw_data: Dict, symbol: str) -> pd.DataFrame:
        """
        Converte dados brutos em DataFrame estruturado.

        Args:
            raw_data: Dados brutos do Yahoo Finance
            symbol: Símbolo da ação

        Returns:
            DataFrame com dados de preços
        """
        result = raw_data['chart']['result'][0]

        # Extrair timestamps e dados de preços
        timestamps = result['timestamp']
        indicators = result['indicators']['quote'][0]

        # Criar DataFrame
        df = pd.DataFrame({
            'timestamp': timestamps,
            'open': indicators.get('open', []),
            'high': indicators.get('high', []),
            'low': indicators.get('low', []),
            'close': indicators.get('close', []),
            'volume': indicators.get('volume', [])
        })

        # Adicionar adjusted close se disponível
        if 'adjclose' in result['indicators']:
            adj_close = result['indicators']['adjclose'][0]['adjclose']
            df['adj_close'] = adj_close
        else:
            df['adj_close'] = df['close']  # Fallback para close

        # Converter timestamp para datetime
        df['datetime'] = pd.to_datetime(df['timestamp'], unit='s')
        df['date'] = df['datetime'].dt.date

        # Adicionar metadados
        meta = result['meta']
        df['symbol'] = symbol
        df['currency'] = meta.get('currency', 'Unknown')
        df['exchange'] = meta.get('exchangeName', 'Unknown')

        # Reordenar colunas para melhor usabilidade
        column_order = [
            'datetime', 'date', 'symbol', 'open', 'high',
            'low', 'close', 'adj_close', 'volume', 'currency', 'exchange'
        ]
        df = df[column_order]

        # Remover linhas com dados nulos (fins de semana, feriados)
        df = df.dropna(subset=['open', 'high', 'low', 'close'])

        # Ordenar por data
        df = df.sort_values('datetime').reset_index(drop=True)

        return df

    def _validate_dataframe(self, df: pd.DataFrame, symbol: str) -> Tuple[bool, List[str]]:
        """
        Valida qualidade dos dados no DataFrame.

        Args:
            df: DataFrame para validar
            symbol: Símbolo da ação

        Returns:
            Tuple (is_valid, list_of_issues)
        """
        issues = []

        # Verificar se há dados suficientes
        if len(df) < 2:
            issues.append(f"Dados insuficientes: apenas {len(df)} registros")

        # Verificar dados de preço básicos
        required_columns = ['open', 'high', 'low', 'close', 'volume']
        for col in required_columns:
            if col not in df.columns:
                issues.append(f"Coluna obrigatória ausente: {col}")
            elif df[col].isna().all():
                issues.append(f"Coluna {col} está completamente vazia")

        # Verificar lógica de preços (high >= low, etc.)
        if 'high' in df.columns and 'low' in df.columns:
            invalid_hl = df[df['high'] < df['low']]
            if len(invalid_hl) > 0:
                issues.append(f"{len(invalid_hl)} registros com high < low")

        # Verificar preços negativos
        price_columns = ['open', 'high', 'low', 'close']
        for col in price_columns:
            if col in df.columns:
                negative_prices = df[df[col] <= 0]
                if len(negative_prices) > 0:
                    issues.append(f"{len(negative_prices)} preços negativos/zero na coluna {col}")

        # Verificar continuidade temporal (gaps grandes)
        if len(df) > 1:
            time_diffs = df['datetime'].diff().dt.total_seconds()
            # Para dados diários, gap de mais de 7 dias é suspeito
            large_gaps = time_diffs[time_diffs > 7 * 24 * 3600]
            if len(large_gaps) > len(df) * 0.1:  # Mais de 10% gaps grandes
                issues.append(f"Muitos gaps temporais: {len(large_gaps)} gaps > 7 dias")

        is_valid = len(issues) == 0
        return is_valid, issues

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
                is_valid, issues = self._validate_dataframe(df, symbol)

                if not is_valid:
                    self.logger.warning(
                        f"Dados com problemas de qualidade para {symbol}",
                        extra={
                            "symbol": symbol,
                            "issues": issues,
                            "records_count": len(df)
                        }
                    )

                    # Decidir se rejeitar ou aceitar com warning
                    critical_issues = [issue for issue in issues if
                                     'ausente' in issue or 'insuficientes' in issue]

                    if critical_issues:
                        raise ValueError(f"Dados críticos inválidos para {symbol}: {critical_issues}")

            collection_time = time.time() - start_time

            self.logger.info(
                f"Dados históricos coletados com sucesso",
                extra={
                    "symbol": symbol,
                    "period": period,
                    "interval": interval,
                    "records_collected": len(df),
                    "date_range": f"{df['date'].min()} to {df['date'].max()}",
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
