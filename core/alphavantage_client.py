"""
Alpha Vantage HTTP Client
========================

Cliente HTTP especializado para Alpha Vantage API.
Reutiliza toda infraestrutura existente: rate limiting, retry, logging.
"""

import requests
import time
from typing import Dict, Any, Optional
from core.http_client import YahooFinanceClient  # Vamos herdar e adaptar
from core.rate_limiter import RateLimiter
from core.retry import RetryConfig
from core.logger import setup_logger
from config.alphavantage_config import get_alphavantage_config, AlphaVantageConfig


class AlphaVantageClient:
    """
    Cliente HTTP especializado para Alpha Vantage API.
    Reutiliza toda infraestrutura: rate limiting, retry, logging estruturado.
    """

    def __init__(
        self,
        api_key: str = None,
        rate_limiter: RateLimiter = None,
        retry_config: RetryConfig = None,
        timeout: int = 30
    ):
        """
        Inicializa cliente Alpha Vantage.

        Args:
            api_key: API key (usa configuração padrão se None)
            rate_limiter: Rate limiter personalizado (usa padrão se None)
            retry_config: Configuração de retry
            timeout: Timeout em segundos
        """
        # Carregar configuração
        self.config = get_alphavantage_config()
        self.api_key = api_key or self.config.api_key

        # Rate limiter específico para Alpha Vantage (5 calls/min)
        if rate_limiter is None:
            rate_limiter = RateLimiter(requests_per_minute=self.config.RATE_LIMIT_CALLS_PER_MINUTE)

        # Configurar retry específico para Alpha Vantage
        if retry_config is None:
            retry_config = RetryConfig(
                max_attempts=3,
                base_delay=2.0,  # Alpha Vantage pode ser lenta
                backoff_factor=2.0
            )

        self.rate_limiter = rate_limiter
        self.timeout = timeout
        self.logger = setup_logger("scraper.alphavantage_client")

        # Configurar sessão HTTP reutilizável
        self.session = requests.Session()
        self._setup_session_headers()

        # Setup retry handler
        from core.retry import RetryHandler
        self.retry_handler = RetryHandler(retry_config)

        self.logger.info(
            "Alpha Vantage client inicializado",
            extra={
                "api_key_prefix": self.api_key[:8] + "..." if self.api_key else "None",
                "base_url": self.config.BASE_URL,
                "rate_limit_per_minute": self.config.RATE_LIMIT_CALLS_PER_MINUTE,
                "timeout": timeout
            }
        )

    def _setup_session_headers(self):
        """Configura headers apropriados para Alpha Vantage"""

        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate',  # Removido 'br' (Brotli)
            'Connection': 'keep-alive',
            'Cache-Control': 'no-cache'
        }

        self.session.headers.update(headers)

        self.logger.debug(
            "Headers configurados para Alpha Vantage",
            extra={"headers_count": len(headers)}
        )

    def _make_request(self, function: str, symbol: str = None, **kwargs) -> requests.Response:
        """
        Faz requisição para Alpha Vantage com rate limiting e retry.

        Args:
            function: Função da API (ex: 'TIME_SERIES_DAILY')
            symbol: Símbolo da ação (se aplicável)
            **kwargs: Parâmetros adicionais da API

        Returns:
            Response object
        """
        # Preparar parâmetros base
        params = {
            'function': function,
            'apikey': self.api_key
        }

        # Adicionar símbolo se fornecido
        if symbol:
            params['symbol'] = symbol

        # Adicionar parâmetros extras
        params.update(kwargs)

        # Request ID para tracking
        request_id = f"av_req_{int(time.time() * 1000)}"

        def _execute_request():
            """Função interna executada com retry"""

            # Rate limiting sempre aplicado
            self.rate_limiter.acquire()

            start_time = time.time()

            self.logger.debug(
                f"Executando requisição Alpha Vantage",
                extra={
                    "request_id": request_id,
                    "function": function,
                    "symbol": symbol,
                    "params_count": len(params),
                    "timeout": self.timeout
                }
            )

            try:
                response = self.session.get(
                    url=self.config.BASE_URL,
                    params=params,
                    timeout=self.timeout
                )

                # Verificar status HTTP
                response.raise_for_status()

                self.logger.debug(
                    f"Resposta Alpha Vantage recebida",
                    extra={
                        "status_code": response.status_code,
                        "content_type": response.headers.get('Content-Type'),
                        "content_length": len(response.content)
                    }
                )

                # Verificar se response é JSON válido
                try:
                    data = response.json()
                except ValueError:
                    # Log da resposta para debug
                    self.logger.error(
                        f"Resposta não é JSON válido",
                        extra={
                            "status_code": response.status_code,
                            "headers": dict(response.headers),
                            "content": response.text[:500],  # Primeiros 500 chars
                            "url": response.url
                        }
                    )
                    raise ValueError("Resposta não é JSON válido")

                response_time = time.time() - start_time

                # Verificar se Alpha Vantage retornou erro
                if 'Error Message' in data:
                    raise ValueError(f"Alpha Vantage Error: {data['Error Message']}")

                if 'Information' in data:
                    # Rate limiting da própria Alpha Vantage
                    if 'call frequency' in data['Information'].lower():
                        raise requests.exceptions.HTTPError("Alpha Vantage rate limit exceeded")
                    else:
                        # Outros avisos informativos
                        self.logger.warning(
                            "Alpha Vantage Information",
                            extra={
                                "request_id": request_id,
                                "information": data['Information']
                            }
                        )

                # Log de sucesso
                self.logger.info(
                    f"Requisição Alpha Vantage bem-sucedida",
                    extra={
                        "request_id": request_id,
                        "function": function,
                        "symbol": symbol,
                        "response_time_ms": round(response_time * 1000, 2),
                        "content_length": len(response.content),
                        "data_keys": list(data.keys()) if isinstance(data, dict) else "non-dict"
                    }
                )

                return response

            except Exception as e:
                response_time = time.time() - start_time

                self.logger.error(
                    f"Requisição Alpha Vantage falhou",
                    extra={
                        "request_id": request_id,
                        "function": function,
                        "symbol": symbol,
                        "error_type": type(e).__name__,
                        "error_message": str(e),
                        "response_time_ms": round(response_time * 1000, 2)
                    }
                )

                raise

        # Executar com retry automático
        return self.retry_handler.execute(_execute_request)

    def get_daily_data(self, symbol: str, outputsize: str = "compact") -> Dict[str, Any]:
        """
        Busca dados diários ajustados.

        Args:
            symbol: Símbolo da ação (ex: 'AAPL', 'ASML.AS')
            outputsize: 'compact' (100 points) ou 'full' (até 20 anos)

        Returns:
            Dados JSON da Alpha Vantage
        """
        response = self._make_request(
            function='TIME_SERIES_DAILY',  # Versão gratuita
            symbol=symbol,
            outputsize=outputsize
        )
        return response.json()

    def get_weekly_data(self, symbol: str) -> Dict[str, Any]:
        """
        Busca dados semanais ajustados.

        Args:
            symbol: Símbolo da ação

        Returns:
            Dados JSON da Alpha Vantage
        """
        response = self._make_request(
            function='TIME_SERIES_WEEKLY_ADJUSTED',
            symbol=symbol
        )
        return response.json()

    def get_monthly_data(self, symbol: str) -> Dict[str, Any]:
        """
        Busca dados mensais ajustados.

        Args:
            symbol: Símbolo da ação

        Returns:
            Dados JSON da Alpha Vantage
        """
        response = self._make_request(
            function='TIME_SERIES_MONTHLY_ADJUSTED',
            symbol=symbol
        )
        return response.json()

    def get_quote(self, symbol: str) -> Dict[str, Any]:
        """
        Busca cotação atual.

        Args:
            symbol: Símbolo da ação

        Returns:
            Dados JSON da Alpha Vantage
        """
        response = self._make_request(
            function='GLOBAL_QUOTE',
            symbol=symbol
        )
        return response.json()

    def get_data_for_interval(self, symbol: str, interval: str, outputsize: str = "compact") -> Dict[str, Any]:
        """
        Busca dados baseado no intervalo (compatível com chart.py).

        Args:
            symbol: Símbolo da ação
            interval: Intervalo ('1d', '1wk', '1mo')
            outputsize: Tamanho da resposta

        Returns:
            Dados JSON da Alpha Vantage
        """
        function = self.config.get_function_for_interval(interval)

        if function == 'TIME_SERIES_DAILY_ADJUSTED':
            return self.get_daily_data(symbol, outputsize)
        elif function == 'TIME_SERIES_WEEKLY_ADJUSTED':
            return self.get_weekly_data(symbol)
        elif function == 'TIME_SERIES_MONTHLY_ADJUSTED':
            return self.get_monthly_data(symbol)
        else:
            raise ValueError(f"Função não implementada: {function}")

    def close(self):
        """Fecha sessão HTTP"""
        if self.session:
            self.session.close()
            self.logger.info("Sessão Alpha Vantage fechada")

    def __enter__(self):
        """Context manager support"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager cleanup"""
        self.close()


# Instância global padrão
default_alphavantage_client = AlphaVantageClient()