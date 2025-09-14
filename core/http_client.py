import requests
import time
from typing import Dict, Any, Optional, Union
from urllib.parse import urljoin, urlparse
from core.logger import setup_logger
from core.rate_limiter import default_rate_limiter, RateLimiter
from core.retry import with_retry, RetryConfig, RetryHandler


class YahooFinanceClient:
    """
    Cliente HTTP especializado para Yahoo Finance com rate limiting,
    retry automático e headers stealth para evitar detecção de bot.
    """

    def __init__(
        self,
        rate_limiter: RateLimiter = None,
        retry_config: RetryConfig = None,
        timeout: int = 5
    ):
        """
        Inicializa o cliente HTTP.

        Args:
            rate_limiter: Rate limiter personalizado (usa padrão se None)
            retry_config: Configuração de retry (usa padrão se None)
            timeout: Timeout em segundos para requisições
        """
        self.rate_limiter = rate_limiter or default_rate_limiter
        self.retry_handler = RetryHandler(retry_config or RetryConfig())
        self.timeout = timeout
        self.logger = setup_logger("scraper.http_client")

        # Configurar sessão HTTP reutilizável para performance
        self.session = requests.Session()
        self._setup_session_headers()

        self.logger.info(
            "Yahoo Finance HTTP client inicializado",
            extra={
                "timeout_seconds": timeout,
                "rate_limiter_rpm": getattr(rate_limiter, 'requests_per_minute', 'default'),
                "retry_max_attempts": retry_config.max_attempts if retry_config else 3
            }
        )

    def _setup_session_headers(self):
        """Configura headers stealth para simular navegador real"""

        # User-Agent do Firefox mais recente
        user_agent = (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) "
            "Gecko/20100101 Firefox/119.0"
        )

        # Headers que simulam comportamento humano/navegador
        stealth_headers = {
            'User-Agent': user_agent,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',  # Do Not Track
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',

            # Headers Sec-Fetch para simular navegação real
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',

            # Cache control
            'Cache-Control': 'max-age=0'
        }

        self.session.headers.update(stealth_headers)

        self.logger.debug(
            "Headers stealth configurados",
            extra={
                "user_agent": user_agent,
                "headers_count": len(stealth_headers)
            }
        )

    def get(
        self,
        url: str,
        params: Dict[str, Any] = None,
        headers: Dict[str, str] = None,
        **kwargs
    ) -> requests.Response:
        """
        Faz GET request com rate limiting e retry automático.

        Args:
            url: URL para requisição
            params: Query parameters
            headers: Headers adicionais (serão merged com os padrão)
            **kwargs: Argumentos adicionais para requests

        Returns:
            Response object

        Raises:
            requests.RequestException: Se todas as tentativas falharam
        """
        return self._make_request('GET', url, params=params, headers=headers, **kwargs)

    def post(
        self,
        url: str,
        data: Any = None,
        json: Any = None,
        headers: Dict[str, str] = None,
        **kwargs
    ) -> requests.Response:
        """
        Faz POST request com rate limiting e retry automático.

        Args:
            url: URL para requisição
            data: Data para POST
            json: JSON data para POST
            headers: Headers adicionais
            **kwargs: Argumentos adicionais para requests

        Returns:
            Response object
        """
        return self._make_request('POST', url, data=data, json=json, headers=headers, **kwargs)

    def _make_request(
        self,
        method: str,
        url: str,
        **kwargs
    ) -> requests.Response:
        """
        Método interno que orquestra rate limiting, retry e logging.

        Args:
            method: HTTP method (GET, POST, etc.)
            url: URL para requisição
            **kwargs: Argumentos para requests

        Returns:
            Response object
        """
        # Preparar headers (merge com headers da sessão)
        headers = kwargs.pop('headers', {}) or {}

        # Adicionar Referer baseado na URL se não fornecido
        if 'Referer' not in headers and 'referer' not in headers:
            parsed_url = urlparse(url)
            base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
            headers['Referer'] = base_url

        # Configurar timeout se não fornecido
        timeout = kwargs.pop('timeout', self.timeout)

        # Request ID para tracking
        request_id = f"req_{int(time.time() * 1000)}"

        def _execute_request():
            """Função interna que será executada com retry"""

            # Rate limiting - sempre aplicado antes da requisição
            self.rate_limiter.acquire()

            start_time = time.time()

            self.logger.debug(
                f"Executando {method} request",
                extra={
                    "request_id": request_id,
                    "method": method,
                    "url": url,
                    "has_params": 'params' in kwargs,
                    "timeout": timeout
                }
            )

            try:
                # Fazer a requisição HTTP
                response = self.session.request(
                    method=method,
                    url=url,
                    headers=headers,
                    timeout=timeout,
                    **kwargs
                )

                response_time = time.time() - start_time

                # Log de sucesso
                self.logger.info(
                    f"{method} request bem-sucedida",
                    extra={
                        "request_id": request_id,
                        "method": method,
                        "url": url,
                        "status_code": response.status_code,
                        "response_time_ms": round(response_time * 1000, 2),
                        "content_length": len(response.content),
                        "content_type": response.headers.get('content-type', 'unknown')
                    }
                )

                # Verificar se status code indica erro
                response.raise_for_status()

                return response

            except requests.exceptions.RequestException as e:
                response_time = time.time() - start_time

                # Log de erro
                self.logger.error(
                    f"{method} request falhou",
                    extra={
                        "request_id": request_id,
                        "method": method,
                        "url": url,
                        "error_type": type(e).__name__,
                        "error_message": str(e),
                        "response_time_ms": round(response_time * 1000, 2),
                        "status_code": getattr(e.response, 'status_code', None) if hasattr(e, 'response') else None
                    }
                )

                raise  # Re-raise para o retry handler

        # Executar com retry automático
        return self.retry_handler.execute(_execute_request)

    def get_yahoo_quote(self, symbol: str) -> Dict[str, Any]:
        """
        Método de conveniência para buscar cotação de uma ação.

        Args:
            symbol: Símbolo da ação (ex: 'ASML.AS')

        Returns:
            Dict com dados da cotação
        """
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"

        params = {
            'interval': '1d',
            'range': '1d',
            'includePrePost': 'false'
        }

        response = self.get(url, params=params)
        return response.json()

    def close(self):
        """Fecha a sessão HTTP"""
        if self.session:
            self.session.close()
            self.logger.info("Sessão HTTP fechada")

    def __enter__(self):
        """Context manager support"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager cleanup"""
        self.close()


# Instância global padrão
default_client = YahooFinanceClient()
