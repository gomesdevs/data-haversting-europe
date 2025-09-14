import time
import random
import functools
from typing import Callable, Any, List, Type, Optional, Union
from core.logger import setup_logger


class RetryConfig:
    def __init__(
        self,
        max_attempts: int = 3,
        base_delay: float = 1.0,
        max_delay: float = 60.0,
        backoff_factor: float = 2.0,
        jitter: bool = True
    ):
        """
            max_attempts: Número máximo de tentativas (incluindo a primeira)
            base_delay: Delay inicial em segundos
            max_delay: Delay máximo em segundos
            backoff_factor: Fator de multiplicação para exponential backoff
            jitter: Se deve adicionar aleatoriedade ao delay
        """
        self.max_attempts = max_attempts
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.backoff_factor = backoff_factor
        self.jitter = jitter


class RetryableError(Exception):
    """Exceção que indica que a operação pode ser tentada novamente"""

    def __init__(self, message: str, original_error: Exception = None):
        super().__init__(message)
        self.original_error = original_error


class RetryHandler:
    """Classe principal para handling de retry com exponential backoff"""

    def __init__(self, config: RetryConfig = None):
        self.config = config or RetryConfig()
        self.logger = setup_logger("scraper.retry")

    def _calculate_delay(self, attempt: int) -> float:
        """
        Calcula o delay para a próxima tentativa usando exponential backoff

        Args:
            attempt: Número da tentativa atual (1-based)

        Returns:
            Delay em segundos
        """
        # Exponential backoff: base_delay * (backoff_factor ^ (attempt - 1))
        delay = self.config.base_delay * (self.config.backoff_factor ** (attempt - 1))

        # Aplica o limite máximo
        delay = min(delay, self.config.max_delay)

        # Adiciona jitter se habilitado (±25% de variação)
        if self.config.jitter:
            jitter_range = delay * 0.25
            delay += random.uniform(-jitter_range, jitter_range)
            delay = max(0, delay)  # Garante que não seja negativo

        return delay

    def _is_retryable_error(self, error: Exception) -> bool:
        """
        Determina se o erro é elegível para retry

        Args:
            error: A exceção que ocorreu

        Returns:
            True se deve tentar novamente, False caso contrário
        """
        # Se é nossa própria RetryableError, sempre retry
        if isinstance(error, RetryableError):
            return True

        # Erros de rede/timeout (requests library)
        import requests
        if isinstance(error, (requests.exceptions.Timeout,
                             requests.exceptions.ConnectionError,
                             requests.exceptions.ChunkedEncodingError)):
            return True

        # HTTP errors específicos
        if isinstance(error, requests.exceptions.HTTPError):
            status_code = error.response.status_code if error.response else None

            # 5xx: Server errors (temporários)
            if status_code and 500 <= status_code < 600:
                return True

            # 429: Too Many Requests
            if status_code == 429:
                return True

            # 408: Request Timeout
            if status_code == 408:
                return True

        return False

    def _calculate_delay_for_error(self, attempt: int, error: Exception) -> float:
        """
        Calcula delay específico baseado no tipo de erro.

        Args:
            attempt: Número da tentativa atual
            error: Erro que ocorreu

        Returns:
            Delay em segundos
        """
        # Para 429 (Too Many Requests), usar delay muito maior
        import requests
        if (isinstance(error, requests.exceptions.HTTPError) and
            hasattr(error, 'response') and error.response and
            error.response.status_code == 429):

            base_delay_429 = 30.0
            delay = base_delay_429 * attempt

            # Adiciona jitter para evitar thundering herd
            if self.config.jitter:
                jitter_range = delay * 0.25
                delay += random.uniform(-jitter_range, jitter_range)
                delay = max(15.0, delay)  # Mínimo 15s para 429

            return min(delay, 120.0)  # Máximo 2 minutos

        # Para outros erros, usar delay normal
        return self._calculate_delay(attempt)

    def execute(self, func: Callable, *args, **kwargs) -> Any:
        """
        Executa uma função com retry automático

        Args:
            func: Função a ser executada
            *args, **kwargs: Argumentos para a função

        Returns:
            Resultado da função

        Raises:
            A última exceção se todas as tentativas falharam
        """
        last_error = None

        for attempt in range(1, self.config.max_attempts + 1):
            try:
                self.logger.debug(
                    f"Executando tentativa {attempt}/{self.config.max_attempts}",
                    extra={
                        "attempt": attempt,
                        "max_attempts": self.config.max_attempts,
                        "function": func.__name__
                    }
                )

                result = func(*args, **kwargs)

                # Sucesso! Se não foi a primeira tentativa, log de recuperação
                if attempt > 1:
                    self.logger.info(
                        f"Operação bem-sucedida na tentativa {attempt}",
                        extra={
                            "attempt": attempt,
                            "function": func.__name__,
                            "recovered_from_errors": attempt - 1
                        }
                    )

                return result

            except Exception as error:
                last_error = error

                # Verifica se deve tentar novamente
                if not self._is_retryable_error(error) or attempt == self.config.max_attempts:
                    self.logger.error(
                        f"Falha definitiva na tentativa {attempt}/{self.config.max_attempts}",
                        extra={
                            "attempt": attempt,
                            "max_attempts": self.config.max_attempts,
                            "function": func.__name__,
                            "error_type": type(error).__name__,
                            "error_message": str(error),
                            "retryable": self._is_retryable_error(error)
                        }
                    )
                    raise error

                # Calcula delay para próxima tentativa (específico para o tipo de erro)
                delay = self._calculate_delay_for_error(attempt, error)

                # Log especial para 429s
                import requests
                is_429 = (isinstance(error, requests.exceptions.HTTPError) and
                         hasattr(error, 'response') and error.response and
                         error.response.status_code == 429)

                if is_429:
                    self.logger.warning(
                        f"HTTP 429 (Too Many Requests) - aguardando {delay:.0f}s antes da próxima tentativa",
                        extra={
                            "attempt": attempt,
                            "max_attempts": self.config.max_attempts,
                            "function": func.__name__,
                            "error_type": "HTTPError_429",
                            "delay_seconds": delay,
                            "next_attempt": attempt + 1,
                            "rate_limit_backoff": True
                        }
                    )
                else:
                    self.logger.warning(
                        f"Tentativa {attempt} falhou, tentando novamente em {delay:.2f}s",
                        extra={
                            "attempt": attempt,
                            "max_attempts": self.config.max_attempts,
                            "function": func.__name__,
                            "error_type": type(error).__name__,
                            "error_message": str(error),
                            "delay_seconds": delay,
                            "next_attempt": attempt + 1
                        }
                    )

                time.sleep(delay)

        # Isso nunca deveria ser alcançado, mas por segurança
        raise last_error


def with_retry(config: RetryConfig = None):
    """
    Decorator para adicionar retry automático a qualquer função

    Args:
        config: Configuração de retry (usa padrão se None)

    Returns:
        Decorator function
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            handler = RetryHandler(config)
            return handler.execute(func, *args, **kwargs)
        return wrapper
    return decorator


# Instância global padrão
default_retry_handler = RetryHandler()


# Função de conveniência
def retry_on_failure(func: Callable, *args, config: RetryConfig = None, **kwargs) -> Any:
    """
    Executa uma função com retry usando configuração personalizada

    Args:
        func: Função a ser executada
        *args, **kwargs: Argumentos para a função
        config: Configuração de retry (usa padrão se None)

    Returns:
        Resultado da função
    """
    handler = RetryHandler(config)
    return handler.execute(func, *args, **kwargs)
