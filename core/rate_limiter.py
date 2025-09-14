import time
from typing import Optional
from core.logger import setup_logger


class RateLimiter:
    """
    Rate limiter para controlar velocidade de requisições ao Yahoo Finance.
    Evita bloqueios respeitando limites de requisições por minuto.
    """

    def __init__(self, requests_per_minute: int = 10):
        """
        Args:
            requests_per_minute: Número máximo de requisições por minuto (padrão: 15)
        """
        self.requests_per_minute = requests_per_minute
        self.min_interval = 60.0 / requests_per_minute  # segundos entre requisições
        self.last_request_time: Optional[float] = None
        self.logger = setup_logger("scraper.rate_limiter")

        self.logger.info(
            "Rate limiter inicializado",
            extra={
                "requests_per_minute": requests_per_minute,
                "min_interval_seconds": self.min_interval
            }
        )

    def acquire(self) -> None:
        """
        Espera o tempo necessário antes de permitir a próxima requisição.
        Chame este método antes de cada requisição HTTP.
        """
        current_time = time.time()

        if self.last_request_time is not None:
            # Calcula quanto tempo passou desde a última requisição
            time_since_last = current_time - self.last_request_time

            # Se não passou tempo suficiente, espera
            if time_since_last < self.min_interval:
                sleep_time = self.min_interval - time_since_last

                self.logger.info(
                    "Rate limiting ativo - aguardando intervalo",
                    extra={
                        "sleep_time_seconds": round(sleep_time, 2),
                        "time_since_last_request": round(time_since_last, 2),
                        "min_interval_required": self.min_interval
                    }
                )

                time.sleep(sleep_time)

        # Atualiza o timestamp da última requisição
        self.last_request_time = time.time()

        self.logger.debug("Requisição liberada pelo rate limiter")

    def reset(self) -> None:
        """
        Reseta o rate limiter, permitindo requisição imediata na próxima chamada.
        Útil para testes ou reinicialização.
        """
        self.last_request_time = None
        self.logger.info("Rate limiter resetado")

    def get_status(self) -> dict:
        """
        Retorna status atual do rate limiter.

        Returns:
            Dict com informações sobre o estado atual
        """
        current_time = time.time()

        if self.last_request_time is None:
            time_until_next = 0
        else:
            time_since_last = current_time - self.last_request_time
            time_until_next = max(0, self.min_interval - time_since_last)

        return {
            "requests_per_minute": self.requests_per_minute,
            "min_interval_seconds": self.min_interval,
            "time_until_next_request": round(time_until_next, 2),
            "can_request_now": time_until_next == 0
        }


# Instância global para uso em todo o projeto
default_rate_limiter = RateLimiter()
