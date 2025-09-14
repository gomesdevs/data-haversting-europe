from datetime import datetime
from core.logger import logger, setup_logger

def test_basic_logging():
    print("=== Testando Logger Básico ===")

    # Testes básicos
    logger.debug("Mensagem de debug - pode não aparecer se level for INFO")
    logger.info("Sistema iniciado com sucesso")
    logger.warning("Rate limit próximo do limite")
    logger.error("Falha ao conectar com Yahoo Finance")

def test_structured_logging():
    """Teste do logging estruturado com extra data"""
    print("\n=== Testando Logger Estruturado ===")
    from logging import LoggerAdapter

    class StructuredLogger(LoggerAdapter):
        def process(self, msg, kwargs):
            if 'extra_data' in kwargs:
                # Move extra_data para extra
                if 'extra' not in kwargs:
                    kwargs['extra'] = {}
                kwargs['extra'].update(kwargs.pop('extra_data'))
            return msg, kwargs

    structured_logger = StructuredLogger(logger, {})

    # Teste com dados do scraper
    structured_logger.info(
        "Iniciando coleta de dados",
        extra_data={
            "symbol": "ASML.AS",
            "request_id": "req-123",
            "retry_attempt": 1
        }
    )

if __name__ == "__main__":
    test_basic_logging()
    test_structured_logging()

    print("\n=== Verificando arquivos ===")
    print(f"Arquivo de log deve estar em: logs/scraper_{datetime.now().strftime('%Y-%m-%d')}.ndjson")