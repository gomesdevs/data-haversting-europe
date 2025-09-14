import time
import random
from core.retry import RetryHandler, RetryConfig, with_retry, RetryableError
import requests


# Função que falha algumas vezes para testar retry
class TestAPI:
    def __init__(self):
        self.call_count = 0

    def unreliable_function(self):
        """Função que falha nas 2 primeiras tentativas"""
        self.call_count += 1
        print(f"   → Tentativa {self.call_count}")

        if self.call_count <= 2:
            raise requests.exceptions.ConnectionError("Simulated network error")

        return {"success": True, "data": "Yahoo Finance data"}

    def always_fails(self):
        """Função que sempre falha para testar limite de tentativas"""
        self.call_count += 1
        print(f"   → Tentativa {self.call_count} (sempre falha)")
        raise requests.exceptions.HTTPError("500 Server Error")

    def non_retryable_error(self):
        """Função que falha com erro não retryable"""
        self.call_count += 1
        print(f"   → Tentativa {self.call_count} (erro não retryable)")
        raise ValueError("Invalid parameter - não deve fazer retry")


def test_retry_success():
    """Teste de retry que consegue recuperar"""
    print("=== Teste: Retry com Sucesso ===")

    api = TestAPI()
    handler = RetryHandler(RetryConfig(max_attempts=3, base_delay=0.5))

    try:
        result = handler.execute(api.unreliable_function)
        print(f"✅ Sucesso após {api.call_count} tentativas!")
        print(f"   Resultado: {result}")
    except Exception as e:
        print(f"❌ Falhou: {e}")


def test_retry_failure():
    """Teste de retry que esgota todas as tentativas"""
    print("\n=== Teste: Retry que Esgota Tentativas ===")

    api = TestAPI()
    handler = RetryHandler(RetryConfig(max_attempts=3, base_delay=0.5))

    try:
        result = handler.execute(api.always_fails)
        print(f"❌ Não deveria ter sucesso: {result}")
    except Exception as e:
        print(f"✅ Falhou como esperado após {api.call_count} tentativas")
        print(f"   Erro final: {type(e).__name__}: {e}")


def test_retry_non_retryable():
    """Teste de erro que não deve fazer retry"""
    print("\n=== Teste: Erro Não Retryable ===")

    api = TestAPI()
    handler = RetryHandler(RetryConfig(max_attempts=3, base_delay=0.5))

    try:
        result = handler.execute(api.non_retryable_error)
        print(f"❌ Não deveria ter sucesso: {result}")
    except Exception as e:
        print(f"✅ Falhou imediatamente (sem retry) após {api.call_count} tentativa(s)")
        print(f"   Erro: {type(e).__name__}: {e}")


def test_retry_decorator():
    """Teste do decorator @with_retry"""
    print("\n=== Teste: Decorator @with_retry ===")

    call_count = 0

    @with_retry(RetryConfig(max_attempts=3, base_delay=0.5))
    def decorated_function():
        nonlocal call_count
        call_count += 1
        print(f"   → Tentativa {call_count} (decorator)")

        if call_count <= 2:
            raise requests.exceptions.Timeout("Simulated timeout")

        return "Decorator funcionou!"

    try:
        result = decorated_function()
        print(f"✅ Decorator funcionou após {call_count} tentativas!")
        print(f"   Resultado: {result}")
    except Exception as e:
        print(f"❌ Decorator falhou: {e}")


def test_exponential_backoff():
    """Teste para verificar o exponential backoff"""
    print("\n=== Teste: Exponential Backoff ===")

    config = RetryConfig(
        max_attempts=4,
        base_delay=0.5,
        backoff_factor=2.0,
        jitter=False  # Sem jitter para teste previsível
    )

    handler = RetryHandler(config)

    print("Delays esperados:")
    for attempt in range(1, 4):
        delay = handler._calculate_delay(attempt)
        expected = 0.5 * (2.0 ** (attempt - 1))
        print(f"   Tentativa {attempt}: {delay:.2f}s (esperado: {expected:.2f}s)")


def test_realistic_scenario():
    """Teste com cenário realista do Yahoo Finance"""
    print("\n=== Teste: Cenário Realista Yahoo Finance ===")

    def simulate_yahoo_request(symbol: str):
        """Simula requisição ao Yahoo Finance"""
        # 30% chance de falha temporária
        if random.random() < 0.3:
            errors = [
                requests.exceptions.Timeout("Yahoo Finance timeout"),
                requests.exceptions.HTTPError("429 Too Many Requests"),
                requests.exceptions.ConnectionError("Network error")
            ]
            raise random.choice(errors)

        return {"symbol": symbol, "price": round(random.uniform(100, 500), 2)}

    symbols = ["ASML.AS", "INGA.AS", "HEIA.AS"]
    handler = RetryHandler(RetryConfig(max_attempts=3, base_delay=1.0))

    for symbol in symbols:
        try:
            print(f"Coletando dados para {symbol}...")
            result = handler.execute(simulate_yahoo_request, symbol)
            print(f"   ✅ Sucesso: {result}")
        except Exception as e:
            print(f"   ❌ Falha final: {type(e).__name__}")


if __name__ == "__main__":
    print("Testando Sistema de Retry...")
    print("⚠️  Alguns testes incluem delays intencionais\n")

    test_retry_success()
    test_retry_failure()
    test_retry_non_retryable()
    test_retry_decorator()
    test_exponential_backoff()
    test_realistic_scenario()

    print("\n✅ Todos os testes concluídos!")