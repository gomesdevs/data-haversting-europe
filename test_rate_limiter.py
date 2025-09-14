from datetime import datetime
import time
from core.rate_limiter import RateLimiter


def test_rate_limiter_basic():
    """Teste básico do rate limiter"""
    print("=== Teste Rate Limiter Básico ===")

    # Rate limiter bem agressivo para testar rapidamente (4 req/min = 15s intervalo)
    limiter = RateLimiter(requests_per_minute=4)

    print("1. Primeira requisição (deve ser imediata)")
    start_time = time.time()
    limiter.acquire()
    print(f"   Tempo decorrido: {time.time() - start_time:.2f}s")

    print("2. Segunda requisição (deve esperar ~15s)")
    start_time = time.time()
    limiter.acquire()
    elapsed = time.time() - start_time
    print(f"   Tempo decorrido: {elapsed:.2f}s")

    # Verifica se esperou o tempo correto (com margem de erro)
    expected_wait = 60 / 4  # 15 segundos
    if 14 <= elapsed <= 16:
        print("   ✅ Rate limiting funcionando corretamente!")
    else:
        print(f"   ❌ Esperado ~{expected_wait}s, mas aguardou {elapsed:.2f}s")


def test_rate_limiter_status():
    """Teste do status do rate limiter"""
    print("\n=== Teste Status do Rate Limiter ===")

    limiter = RateLimiter(requests_per_minute=15)  # 4s intervalo

    print("Status inicial:")
    status = limiter.get_status()
    print(f"   Pode fazer requisição agora: {status['can_request_now']}")
    print(f"   Tempo até próxima: {status['time_until_next_request']}s")

    print("\nFazendo uma requisição...")
    limiter.acquire()

    print("Status após requisição:")
    status = limiter.get_status()
    print(f"   Pode fazer requisição agora: {status['can_request_now']}")
    print(f"   Tempo até próxima: {status['time_until_next_request']}s")


def test_rate_limiter_realistic():
    """Teste com cenário realista"""
    print("\n=== Teste Cenário Realista ===")

    limiter = RateLimiter(requests_per_minute=60)  # 1s intervalo para teste rápido
    symbols = ["ASML.AS", "INGA.AS", "HEIA.AS"]

    print("Simulando coleta de dados para 3 símbolos...")

    start_total = time.time()
    for i, symbol in enumerate(symbols, 1):
        print(f"{i}. Coletando dados para {symbol}...")

        start_request = time.time()
        limiter.acquire()  # Rate limiting

        # Simula requisição HTTP
        print(f"   → Fazendo requisição para {symbol}")
        time.sleep(0.1)  # Simula tempo de resposta da API

        elapsed = time.time() - start_request
        print(f"   → Tempo total (rate limit + request): {elapsed:.2f}s")

    total_time = time.time() - start_total
    print(f"\nTempo total para {len(symbols)} símbolos: {total_time:.2f}s")


if __name__ == "__main__":
    print("Testando Rate Limiter...")
    print("⚠️  Alguns testes demoram devido ao rate limiting (isso é esperado!)\n")

    test_rate_limiter_status()
    test_rate_limiter_realistic()

    # Teste lento - descomente se quiser ver o rate limiting em ação
    # test_rate_limiter_basic()

    print("\n✅ Testes concluídos!")