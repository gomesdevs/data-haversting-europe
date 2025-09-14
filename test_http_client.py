import json
from core.http_client import YahooFinanceClient
from core.rate_limiter import RateLimiter
from core.retry import RetryConfig


def test_client_initialization():
    """Teste de inicialização do cliente"""
    print("=== Teste: Inicialização do Cliente ===")

    # Cliente padrão
    client = YahooFinanceClient()
    print("✅ Cliente padrão inicializado")

    # Cliente personalizado
    custom_client = YahooFinanceClient(
        rate_limiter=RateLimiter(requests_per_minute=30),
        retry_config=RetryConfig(max_attempts=5),
        timeout=10
    )
    print("✅ Cliente personalizado inicializado")

    # Verificar headers stealth
    headers = client.session.headers
    print(f"   User-Agent: {headers.get('User-Agent', 'N/A')[:50]}...")
    print(f"   Headers stealth: {len(headers)} headers configurados")

    if 'Sec-Fetch-Mode' in headers:
        print("   ✅ Headers Sec-Fetch configurados")
    else:
        print("   ❌ Headers Sec-Fetch não encontrados")


def test_simple_request():
    """Teste de requisição simples"""
    print("\n=== Teste: Requisição Simples ===")

    client = YahooFinanceClient(timeout=10)

    try:
        # Teste com httpbin.org (serviço de teste)
        print("Testando requisição GET para httpbin.org...")
        response = client.get("https://httpbin.org/get")

        print(f"   Status: {response.status_code}")
        print(f"   Content-Length: {len(response.content)} bytes")

        # Verificar se nossos headers foram enviados
        data = response.json()
        sent_headers = data.get('headers', {})

        if 'Sec-Fetch-Mode' in sent_headers:
            print("   ✅ Headers stealth enviados com sucesso")
        else:
            print("   ⚠️  Headers stealth não detectados na resposta")

        print(f"   User-Agent enviado: {sent_headers.get('User-Agent', 'N/A')[:50]}...")

    except Exception as e:
        print(f"   ❌ Erro na requisição: {type(e).__name__}: {e}")


def test_yahoo_finance_request():
    """Teste de requisição real ao Yahoo Finance"""
    print("\n=== Teste: Requisição Yahoo Finance ===")

    client = YahooFinanceClient(
        rate_limiter=RateLimiter(requests_per_minute=60),  # Mais rápido para teste
        timeout=10
    )

    symbols = ["ASML.AS", "INGA.AS"]

    for symbol in symbols:
        try:
            print(f"Buscando dados para {symbol}...")

            # Usar método de conveniência
            data = client.get_yahoo_quote(symbol)

            # Extrair informações básicas
            chart = data.get('chart', {})
            result = chart.get('result', [{}])[0] if chart.get('result') else {}
            meta = result.get('meta', {})

            print(f"   ✅ Sucesso para {symbol}")
            print(f"   Símbolo: {meta.get('symbol', 'N/A')}")
            print(f"   Preço atual: {meta.get('regularMarketPrice', 'N/A')}")
            print(f"   Moeda: {meta.get('currency', 'N/A')}")

        except Exception as e:
            print(f"   ❌ Erro para {symbol}: {type(e).__name__}: {e}")


def test_rate_limiting_integration():
    """Teste de integração com rate limiting"""
    print("\n=== Teste: Integração Rate Limiting ===")

    # Rate limiter agressivo para ver o efeito
    client = YahooFinanceClient(
        rate_limiter=RateLimiter(requests_per_minute=12),  # 5s entre requests
        timeout=5
    )

    urls = [
        "https://httpbin.org/delay/1",
        "https://httpbin.org/get",
        "https://httpbin.org/user-agent"
    ]

    import time
    start_time = time.time()

    for i, url in enumerate(urls, 1):
        try:
            print(f"Requisição {i}/3 para {url.split('/')[-1]}...")
            response = client.get(url)
            print(f"   ✅ Status: {response.status_code}")

        except Exception as e:
            print(f"   ❌ Erro: {type(e).__name__}: {e}")

    total_time = time.time() - start_time
    print(f"   Tempo total: {total_time:.2f}s")

    # Com 12 req/min = 5s intervalo, 3 requests devem levar ~10s
    if total_time >= 8:  # Margem para latência
        print("   ✅ Rate limiting funcionando (tempo adequado)")
    else:
        print("   ⚠️  Rate limiting pode não estar funcionando (muito rápido)")


def test_context_manager():
    """Teste do context manager"""
    print("\n=== Teste: Context Manager ===")

    try:
        with YahooFinanceClient() as client:
            response = client.get("https://httpbin.org/get")
            print(f"   ✅ Requisição no context manager: {response.status_code}")

        print("   ✅ Context manager funcionando (sessão fechada)")

    except Exception as e:
        print(f"   ❌ Erro no context manager: {type(e).__name__}: {e}")


def test_error_handling():
    """Teste de tratamento de erros"""
    print("\n=== Teste: Tratamento de Erros ===")

    client = YahooFinanceClient(
        retry_config=RetryConfig(max_attempts=2, base_delay=0.5),
        timeout=1  # Timeout muito baixo para forçar erro
    )

    try:
        # URL que vai dar timeout
        print("Testando timeout (deve falhar após retries)...")
        response = client.get("https://httpbin.org/delay/3")
        print(f"   ❌ Não deveria ter sucesso: {response.status_code}")

    except Exception as e:
        print(f"   ✅ Erro esperado após retries: {type(e).__name__}")


if __name__ == "__main__":
    print("Testando Yahoo Finance HTTP Client...")
    print("⚠️  Alguns testes fazem requisições reais e podem demorar\n")

    test_client_initialization()
    test_simple_request()

    # Testes com Yahoo Finance (comente se quiser evitar requisições reais)
    test_yahoo_finance_request()
    test_rate_limiting_integration()

    test_context_manager()
    test_error_handling()

    print("\n✅ Todos os testes concluídos!")