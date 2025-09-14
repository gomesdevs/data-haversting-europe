"""
Teste simples da Alpha Vantage API para verificar se a chave está funcionando.
"""

import requests

def test_api_key():
    """Testa a chave da API diretamente."""

    # Sua chave API
    api_key = "0W1PO89YK9DIR9RP"

    # URL de teste direta
    url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=AAPL&apikey={api_key}"

    print(f"🔄 Testando URL: {url}")

    try:
        response = requests.get(url, timeout=30)
        print(f"📋 Status code: {response.status_code}")
        print(f"📋 Headers: {dict(response.headers)}")
        print(f"📋 Conteúdo (primeiros 500 chars):")
        print(response.text[:500])

        if response.status_code == 200:
            try:
                data = response.json()
                print(f"✅ JSON válido!")
                print(f"📋 Chaves: {list(data.keys())}")

                if 'Error Message' in data:
                    print(f"❌ Erro da Alpha Vantage: {data['Error Message']}")
                elif 'Information' in data:
                    print(f"ℹ️ Informação da Alpha Vantage: {data['Information']}")
                else:
                    print(f"✅ Dados recebidos com sucesso!")

            except Exception as e:
                print(f"❌ Erro ao parsear JSON: {e}")
        else:
            print(f"❌ Status HTTP não-200: {response.status_code}")

    except Exception as e:
        print(f"❌ Erro na requisição: {e}")

if __name__ == "__main__":
    test_api_key()