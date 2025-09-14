"""
Teste do Chart Data Collector
=============================

Testa a coleta de dados históricos para forecasting.
"""

import pandas as pd
from endpoints.chart import ChartDataCollector
from core.rate_limiter import RateLimiter


def test_chart_collector_basic():
    """Teste básico do chart collector"""
    print("=== Teste: Chart Collector Básico ===")

    # Collector com rate limiting conservador
    collector = ChartDataCollector()

    print(f"Intervalos suportados: {list(collector.VALID_INTERVALS.keys())}")
    print(f"Períodos suportados: {list(collector.VALID_PERIODS.keys())}")
    print("✅ Collector inicializado com configurações para forecasting")


def test_historical_data_collection():
    """Teste de coleta de dados históricos"""
    print("\n=== Teste: Coleta de Dados Históricos ===")

    collector = ChartDataCollector()
    symbol = "ASML.AS"  # ASML - ação europeia líquida

    # Testar diferentes configurações para forecasting
    test_configs = [
        {"period": "1y", "interval": "1d", "desc": "1 ano diário"},
        {"period": "6mo", "interval": "1wk", "desc": "6 meses semanal"},
        {"period": "5y", "interval": "1mo", "desc": "5 anos mensal"}
    ]

    for config in test_configs:
        try:
            print(f"\n🔄 Testando {config['desc']} para {symbol}...")

            df = collector.get_historical_data(
                symbol=symbol,
                period=config["period"],
                interval=config["interval"]
            )

            print(f"   ✅ Sucesso: {len(df)} registros coletados")
            print(f"   📅 Período: {df['date'].min()} até {df['date'].max()}")
            print(f"   💰 Preço inicial: {df['close'].iloc[0]:.2f}")
            print(f"   💰 Preço final: {df['close'].iloc[-1]:.2f}")
            print(f"   📊 Colunas: {list(df.columns)}")

            # Verificar estrutura para forecasting
            required_cols = ['datetime', 'open', 'high', 'low', 'close', 'volume']
            missing_cols = [col for col in required_cols if col not in df.columns]

            if not missing_cols:
                print("   ✅ Todas as colunas necessárias para forecasting presentes")
            else:
                print(f"   ⚠️  Colunas ausentes: {missing_cols}")

        except Exception as e:
            print(f"   ❌ Falha: {type(e).__name__}: {e}")


def test_data_validation():
    """Teste de validação de dados"""
    print("\n=== Teste: Validação de Dados ===")

    collector = ChartDataCollector()
    symbol = "ASML.AS"

    try:
        print(f"Coletando dados com validação para {symbol}...")

        df = collector.get_historical_data(
            symbol=symbol,
            period="1y",
            interval="1d",
            validate=True
        )

        print(f"✅ Dados validados: {len(df)} registros")

        # Verificar qualidade dos dados para forecasting
        print("\n📊 Análise de qualidade dos dados:")
        print(f"   Records totais: {len(df)}")
        print(f"   Dados ausentes por coluna:")

        for col in ['open', 'high', 'low', 'close', 'volume']:
            missing = df[col].isna().sum()
            print(f"     {col}: {missing} ({missing/len(df)*100:.1f}%)")

        # Verificar consistência de preços
        price_issues = df[df['high'] < df['low']]
        print(f"   Inconsistências high < low: {len(price_issues)}")

        # Verificar preços negativos
        negative_prices = df[(df['open'] <= 0) | (df['close'] <= 0)].shape[0]
        print(f"   Preços <= 0: {negative_prices}")

        if len(price_issues) == 0 and negative_prices == 0:
            print("   ✅ Dados consistentes para forecasting")
        else:
            print("   ⚠️  Problemas de qualidade detectados")

    except Exception as e:
        print(f"❌ Falha na validação: {type(e).__name__}: {e}")


def test_latest_price():
    """Teste de coleta de preço atual"""
    print("\n=== Teste: Preço Atual ===")

    collector = ChartDataCollector()
    symbols = ["ASML.AS", "INGA.AS"]

    for symbol in symbols:
        try:
            print(f"🔄 Buscando preço atual de {symbol}...")

            price_data = collector.get_latest_price(symbol)

            print(f"   ✅ {symbol}:")
            print(f"     Preço: {price_data['price']:.2f} {price_data['currency']}")
            print(f"     Volume: {price_data['volume']:,}")
            print(f"     Data: {price_data['date']}")
            print(f"     Bolsa: {price_data['exchange']}")

        except Exception as e:
            print(f"   ❌ Falha para {symbol}: {type(e).__name__}: {e}")


def test_bulk_collection():
    """Teste de coleta em lote"""
    print("\n=== Teste: Coleta em Lote ===")

    collector = ChartDataCollector()

    # Símbolos europeus para teste
    symbols = ["ASML.AS", "INGA.AS", "HEIA.AS"]

    try:
        print(f"🔄 Coletando dados para {len(symbols)} símbolos...")

        results = collector.bulk_collect(
            symbols=symbols,
            period="3mo",
            interval="1d"
        )

        print(f"✅ Coleta em lote concluída!")
        print(f"   Sucessos: {len(results)}/{len(symbols)}")

        for symbol, df in results.items():
            print(f"   {symbol}: {len(df)} registros")

        if len(results) > 0:
            print("✅ Coleta em lote funcionando")
        else:
            print("⚠️  Nenhum símbolo coletado com sucesso")

    except Exception as e:
        print(f"❌ Falha na coleta em lote: {type(e).__name__}: {e}")


def test_forecasting_ready_data():
    """Teste específico para verificar se dados estão prontos para forecasting"""
    print("\n=== Teste: Dados Prontos para Forecasting ===")

    collector = ChartDataCollector()
    symbol = "ASML.AS"

    try:
        print(f"🔄 Verificando estrutura de dados para forecasting ({symbol})...")

        # Coletar dados de 2 anos para forecasting
        df = collector.get_historical_data(
            symbol=symbol,
            period="2y",
            interval="1d"
        )

        print(f"✅ Dados coletados: {len(df)} registros")

        # Verificações específicas para forecasting
        checks = []

        # 1. Dados suficientes (pelo menos 252 trading days = 1 ano)
        if len(df) >= 252:
            checks.append("✅ Dados suficientes para forecasting (≥252 days)")
        else:
            checks.append(f"⚠️  Poucos dados: {len(df)} registros")

        # 2. Colunas essenciais
        required = ['datetime', 'close', 'volume', 'open', 'high', 'low']
        missing = [col for col in required if col not in df.columns]
        if not missing:
            checks.append("✅ Todas as colunas necessárias presentes")
        else:
            checks.append(f"❌ Colunas ausentes: {missing}")

        # 3. Continuidade temporal
        gaps = df['datetime'].diff().dt.days.max()
        if gaps <= 7:  # Max 1 semana de gap
            checks.append("✅ Continuidade temporal adequada")
        else:
            checks.append(f"⚠️  Gaps temporais grandes: {gaps} dias")

        # 4. Preços válidos
        invalid_prices = df[df['close'] <= 0].shape[0]
        if invalid_prices == 0:
            checks.append("✅ Preços válidos")
        else:
            checks.append(f"❌ {invalid_prices} preços inválidos")

        print("\n📊 Checklist para Forecasting:")
        for check in checks:
            print(f"   {check}")

        # Mostrar amostra dos dados
        print(f"\n📈 Amostra dos dados (últimos 5 registros):")
        sample = df[['date', 'close', 'volume']].tail()
        for _, row in sample.iterrows():
            print(f"   {row['date']}: {row['close']:.2f} (vol: {row['volume']:,})")

    except Exception as e:
        print(f"❌ Falha: {type(e).__name__}: {e}")


if __name__ == "__main__":
    print("🚀 TESTANDO CHART DATA COLLECTOR PARA FORECASTING")
    print("Este teste verifica se os dados estão prontos para análise preditiva\n")

    test_chart_collector_basic()
    test_historical_data_collection()
    test_data_validation()
    test_latest_price()
    test_bulk_collection()
    test_forecasting_ready_data()

    print("\n✅ Testes do Chart Collector concluídos!")