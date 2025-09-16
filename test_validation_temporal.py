import sys
import os
import pandas as pd
from datetime import datetime, timedelta

# Adicionar o diretório raiz ao path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.validation import FinancialDataValidator, Severity

def create_base_df(dates, symbol="TEST"):
    """Cria DataFrame base com datas específicas"""
    return pd.DataFrame({
        'datetime': pd.to_datetime(dates),
        'date': [d.strftime('%Y-%m-%d') if hasattr(d, 'strftime') else str(d) for d in dates],
        'symbol': [symbol] * len(dates),
        'open': [100.0] * len(dates),
        'high': [105.0] * len(dates),
        'low': [95.0] * len(dates),
        'close': [102.0] * len(dates),
        'adj_close': [102.0] * len(dates),
        'volume': [1000] * len(dates),
        'currency': ['USD'] * len(dates),
        'exchange': ['US'] * len(dates)
    })

def test_temporal_validations():
    """Testa as validações temporais com diferentes cenários"""

    validator = FinancialDataValidator(auto_correct=False)

    print("🔄 Testando validações temporais...")

    # 1. Dados com sequência normal (dias úteis)
    print("\n1️⃣ Teste: Sequência temporal normal")
    normal_dates = [
        '2025-01-06',  # Segunda
        '2025-01-07',  # Terça
        '2025-01-08',  # Quarta
        '2025-01-09',  # Quinta
        '2025-01-10',  # Sexta
        '2025-01-13',  # Segunda (gap de fim de semana)
        '2025-01-14'   # Terça
    ]
    normal_df = create_base_df(normal_dates)
    result = validator._validate_temporal_sequence(normal_df, "TEST")
    print(f"Issues encontradas: {len(result)}")
    for issue in result:
        print(f"  - {issue.severity.value}: {issue.description}")

    # 2. Datas duplicadas (CRITICAL)
    print("\n2️⃣ Teste: Datas duplicadas")
    duplicate_dates = [
        '2025-01-06',
        '2025-01-07',
        '2025-01-07',  # Duplicata
        '2025-01-08',
        '2025-01-08'   # Outra duplicata
    ]
    duplicate_df = create_base_df(duplicate_dates)
    result = validator._validate_temporal_sequence(duplicate_df, "TEST")
    print(f"Issues encontradas: {len(result)}")
    for issue in result:
        print(f"  - {issue.severity.value}: {issue.description}")

    # 3. Datas fora de ordem (WARNING)
    print("\n3️⃣ Teste: Datas fora de ordem")
    unordered_dates = [
        '2025-01-06',
        '2025-01-08',  # Pula o dia 7
        '2025-01-07',  # Volta no tempo
        '2025-01-10',
        '2025-01-09'   # Volta no tempo novamente
    ]
    unordered_df = create_base_df(unordered_dates)
    result = validator._validate_temporal_sequence(unordered_df, "TEST")
    print(f"Issues encontradas: {len(result)}")
    for issue in result:
        print(f"  - {issue.severity.value}: {issue.description}")

    # 4. Gaps suspeitos (> 7 dias)
    print("\n4️⃣ Teste: Gaps suspeitos na série temporal")
    gap_dates = [
        '2025-01-06',
        '2025-01-07',
        '2025-01-20',  # Gap de 13 dias
        '2025-01-21',
        '2025-02-10'   # Gap de 20 dias
    ]
    gap_df = create_base_df(gap_dates)
    result = validator._validate_temporal_sequence(gap_df, "TEST")
    print(f"Issues encontradas: {len(result)}")
    for issue in result:
        print(f"  - {issue.severity.value}: {issue.description}")

    # 5. Negociação em fins de semana (WARNING)
    print("\n5️⃣ Teste: Negociação em fins de semana")
    weekend_dates = [
        '2025-01-06',  # Segunda
        '2025-01-07',  # Terça
        '2025-01-11',  # Sábado - suspeito!
        '2025-01-12',  # Domingo - suspeito!
        '2025-01-13'   # Segunda
    ]
    weekend_df = create_base_df(weekend_dates)
    result = validator._validate_temporal_sequence(weekend_df, "TEST")
    print(f"Issues encontradas: {len(result)}")
    for issue in result:
        print(f"  - {issue.severity.value}: {issue.description}")

    # 6. Série com muitos gaps (INFO)
    print("\n6️⃣ Teste: Série com muitos gaps")
    sparse_dates = [
        '2025-01-01',
        '2025-01-05',  # Gap 4 dias
        '2025-01-10',  # Gap 5 dias
        '2025-01-15',  # Gap 5 dias
        '2025-01-20'   # Gap 5 dias
    ]
    sparse_df = create_base_df(sparse_dates)
    result = validator._validate_temporal_sequence(sparse_df, "TEST")
    print(f"Issues encontradas: {len(result)}")
    for issue in result:
        print(f"  - {issue.severity.value}: {issue.description}")

    print("\n✅ Teste das validações temporais concluído!")

if __name__ == "__main__":
    test_temporal_validations()