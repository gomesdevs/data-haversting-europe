import sys
import os
import pandas as pd
from datetime import datetime

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.validation import FinancialDataValidator, Severity

def test_basic_validations():
    """Testa as validações básicas com diferentes cenários"""

    validator = FinancialDataValidator(auto_correct=False)

    print("🔄 Testando validações básicas...")

    # 1. Teste com DataFrame vazio
    print("\n1️⃣ Teste: DataFrame vazio")
    empty_df = pd.DataFrame()
    result = validator._validate_basic_structure(empty_df, "TEST")

    print(f"Issues encontradas: {len(result)}")
    for issue in result:
        print(f"  - {issue.severity.value}: {issue.description}")

    # 2. Teste com colunas faltando
    print("\n2️⃣ Teste: Colunas obrigatórias faltando")
    incomplete_df = pd.DataFrame({
        'open': [100.0, 101.0],
        'close': [102.0, 103.0]
    })
    result = validator._validate_basic_structure(incomplete_df, "TEST")

    print(f"Issues encontradas: {len(result)}")
    for issue in result:
        print(f"  - {issue.severity.value}: {issue.description}")

    # 3. Teste com tipos incorretos
    print("\n3️⃣ Teste: Tipos de dados incorretos")
    wrong_types_df = pd.DataFrame({
        'datetime': ['2025-01-01', '2025-01-02'],  # String em vez de datetime
        'date': ['2025-01-01', '2025-01-02'],
        'symbol': ['TEST', 'TEST'],
        'open': ['100.0', '101.0'],  # String em vez de float
        'high': [105.0, 106.0],
        'low': [95.0, 96.0],
        'close': [102.0, 103.0],
        'adj_close': [102.0, 103.0],
        'volume': [1000, 1100],
        'currency': ['USD', 'USD'],
        'exchange': ['US', 'US']
    })
    result = validator._validate_basic_structure(wrong_types_df, "TEST")

    print(f"Issues encontradas: {len(result)}")
    for issue in result:
        print(f"  - {issue.severity.value}: {issue.description}")

    # 4. Teste com dados válidos (quantidade suficiente)
    print("\n4️⃣ Teste: Dados completamente válidos")
    valid_df = pd.DataFrame({
        'datetime': pd.to_datetime(['2025-01-01', '2025-01-02', '2025-01-03', '2025-01-04', '2025-01-05', '2025-01-06']),
        'date': ['2025-01-01', '2025-01-02', '2025-01-03', '2025-01-04', '2025-01-05', '2025-01-06'],
        'symbol': ['TEST'] * 6,
        'open': [100.0, 101.0, 102.0, 103.0, 104.0, 105.0],
        'high': [105.0, 106.0, 107.0, 108.0, 109.0, 110.0],
        'low': [95.0, 96.0, 97.0, 98.0, 99.0, 100.0],
        'close': [102.0, 103.0, 104.0, 105.0, 106.0, 107.0],
        'adj_close': [102.0, 103.0, 104.0, 105.0, 106.0, 107.0],
        'volume': [1000, 1100, 1200, 1300, 1400, 1500],
        'currency': ['USD'] * 6,
        'exchange': ['US'] * 6
    })
    result = validator._validate_basic_structure(valid_df, "TEST")

    print(f"Issues encontradas: {len(result)}")
    for issue in result:
        print(f"  - {issue.severity.value}: {issue.description}")

    if len(result) == 0:
        print("  ✅ Nenhuma issue encontrada - dados válidos!")

    print("\n✅ Teste das validações básicas concluído!")

if __name__ == "__main__":
    test_basic_validations()