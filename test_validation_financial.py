import sys
import os
import pandas as pd
import numpy as np

# Adicionar o diretório raiz ao path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.validation import FinancialDataValidator, Severity

def test_financial_validations():
    """Testa as validações financeiras OHLCV com diferentes cenários"""

    validator = FinancialDataValidator(auto_correct=False)

    print("🔄 Testando validações financeiras OHLCV...")

    # 1. Dados válidos
    print("\n1️⃣ Teste: Dados OHLCV válidos")
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
    result = validator._validate_financial_consistency(valid_df, "TEST")
    print(f"Issues encontradas: {len(result)}")
    for issue in result:
        print(f"  - {issue.severity.value}: {issue.description}")

    # 2. Preços negativos (CRITICAL)
    print("\n2️⃣ Teste: Preços negativos")
    negative_df = valid_df.copy()
    negative_df.loc[1, 'open'] = -50.0  # Preço negativo
    negative_df.loc[2, 'close'] = 0.0   # Preço zero
    result = validator._validate_financial_consistency(negative_df, "TEST")
    print(f"Issues encontradas: {len(result)}")
    for issue in result:
        print(f"  - {issue.severity.value}: {issue.description}")

    # 3. Volume zero e negativo
    print("\n3️⃣ Teste: Volume zero e negativo")
    volume_df = valid_df.copy()
    volume_df.loc[1, 'volume'] = 0      # Volume zero (WARNING)
    volume_df.loc[2, 'volume'] = -100   # Volume negativo (CRITICAL)
    result = validator._validate_financial_consistency(volume_df, "TEST")
    print(f"Issues encontradas: {len(result)}")
    for issue in result:
        print(f"  - {issue.severity.value}: {issue.description}")

    # 4. Inconsistências OHLCV
    print("\n4️⃣ Teste: Inconsistências OHLCV")
    inconsistent_df = valid_df.copy()
    inconsistent_df.loc[1, 'low'] = 110.0   # Low > High (CRITICAL)
    inconsistent_df.loc[2, 'low'] = 105.0   # Low > Open (WARNING)
    inconsistent_df.loc[3, 'high'] = 95.0   # High < Close (WARNING)
    result = validator._validate_financial_consistency(inconsistent_df, "TEST")
    print(f"Issues encontradas: {len(result)}")
    for issue in result:
        print(f"  - {issue.severity.value}: {issue.description}")

    # 5. Preços todos iguais (muito raro)
    print("\n5️⃣ Teste: Preços OHLC idênticos")
    equal_df = valid_df.copy()
    equal_df.loc[1, ['open', 'high', 'low', 'close']] = 100.0  # Todos iguais
    result = validator._validate_financial_consistency(equal_df, "TEST")
    print(f"Issues encontradas: {len(result)}")
    for issue in result:
        print(f"  - {issue.severity.value}: {issue.description}")

    # 6. Teste de tolerância (arredondamento)
    print("\n6️⃣ Teste: Tolerância de arredondamento")
    tolerance_df = valid_df.copy()
    tolerance_df.loc[1, 'low'] = 100.005   # Diferença de 0.005 (menor que tolerância 0.01)
    tolerance_df.loc[1, 'open'] = 100.0
    result = validator._validate_financial_consistency(tolerance_df, "TEST")
    print(f"Issues encontradas: {len(result)}")
    for issue in result:
        print(f"  - {issue.severity.value}: {issue.description}")

    print("\n✅ Teste das validações financeiras concluído!")

if __name__ == "__main__":
    test_financial_validations()