"""
Pipeline sequencial com política de rejeição e auto-correção limitada.
"""

import pandas as pd
import numpy as np
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timedelta
from enum import Enum
from dataclasses import dataclass
from core.logger import setup_logger


class Severity(Enum):
    """Níveis de severidade para issues de validação"""
    CRITICAL = "CRITICAL"  # Rejeita dados
    WARNING = "WARNING"    # Aceita mas loga
    INFO = "INFO"         # Apenas informa


class IssueType(Enum):
    """Tipos de problemas de validação"""
    # Básicos
    MISSING_DATA = "missing_data"
    INVALID_TYPE = "invalid_type"

    # Financeiros
    NEGATIVE_PRICE = "negative_price"
    ZERO_VOLUME = "zero_volume"
    PRICE_INCONSISTENCY = "price_inconsistency"

    # Temporais
    DATE_GAP = "date_gap"
    DATE_DUPLICATE = "date_duplicate"
    DATE_ORDER = "date_order"

    # Outliers
    EXTREME_VARIATION = "extreme_variation"
    VOLUME_ANOMALY = "volume_anomaly"


@dataclass
class Issue:
    """Representa um problema encontrado na validação"""
    type: IssueType
    severity: Severity
    description: str
    symbol: str
    affected_rows: Optional[List[int]] = None
    suggested_fix: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Converte para dict para logging já estruturado"""
        return {
            "issue_type": self.type.value,
            "severity": self.severity.value,
            "description": self.description,
            "symbol": self.symbol,
            "affected_rows_count": len(self.affected_rows) if self.affected_rows else 0,
            "suggested_fix": self.suggested_fix
        }


@dataclass
class ValidationResult:
    """Resultado da validação com todas as issues encontradas"""
    is_valid: bool
    symbol: str
    issues: List[Issue]
    corrected_data: Optional[pd.DataFrame] = None

    @property
    def critical_issues(self) -> List[Issue]:
        """Issues críticas que rejeitam os dados"""
        return [i for i in self.issues if i.severity == Severity.CRITICAL]

    @property
    def warning_issues(self) -> List[Issue]:
        """Issues de warning que são aceitas mas vao para o log"""
        return [i for i in self.issues if i.severity == Severity.WARNING]

    @property
    def info_issues(self) -> List[Issue]:
        """Issues informativas"""
        return [i for i in self.issues if i.severity == Severity.INFO]

    def summary(self) -> Dict[str, Any]:
        """Resumo para logging estruturado"""
        return {
            "is_valid": self.is_valid,
            "symbol": self.symbol,
            "total_issues": len(self.issues),
            "critical_count": len(self.critical_issues),
            "warning_count": len(self.warning_issues),
            "info_count": len(self.info_issues),
            "has_corrections": self.corrected_data is not None
        }


class FinancialDataValidator:
    """
    Pipeline de validação:
    1. Validações básicas (tipos, nulos)
    2. Validações financeiras (OHLCV consistency)
    3. Validações temporais (sequência, gaps)
    4. Detecção de anomalias (outliers, volume)
    5. Auto-correção
    """

    def __init__(self, auto_correct: bool = True):
        """
        Inicializa validador.

        Args:
            auto_correct: Se deve aplicar correções automáticas simples
        """
        self.auto_correct = auto_correct
        self.logger = setup_logger("scraper.validation")

        # Limites configuráveis
        self.max_daily_variation = 0.20  # 20% variação diária
        self.max_missing_days = 2        # Máximo de dias para interpolar

        self.logger.info(
            "Financial data validator inicializado",
            extra={
                "auto_correct": auto_correct,
                "max_daily_variation": self.max_daily_variation,
                "max_missing_days": self.max_missing_days
            }
        )

    def validate(self, df: pd.DataFrame, symbol: str) -> ValidationResult:
        """
        Args:
            df: DataFrame com dados financeiros
            symbol: Símbolo da ação

        Returns:
            ValidationResult com todas as issues encontradas
        """
        issues = []
        corrected_data = df.copy() if self.auto_correct else None

        self.logger.info(
            f"Iniciando validação de dados",
            extra={
                "symbol": symbol,
                "rows": len(df),
                "columns": list(df.columns),
                "date_range": f"{df['date'].min()} to {df['date'].max()}" if 'date' in df.columns else "unknown"
            }
        )

        # Pipeline sequencial de validação
        issues.extend(self._validate_basic_structure(df, symbol))
        issues.extend(self._validate_financial_consistency(df, symbol))
        issues.extend(self._validate_temporal_sequence(df, symbol))
        issues.extend(self._validate_market_anomalies(df, symbol))

        # Auto-correção se habilitada e não há issues críticas
        critical_issues = [i for i in issues if i.severity == Severity.CRITICAL]

        if self.auto_correct and not critical_issues and corrected_data is not None:
            corrected_data, correction_issues = self._apply_corrections(corrected_data, symbol)
            issues.extend(correction_issues)

        # Determinar se dados são válidos (sem issues críticas)
        is_valid = len(critical_issues) == 0

        result = ValidationResult(
            is_valid=is_valid,
            symbol=symbol,
            issues=issues,
            corrected_data=corrected_data if self.auto_correct else None
        )

        # Log do resultado
        self.logger.info(
            f"Validação concluída para {symbol}",
            extra=result.summary()
        )

        # Log issues críticas separadamente
        for issue in critical_issues:
            self.logger.error(
                f"Issue crítica encontrada: {issue.description}",
                extra=issue.to_dict()
            )

        return result

    def _validate_basic_structure(self, df: pd.DataFrame, symbol: str) -> List[Issue]:
        """
        Args:
            df: DataFrame para validar
            symbol: Símbolo da ação

        Returns:
            Lista de issues encontradas
        """
        issues = []

        # Colunas obrigatórias que esperamos do chart.py
        required_columns = {
            'datetime': 'datetime64[ns]',
            'date': 'object',
            'symbol': 'object',
            'open': 'float64',
            'high': 'float64',
            'low': 'float64',
            'close': 'float64',
            'adj_close': 'float64',
            'volume': 'int64',
            'currency': 'object',
            'exchange': 'object'
        }

        # 1. Verificar se DataFrame não está vazio
        if df.empty:
            issues.append(Issue(
                type=IssueType.MISSING_DATA,
                severity=Severity.CRITICAL,
                description="DataFrame está vazio",
                symbol=symbol,
                suggested_fix="Verificar coleta de dados da Alpha Vantage"
            ))
            return issues  # Não vale a pena continuar se vazio

        # 2. Verificar colunas obrigatórias
        missing_columns = set(required_columns.keys()) - set(df.columns)
        if missing_columns:
            issues.append(Issue(
                type=IssueType.MISSING_DATA,
                severity=Severity.CRITICAL,
                description=f"Colunas obrigatórias ausentes: {list(missing_columns)}",
                symbol=symbol,
                suggested_fix="Verificar parsing do chart.py"
            ))

        # 3. Verificar tipos de dados das colunas presentes
        for col, expected_type in required_columns.items():
            if col in df.columns:
                actual_type = str(df[col].dtype)

                # Verificação mais flexível para tipos numéricos
                if expected_type == 'float64' and not pd.api.types.is_numeric_dtype(df[col]):
                    issues.append(Issue(
                        type=IssueType.INVALID_TYPE,
                        severity=Severity.CRITICAL,
                        description=f"Coluna '{col}' deve ser numérica, encontrado: {actual_type}",
                        symbol=symbol,
                        suggested_fix=f"Converter {col} para float64"
                    ))

                elif expected_type == 'int64' and not pd.api.types.is_integer_dtype(df[col]):
                    # Volume pode vir como float, isso é aceitável
                    if col == 'volume' and pd.api.types.is_numeric_dtype(df[col]):
                        issues.append(Issue(
                            type=IssueType.INVALID_TYPE,
                            severity=Severity.WARNING,
                            description=f"Volume como {actual_type} será convertido para int64",
                            symbol=symbol,
                            suggested_fix="Converter volume para int64"
                        ))
                    else:
                        issues.append(Issue(
                            type=IssueType.INVALID_TYPE,
                            severity=Severity.CRITICAL,
                            description=f"Coluna '{col}' deve ser inteiro, encontrado: {actual_type}",
                            symbol=symbol,
                            suggested_fix=f"Converter {col} para int64"
                        ))

        # 4. Verificar valores nulos em colunas críticas
        critical_columns = ['open', 'high', 'low', 'close', 'datetime']
        for col in critical_columns:
            if col in df.columns:
                null_count = df[col].isnull().sum()
                if null_count > 0:
                    null_rows = df[df[col].isnull()].index.tolist()
                    issues.append(Issue(
                        type=IssueType.MISSING_DATA,
                        severity=Severity.CRITICAL,
                        description=f"Coluna crítica '{col}' tem {null_count} valores nulos",
                        symbol=symbol,
                        affected_rows=null_rows,
                        suggested_fix="Remover linhas com dados de preço nulos"
                    ))

        # 5. Verificar valores nulos em colunas opcionais (só warning)
        optional_columns = ['volume', 'adj_close']
        for col in optional_columns:
            if col in df.columns:
                null_count = df[col].isnull().sum()
                if null_count > 0:
                    issues.append(Issue(
                        type=IssueType.MISSING_DATA,
                        severity=Severity.WARNING,
                        description=f"Coluna '{col}' tem {null_count} valores nulos",
                        symbol=symbol,
                        suggested_fix=f"Interpolar ou usar valores padrão para {col}"
                    ))

        # 6. Verificar se há dados suficientes para análise
        min_rows = 5  # Mínimo para qualquer análise financeira
        if len(df) < min_rows:
            issues.append(Issue(
                type=IssueType.MISSING_DATA,
                severity=Severity.CRITICAL,
                description=f"Dados insuficientes: {len(df)} linhas (mínimo: {min_rows})",
                symbol=symbol,
                suggested_fix="Coletar mais dados históricos"
            ))

        return issues

    def _validate_financial_consistency(self, df: pd.DataFrame, symbol: str) -> List[Issue]:
        """
        Args:
            df: DataFrame para validar
            symbol: Símbolo da ação

        Returns:
            Lista de issues encontradas
        """
        issues = []

        # Verificar se temos as colunas necessárias
        required_price_cols = ['open', 'high', 'low', 'close']
        missing_cols = [col for col in required_price_cols if col not in df.columns]
        if missing_cols:
            # Já foi detectado nas validações básicas, não duplicar
            return issues

        # Tolerância para arredondamento (1 centavo)
        tolerance = 0.01

        # 1. Verificar preços negativos (CRITICAL)
        for col in required_price_cols:
            negative_mask = df[col] <= 0
            if negative_mask.any():
                negative_rows = df[negative_mask].index.tolist()
                issues.append(Issue(
                    type=IssueType.NEGATIVE_PRICE,
                    severity=Severity.CRITICAL,
                    description=f"Preços negativos ou zero encontrados em '{col}': {len(negative_rows)} ocorrências",
                    symbol=symbol,
                    affected_rows=negative_rows,
                    suggested_fix=f"Remover linhas com {col} <= 0"
                ))

        if 'volume' in df.columns:
            # Volume negativo é impossível
            negative_volume_mask = df['volume'] < 0
            if negative_volume_mask.any():
                negative_vol_rows = df[negative_volume_mask].index.tolist()
                issues.append(Issue(
                    type=IssueType.ZERO_VOLUME,
                    severity=Severity.CRITICAL,
                    description=f"Volume negativo encontrado: {len(negative_vol_rows)} ocorrências",
                    symbol=symbol,
                    affected_rows=negative_vol_rows,
                    suggested_fix="Remover linhas com volume negativo"
                ))

            # Volume zero indica falta de liquidez
            zero_volume_mask = df['volume'] == 0
            if zero_volume_mask.any():
                zero_vol_rows = df[zero_volume_mask].index.tolist()
                issues.append(Issue(
                    type=IssueType.ZERO_VOLUME,
                    severity=Severity.WARNING,
                    description=f"Volume zero encontrado: {len(zero_vol_rows)} ocorrências (falta de liquidez)",
                    symbol=symbol,
                    affected_rows=zero_vol_rows,
                    suggested_fix="Verificar se são feriados ou problemas na coleta"
                ))

        # Low deve ser <= Open, High, Close
        low_open_violations = (df['low'] > df['open'] + tolerance)
        if low_open_violations.any():
            violation_rows = df[low_open_violations].index.tolist()
            issues.append(Issue(
                type=IssueType.PRICE_INCONSISTENCY,
                severity=Severity.WARNING,
                description=f"Low > Open encontrado: {len(violation_rows)} ocorrências",
                symbol=symbol,
                affected_rows=violation_rows,
                suggested_fix="Verificar dados da fonte ou ajustar para consistência"
            ))

        low_high_violations = (df['low'] > df['high'] + tolerance)
        if low_high_violations.any():
            violation_rows = df[low_high_violations].index.tolist()
            issues.append(Issue(
                type=IssueType.PRICE_INCONSISTENCY,
                severity=Severity.CRITICAL,  # Este é realmente impossível
                description=f"Low > High encontrado: {len(violation_rows)} ocorrências (impossível)",
                symbol=symbol,
                affected_rows=violation_rows,
                suggested_fix="Remover linhas com Low > High"
            ))

        low_close_violations = (df['low'] > df['close'] + tolerance)
        if low_close_violations.any():
            violation_rows = df[low_close_violations].index.tolist()
            issues.append(Issue(
                type=IssueType.PRICE_INCONSISTENCY,
                severity=Severity.WARNING,
                description=f"Low > Close encontrado: {len(violation_rows)} ocorrências",
                symbol=symbol,
                affected_rows=violation_rows,
                suggested_fix="Verificar dados da fonte"
            ))

        # High deve ser >= Open, Low, Close
        high_open_violations = (df['high'] + tolerance < df['open'])
        if high_open_violations.any():
            violation_rows = df[high_open_violations].index.tolist()
            issues.append(Issue(
                type=IssueType.PRICE_INCONSISTENCY,
                severity=Severity.WARNING,
                description=f"High < Open encontrado: {len(violation_rows)} ocorrências",
                symbol=symbol,
                affected_rows=violation_rows,
                suggested_fix="Verificar dados da fonte"
            ))

        high_close_violations = (df['high'] + tolerance < df['close'])
        if high_close_violations.any():
            violation_rows = df[high_close_violations].index.tolist()
            issues.append(Issue(
                type=IssueType.PRICE_INCONSISTENCY,
                severity=Severity.WARNING,
                description=f"High < Close encontrado: {len(violation_rows)} ocorrências",
                symbol=symbol,
                affected_rows=violation_rows,
                suggested_fix="Verificar dados da fonte"
            ))

        # Detectar preços todos iguais (muito raro)
        all_equal_mask = (
            (abs(df['open'] - df['high']) <= tolerance) &
            (abs(df['open'] - df['low']) <= tolerance) &
            (abs(df['open'] - df['close']) <= tolerance)
        )
        if all_equal_mask.any():
            equal_rows = df[all_equal_mask].index.tolist()
            issues.append(Issue(
                type=IssueType.PRICE_INCONSISTENCY,
                severity=Severity.WARNING,
                description=f"Preços OHLC idênticos encontrados: {len(equal_rows)} ocorrências (muito raro)",
                symbol=symbol,
                affected_rows=equal_rows,
                suggested_fix="Verificar se são realmente dias sem movimento ou erro na coleta"
            ))

        return issues

    def _validate_temporal_sequence(self, df: pd.DataFrame, symbol: str) -> List[Issue]:
        """
        Validações de sequência temporal e gaps de mercado.

        Args:
            df: DataFrame para validar (deve estar ordenado por data)
            symbol: Símbolo da ação

        Returns:
            Lista de issues encontradas
        """
        issues = []

        # Verificar se temos a coluna datetime
        if 'datetime' not in df.columns:
            return issues  # Já detectado nas validações básicas

        if len(df) < 2:
            return issues  # Precisa de pelo menos 2 registros para validar sequência

        # Converter para datetime se ainda não for
        df_dates = pd.to_datetime(df['datetime'])

        # 1. Verificar duplicatas de data (CRITICAL)
        duplicated_dates = df_dates.duplicated()
        if duplicated_dates.any():
            dup_rows = df[duplicated_dates].index.tolist()
            unique_dup_dates = df_dates[duplicated_dates].dt.date.unique()
            issues.append(Issue(
                type=IssueType.DATE_DUPLICATE,
                severity=Severity.CRITICAL,
                description=f"Datas duplicadas encontradas: {len(dup_rows)} registros, {len(unique_dup_dates)} datas únicas",
                symbol=symbol,
                affected_rows=dup_rows,
                suggested_fix="Remover registros duplicados mantendo o mais recente"
            ))

        # 2. Verificar ordem cronológica (WARNING - pode ser só questão de sort)
        if not df_dates.is_monotonic_increasing:
            # Encontrar onde a ordem quebra
            diff = df_dates.diff()
            negative_diffs = diff < pd.Timedelta(0)
            if negative_diffs.any():
                out_of_order_rows = df[negative_diffs].index.tolist()
                issues.append(Issue(
                    type=IssueType.DATE_ORDER,
                    severity=Severity.WARNING,
                    description=f"Sequência de datas fora de ordem: {len(out_of_order_rows)} ocorrências",
                    symbol=symbol,
                    affected_rows=out_of_order_rows,
                    suggested_fix="Reordenar DataFrame por data"
                ))

        # 3. Analisar gaps de mercado (assumindo dados diários)
        # Calcular diferenças entre datas consecutivas
        date_diffs = df_dates.diff().dt.days

        # Gap normal de fim de semana = 3 dias (sexta para segunda)
        # Gap de feriado curto = 4-5 dias
        # Gap suspeito = > 7 dias

        # Gaps de fim de semana (2-3 dias) são normais
        weekend_gaps = (date_diffs >= 2) & (date_diffs <= 3)
        weekend_count = weekend_gaps.sum()

        # Gaps de feriados (4-7 dias) são aceitáveis
        holiday_gaps = (date_diffs >= 4) & (date_diffs <= 7)
        holiday_count = holiday_gaps.sum()

        # Gaps suspeitos (> 7 dias) precisam investigação
        suspicious_gaps = date_diffs > 7
        if suspicious_gaps.any():
            gap_rows = df[suspicious_gaps].index.tolist()
            max_gap = date_diffs.max()
            issues.append(Issue(
                type=IssueType.DATE_GAP,
                severity=Severity.WARNING,
                description=f"Gaps suspeitos encontrados: {len(gap_rows)} gaps > 7 dias (máximo: {max_gap} dias)",
                symbol=symbol,
                affected_rows=gap_rows,
                suggested_fix="Verificar se são feriados prolongados ou problemas na coleta"
            ))

        # 4. Verificar se há dados em fins de semana (suspeito para a maioria dos mercados)
        df_with_weekday = df.copy()
        df_with_weekday['weekday'] = pd.to_datetime(df['datetime']).dt.dayofweek

        # 5 = Sábado, 6 = Domingo
        weekend_trading = df_with_weekday['weekday'].isin([5, 6])
        if weekend_trading.any():
            weekend_rows = df[weekend_trading].index.tolist()
            issues.append(Issue(
                type=IssueType.DATE_GAP,
                severity=Severity.WARNING,
                description=f"Negociação em fins de semana detectada: {len(weekend_rows)} registros",
                symbol=symbol,
                affected_rows=weekend_rows,
                suggested_fix="Verificar se o mercado realmente opera nos fins de semana ou remover"
            ))

        # 5. Verificar padrão geral de frequência
        # Para dados diários, esperamos gaps principalmente de 1-3 dias
        single_day_gaps = (date_diffs == 1).sum()
        total_gaps = len(date_diffs) - 1  # Excluir o primeiro NaN

        if total_gaps > 0:
            single_day_ratio = single_day_gaps / total_gaps

            # Se menos de 60% são gaps de 1 dia, pode haver muitos dados faltando
            if single_day_ratio < 0.6:
                issues.append(Issue(
                    type=IssueType.DATE_GAP,
                    severity=Severity.INFO,
                    description=f"Muitos gaps na série temporal: apenas {single_day_ratio:.1%} são dias consecutivos",
                    symbol=symbol,
                    suggested_fix="Verificar se dados estão completos ou se há muitos feriados"
                ))

        # 6. Log estatísticas temporais para info
        if total_gaps > 0:
            issues.append(Issue(
                type=IssueType.DATE_GAP,
                severity=Severity.INFO,
                description=f"Estatísticas temporais: {weekend_count} gaps de fim de semana, {holiday_count} gaps de feriado, {single_day_gaps} dias consecutivos",
                symbol=symbol,
                suggested_fix="Informação para análise"
            ))

        return issues
