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
