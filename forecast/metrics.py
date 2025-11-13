"""
Métricas de Forecast
====================

Calcula métricas de performance para modelos de forecast:
- RMSE (Root Mean Squared Error)
- MAE (Mean Absolute Error)
- MAPE (Mean Absolute Percentage Error)
- R² (Coefficient of Determination)
- Directional Accuracy
"""

import numpy as np
import pandas as pd
from typing import Dict, Optional


class ForecastMetrics:
    """Classe para calcular métricas de forecast"""

    @staticmethod
    def rmse(y_true: np.ndarray, y_pred: np.ndarray) -> float:
        """
        Root Mean Squared Error

        Args:
            y_true: Valores reais
            y_pred: Valores preditos

        Returns:
            RMSE score
        """
        return np.sqrt(np.mean((y_true - y_pred) ** 2))

    @staticmethod
    def mae(y_true: np.ndarray, y_pred: np.ndarray) -> float:
        """
        Mean Absolute Error

        Args:
            y_true: Valores reais
            y_pred: Valores preditos

        Returns:
            MAE score
        """
        return np.mean(np.abs(y_true - y_pred))

    @staticmethod
    def mape(y_true: np.ndarray, y_pred: np.ndarray) -> float:
        """
        Mean Absolute Percentage Error

        Args:
            y_true: Valores reais
            y_pred: Valores preditos

        Returns:
            MAPE score (%)
        """
        # Evitar divisão por zero
        mask = y_true != 0
        return np.mean(np.abs((y_true[mask] - y_pred[mask]) / y_true[mask])) * 100

    @staticmethod
    def r2_score(y_true: np.ndarray, y_pred: np.ndarray) -> float:
        """
        R² (Coefficient of Determination)

        Args:
            y_true: Valores reais
            y_pred: Valores preditos

        Returns:
            R² score (0 a 1, quanto maior melhor)
        """
        ss_res = np.sum((y_true - y_pred) ** 2)
        ss_tot = np.sum((y_true - np.mean(y_true)) ** 2)
        return 1 - (ss_res / ss_tot)

    @staticmethod
    def directional_accuracy(y_true: np.ndarray, y_pred: np.ndarray) -> float:
        """
        Acurácia direcional (% de vezes que prevê direção correta)

        Args:
            y_true: Valores reais
            y_pred: Valores preditos

        Returns:
            Acurácia direcional (%)
        """
        # Calcular direção (subiu/desceu)
        true_direction = np.diff(y_true) > 0
        pred_direction = np.diff(y_pred) > 0

        return np.mean(true_direction == pred_direction) * 100

    @staticmethod
    def smape(y_true: np.ndarray, y_pred: np.ndarray) -> float:
        """
        Symmetric Mean Absolute Percentage Error

        Args:
            y_true: Valores reais
            y_pred: Valores preditos

        Returns:
            SMAPE score (%)
        """
        denominator = (np.abs(y_true) + np.abs(y_pred))
        mask = denominator != 0

        diff = np.abs(y_true[mask] - y_pred[mask])
        return np.mean(2.0 * diff / denominator[mask]) * 100

    @staticmethod
    def calculate_all(y_true: np.ndarray, y_pred: np.ndarray) -> Dict[str, float]:
        """
        Calcula todas as métricas

        Args:
            y_true: Valores reais
            y_pred: Valores preditos

        Returns:
            Dicionário com todas as métricas
        """
        metrics = {
            'rmse': ForecastMetrics.rmse(y_true, y_pred),
            'mae': ForecastMetrics.mae(y_true, y_pred),
            'mape': ForecastMetrics.mape(y_true, y_pred),
            'r2': ForecastMetrics.r2_score(y_true, y_pred),
            'directional_accuracy': ForecastMetrics.directional_accuracy(y_true, y_pred),
            'smape': ForecastMetrics.smape(y_true, y_pred),
        }
        return metrics

    @staticmethod
    def print_metrics(metrics: Dict[str, float], title: str = "Métricas de Forecast"):
        """
        Imprime métricas formatadas

        Args:
            metrics: Dicionário com métricas
            title: Título do relatório
        """
        print(f"\n{'='*60}")
        print(f"📊 {title}")
        print(f"{'='*60}")
        print(f"RMSE:                    {metrics['rmse']:.4f}")
        print(f"MAE:                     {metrics['mae']:.4f}")
        print(f"MAPE:                    {metrics['mape']:.2f}%")
        print(f"R²:                      {metrics['r2']:.4f}")
        print(f"Directional Accuracy:    {metrics['directional_accuracy']:.2f}%")
        print(f"SMAPE:                   {metrics['smape']:.2f}%")
        print(f"{'='*60}\n")

    @staticmethod
    def compare_models(results: Dict[str, Dict[str, float]]):
        """
        Compara múltiplos modelos

        Args:
            results: Dict {model_name: {metric: value}}
        """
        import pandas as pd

        df = pd.DataFrame(results).T

        print(f"\n{'='*80}")
        print("📊 COMPARAÇÃO DE MODELOS")
        print(f"{'='*80}")
        print(df.to_string())
        print(f"\n🏆 Melhores modelos por métrica:")

        # RMSE e MAE: menor é melhor
        print(f"   RMSE:  {df['rmse'].idxmin()} ({df['rmse'].min():.4f})")
        print(f"   MAE:   {df['mae'].idxmin()} ({df['mae'].min():.4f})")
        print(f"   MAPE:  {df['mape'].idxmin()} ({df['mape'].min():.2f}%)")

        # R² e Accuracy: maior é melhor
        print(f"   R²:    {df['r2'].idxmax()} ({df['r2'].max():.4f})")
        print(f"   Dir. Accuracy: {df['directional_accuracy'].idxmax()} ({df['directional_accuracy'].max():.2f}%)")
        print(f"{'='*80}\n")
