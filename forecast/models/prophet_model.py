"""
Prophet Model - Facebook Prophet
=================================

Implementa forecast usando Prophet (Facebook):
- Modelo robusto para séries temporais
- Lida bem com sazonalidade e feriados
- Captura tendências de longo prazo
- Bom baseline para comparação

Documentação: https://facebook.github.io/prophet/
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Optional, Dict, List
import warnings

try:
    from prophet import Prophet
    PROPHET_AVAILABLE = True
except ImportError:
    PROPHET_AVAILABLE = False
    warnings.warn(
        "Prophet não está instalado. Instale com: pip install prophet",
        ImportWarning
    )


class ProphetModel:
    """
    Wrapper para Facebook Prophet otimizado para dados financeiros
    """

    def __init__(
        self,
        symbol: str,
        growth: str = 'linear',
        changepoint_prior_scale: float = 0.05,
        seasonality_prior_scale: float = 10.0,
        yearly_seasonality: bool = True,
        weekly_seasonality: bool = True,
        daily_seasonality: bool = False,
        add_country_holidays: Optional[str] = None
    ):
        """
        Inicializa modelo Prophet

        Args:
            symbol: Símbolo da ação (para logs)
            growth: 'linear' ou 'logistic'
            changepoint_prior_scale: Flexibilidade da tendência (0.001 a 0.5)
            seasonality_prior_scale: Força da sazonalidade (0.01 a 10)
            yearly_seasonality: Incluir sazonalidade anual
            weekly_seasonality: Incluir sazonalidade semanal
            daily_seasonality: Incluir sazonalidade diária
            add_country_holidays: País para incluir feriados (ex: 'US', 'BR')
        """
        if not PROPHET_AVAILABLE:
            raise ImportError("Prophet não instalado. Execute: pip install prophet")

        self.symbol = symbol
        self.growth = growth
        self.model = None
        self.forecast = None
        self.training_data = None

        # Configurações do modelo
        self.config = {
            'growth': growth,
            'changepoint_prior_scale': changepoint_prior_scale,
            'seasonality_prior_scale': seasonality_prior_scale,
            'yearly_seasonality': yearly_seasonality,
            'weekly_seasonality': weekly_seasonality,
            'daily_seasonality': daily_seasonality,
        }

        self.country_holidays = add_country_holidays

    def _prepare_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Prepara dados no formato Prophet (ds, y)

        Args:
            df: DataFrame com colunas [datetime, close]

        Returns:
            DataFrame formatado para Prophet
        """
        # Prophet requer colunas 'ds' (datetime) e 'y' (target)
        prophet_df = pd.DataFrame({
            'ds': pd.to_datetime(df['datetime'] if 'datetime' in df.columns else df.index),
            'y': df['close']
        })

        # Remover NaN
        prophet_df = prophet_df.dropna()

        # Ordenar por data
        prophet_df = prophet_df.sort_values('ds').reset_index(drop=True)

        return prophet_df

    def train(
        self,
        df: pd.DataFrame,
        add_regressors: Optional[Dict[str, pd.Series]] = None
    ) -> 'ProphetModel':
        """
        Treina o modelo Prophet

        Args:
            df: DataFrame com dados históricos
            add_regressors: Dicionário com regressores adicionais {nome: Series}

        Returns:
            Self (para chaining)
        """
        print(f"🚀 Treinando Prophet para {self.symbol}...")

        # Preparar dados
        self.training_data = self._prepare_data(df)

        # Criar modelo
        self.model = Prophet(**self.config)

        # Adicionar feriados se especificado
        if self.country_holidays:
            self.model.add_country_holidays(country_name=self.country_holidays)

        # Adicionar regressores customizados
        if add_regressors:
            for name, series in add_regressors.items():
                self.model.add_regressor(name)
                self.training_data[name] = series.values

        # Treinar modelo (silenciar warnings)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            self.model.fit(self.training_data)

        print(f"✅ Modelo treinado com {len(self.training_data)} pontos de dados")
        print(f"   Período: {self.training_data['ds'].min()} → {self.training_data['ds'].max()}")

        return self

    def predict(
        self,
        periods: int = 30,
        freq: str = 'D',
        include_history: bool = True
    ) -> pd.DataFrame:
        """
        Gera previsões

        Args:
            periods: Número de períodos para prever
            freq: Frequência ('D' = diário, 'H' = horário, etc.)
            include_history: Incluir dados históricos na previsão

        Returns:
            DataFrame com previsões
        """
        if self.model is None:
            raise ValueError("Modelo não foi treinado. Execute train() primeiro.")

        print(f"🔮 Gerando previsões para {periods} períodos...")

        # Criar dataframe futuro
        if include_history:
            # Incluir histórico + futuro
            future = self.model.make_future_dataframe(periods=periods, freq=freq)
        else:
            # Apenas futuro
            last_date = self.training_data['ds'].max()
            future_dates = pd.date_range(
                start=last_date + timedelta(days=1),
                periods=periods,
                freq=freq
            )
            future = pd.DataFrame({'ds': future_dates})

        # Fazer previsão
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            self.forecast = self.model.predict(future)

        print(f"✅ Previsões geradas")

        return self.forecast

    def get_predictions(self, future_only: bool = True) -> pd.DataFrame:
        """
        Retorna previsões em formato simplificado

        Args:
            future_only: Retornar apenas previsões futuras (sem histórico)

        Returns:
            DataFrame com [datetime, predicted_price, lower_bound, upper_bound]
        """
        if self.forecast is None:
            raise ValueError("Nenhuma previsão disponível. Execute predict() primeiro.")

        # Filtrar apenas futuro se solicitado
        if future_only:
            last_train_date = self.training_data['ds'].max()
            forecast = self.forecast[self.forecast['ds'] > last_train_date].copy()
        else:
            forecast = self.forecast.copy()

        # Formato simplificado
        result = pd.DataFrame({
            'datetime': forecast['ds'],
            'predicted_price': forecast['yhat'],
            'lower_bound': forecast['yhat_lower'],
            'upper_bound': forecast['yhat_upper'],
        })

        return result.reset_index(drop=True)

    def evaluate(self, test_df: pd.DataFrame) -> Dict[str, float]:
        """
        Avalia modelo com dados de teste

        Args:
            test_df: DataFrame com dados reais para comparação

        Returns:
            Dicionário com métricas de performance
        """
        from ..metrics import ForecastMetrics

        # Preparar dados de teste
        test_data = self._prepare_data(test_df)

        # Fazer previsões para as mesmas datas
        future = pd.DataFrame({'ds': test_data['ds']})
        predictions = self.model.predict(future)

        # Calcular métricas
        y_true = test_data['y'].values
        y_pred = predictions['yhat'].values

        metrics = ForecastMetrics.calculate_all(y_true, y_pred)

        return metrics

    def plot_forecast(
        self,
        figsize: tuple = (15, 6),
        show_components: bool = False
    ):
        """
        Plota previsões (requer matplotlib)

        Args:
            figsize: Tamanho da figura
            show_components: Mostrar componentes (trend, seasonality)
        """
        if self.forecast is None:
            raise ValueError("Nenhuma previsão disponível. Execute predict() primeiro.")

        try:
            import matplotlib.pyplot as plt
            from prophet.plot import plot_plotly, plot_components_plotly

            # Plot principal
            fig = self.model.plot(self.forecast, figsize=figsize)
            plt.title(f'Previsão Prophet - {self.symbol}', fontsize=14, fontweight='bold')
            plt.xlabel('Data', fontsize=12)
            plt.ylabel('Preço', fontsize=12)
            plt.grid(True, alpha=0.3)
            plt.tight_layout()
            plt.show()

            # Plot de componentes
            if show_components:
                fig_comp = self.model.plot_components(self.forecast, figsize=(15, 10))
                plt.tight_layout()
                plt.show()

        except ImportError:
            print("⚠️  matplotlib não está instalado. Execute: pip install matplotlib")

    def get_changepoints(self) -> pd.DataFrame:
        """
        Retorna pontos de mudança de tendência detectados

        Returns:
            DataFrame com changepoints
        """
        if self.model is None:
            raise ValueError("Modelo não foi treinado.")

        changepoints = pd.DataFrame({
            'datetime': self.model.changepoints,
            'delta': self.model.params['delta'].mean(axis=0)
        })

        # Ordenar por magnitude do delta
        changepoints['abs_delta'] = changepoints['delta'].abs()
        changepoints = changepoints.sort_values('abs_delta', ascending=False)

        return changepoints[['datetime', 'delta']].reset_index(drop=True)

    def summary(self) -> Dict:
        """
        Retorna sumário do modelo

        Returns:
            Dicionário com informações do modelo
        """
        if self.model is None:
            return {'status': 'Modelo não treinado'}

        summary = {
            'symbol': self.symbol,
            'status': 'Treinado',
            'training_points': len(self.training_data),
            'training_period': f"{self.training_data['ds'].min()} → {self.training_data['ds'].max()}",
            'config': self.config,
            'changepoints_detected': len(self.model.changepoints),
        }

        if self.forecast is not None:
            future_forecast = self.forecast[self.forecast['ds'] > self.training_data['ds'].max()]
            summary['forecast_periods'] = len(future_forecast)
            summary['forecast_range'] = f"{future_forecast['ds'].min()} → {future_forecast['ds'].max()}"

        return summary

    def save_model(self, filepath: str):
        """
        Salva modelo treinado

        Args:
            filepath: Caminho para salvar o modelo
        """
        import pickle

        if self.model is None:
            raise ValueError("Modelo não foi treinado.")

        with open(filepath, 'wb') as f:
            pickle.dump({
                'model': self.model,
                'forecast': self.forecast,
                'training_data': self.training_data,
                'config': self.config,
                'symbol': self.symbol
            }, f)

        print(f"💾 Modelo salvo em: {filepath}")

    @classmethod
    def load_model(cls, filepath: str) -> 'ProphetModel':
        """
        Carrega modelo salvo

        Args:
            filepath: Caminho do modelo salvo

        Returns:
            Instância de ProphetModel
        """
        import pickle

        with open(filepath, 'rb') as f:
            data = pickle.load(f)

        instance = cls(symbol=data['symbol'], **data['config'])
        instance.model = data['model']
        instance.forecast = data['forecast']
        instance.training_data = data['training_data']

        print(f"📂 Modelo carregado de: {filepath}")

        return instance
