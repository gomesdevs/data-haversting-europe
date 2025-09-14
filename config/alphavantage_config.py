"""
Alpha Vantage API Configuration
==============================

Configuração segura para API key e parâmetros da Alpha Vantage.
"""

import os
from typing import Optional
from pathlib import Path


class AlphaVantageConfig:
    """Configuração centralizada para Alpha Vantage API"""

    # URL base da API
    BASE_URL = "https://www.alphavantage.co/query"

    # Rate limiting oficial da Alpha Vantage
    RATE_LIMIT_CALLS_PER_MINUTE = 5  # Free tier: 5 calls/min
    RATE_LIMIT_CALLS_PER_DAY = 500   # Free tier: 500 calls/day

    # Timeouts
    DEFAULT_TIMEOUT = 30  # Alpha Vantage pode ser lenta

    # Funções disponíveis
    FUNCTIONS = {
        'daily': 'TIME_SERIES_DAILY',
        'daily_adjusted': 'TIME_SERIES_DAILY_ADJUSTED',
        'weekly': 'TIME_SERIES_WEEKLY',
        'weekly_adjusted': 'TIME_SERIES_WEEKLY_ADJUSTED',
        'monthly': 'TIME_SERIES_MONTHLY',
        'monthly_adjusted': 'TIME_SERIES_MONTHLY_ADJUSTED',
        'intraday': 'TIME_SERIES_INTRADAY',
        'quote': 'GLOBAL_QUOTE'
    }

    # Mapeamento de intervalos
    INTERVAL_MAPPING = {
        '1d': 'daily_adjusted',      # Diário com ajustes (dividendos, splits)
        '1wk': 'weekly_adjusted',    # Semanal com ajustes
        '1mo': 'monthly_adjusted'    # Mensal com ajustes
    }

    def __init__(self):
        """Inicializa configuração e carrega API key"""
        self.api_key = self._load_api_key()
        self._validate_config()

    def _load_api_key(self) -> Optional[str]:
        """
        Carrega API key de diferentes fontes (ordem de prioridade):
        1. Variável de ambiente ALPHAVANTAGE_API_KEY
        2. Arquivo .env na raiz do projeto
        3. Arquivo config/.api_key
        """
        # 1. Variável de ambiente
        api_key = os.getenv('ALPHAVANTAGE_API_KEY')
        if api_key:
            return api_key.strip()

        # 2. Arquivo .env na raiz do projeto
        env_file = Path(__file__).parent.parent / '.env'
        if env_file.exists():
            try:
                with open(env_file, 'r') as f:
                    for line in f:
                        if line.startswith('ALPHAVANTAGE_API_KEY='):
                            return line.split('=', 1)[1].strip().strip('"\'')
            except Exception:
                pass

        # 3. Arquivo config/.api_key
        api_key_file = Path(__file__).parent / '.api_key'
        if api_key_file.exists():
            try:
                with open(api_key_file, 'r') as f:
                    return f.read().strip()
            except Exception:
                pass

        return None

    def _validate_config(self):
        """Valida configuração"""
        if not self.api_key:
            raise ValueError(
                "API key da Alpha Vantage não encontrada. "
                "Configure via:\n"
                "1. Variável de ambiente: ALPHAVANTAGE_API_KEY=sua_key\n"
                "2. Arquivo .env: ALPHAVANTAGE_API_KEY=sua_key\n"
                "3. Arquivo config/.api_key"
            )

        if len(self.api_key) < 10:  # API keys da Alpha Vantage são longas
            raise ValueError("API key da Alpha Vantage parece inválida (muito curta)")

    def get_function_for_interval(self, interval: str) -> str:
        """
        Retorna a função da Alpha Vantage para o intervalo solicitado.

        Args:
            interval: Intervalo ('1d', '1wk', '1mo')

        Returns:
            Nome da função da API
        """
        if interval not in self.INTERVAL_MAPPING:
            valid_intervals = ', '.join(self.INTERVAL_MAPPING.keys())
            raise ValueError(f"Intervalo '{interval}' não suportado. Válidos: {valid_intervals}")

        function_key = self.INTERVAL_MAPPING[interval]
        return self.FUNCTIONS[function_key]

    def create_api_key_file(self, api_key: str):
        """
        Cria arquivo .api_key para armazenar a chave.
        ATENÇÃO: Adicione config/.api_key ao .gitignore!

        Args:
            api_key: Sua API key da Alpha Vantage
        """
        api_key_file = Path(__file__).parent / '.api_key'

        try:
            with open(api_key_file, 'w') as f:
                f.write(api_key.strip())

            print(f"✅ API key salva em {api_key_file}")
            print("⚠️  IMPORTANTE: Adicione 'config/.api_key' ao .gitignore!")

        except Exception as e:
            raise ValueError(f"Erro ao salvar API key: {e}")


# Instância global
def get_alphavantage_config() -> AlphaVantageConfig:
    """Retorna configuração global da Alpha Vantage"""
    return AlphaVantageConfig()


# Para facilitar importação
try:
    config = get_alphavantage_config()
    API_KEY = config.api_key
    BASE_URL = config.BASE_URL
    RATE_LIMIT_RPM = config.RATE_LIMIT_CALLS_PER_MINUTE
except Exception as e:
    # Durante import, não falhar se API key não estiver configurada
    API_KEY = None
    BASE_URL = "https://www.alphavantage.co/query"
    RATE_LIMIT_RPM = 5