"""
Sistema de layout e organização de arquivos para storage.

Estrutura de pastas:
data/
├── {symbol}/
│   ├── year={YYYY}/
│   │   └── month={MM}/
│   │       ├── original.parquet
│   │       ├── corrected.parquet
│   │       ├── original.csv (opcional)
│   │       ├── corrected.csv (opcional)
│   │       └── metadata.json
│   └── backups/
│       └── corrected_YYYY-MM-DD_HHMMSS.parquet
"""

import os
from pathlib import Path
from typing import List, Dict, Tuple, Optional
from datetime import datetime


class StorageLayout:
    """Gerencia estrutura de pastas e paths para armazenamento de dados"""

    def __init__(self, base_path: str = "data"):
        """
        Args:
            base_path: Diretório base para armazenamento (padrão: 'data/')
        """
        self.base_path = Path(base_path)

    def get_data_path(
        self,
        symbol: str,
        year: int,
        month: int,
        data_type: str = 'corrected',
        file_format: str = 'parquet'
    ) -> Path:
        """
        Constrói path para arquivo de dados.

        Args:
            symbol: Símbolo da ação (ex: 'AAPL', 'ASML.AS')
            year: Ano (ex: 2025)
            month: Mês (1-12)
            data_type: 'original' ou 'corrected'
            file_format: 'parquet' ou 'csv'

        Returns:
            Path completo para o arquivo

        Example:
            >>> layout = StorageLayout()
            >>> layout.get_data_path('AAPL', 2025, 11, 'corrected', 'parquet')
            PosixPath('data/AAPL/year=2025/month=11/corrected.parquet')
        """
        # Normalizar símbolo (substituir caracteres problemáticos)
        safe_symbol = self._safe_filename(symbol)

        # Construir path
        path = (
            self.base_path /
            safe_symbol /
            f"year={year}" /
            f"month={month:02d}" /
            f"{data_type}.{file_format}"
        )

        return path

    def get_backup_path(
        self,
        symbol: str,
        timestamp: Optional[datetime] = None,
        data_type: str = 'corrected'
    ) -> Path:
        """
        Constrói path para arquivo de backup.

        Args:
            symbol: Símbolo da ação
            timestamp: Timestamp do backup (padrão: now)
            data_type: 'original' ou 'corrected'

        Returns:
            Path para arquivo de backup

        Example:
            >>> layout.get_backup_path('AAPL')
            PosixPath('data/AAPL/backups/corrected_2025-11-09_143022.parquet')
        """
        if timestamp is None:
            timestamp = datetime.now()

        safe_symbol = self._safe_filename(symbol)
        timestamp_str = timestamp.strftime("%Y-%m-%d_%H%M%S")

        path = (
            self.base_path /
            safe_symbol /
            "backups" /
            f"{data_type}_{timestamp_str}.parquet"
        )

        return path

    def get_metadata_path(self, symbol: str, year: int, month: int) -> Path:
        """
        Constrói path para arquivo de metadata.

        Args:
            symbol: Símbolo da ação
            year: Ano
            month: Mês

        Returns:
            Path para metadata.json
        """
        safe_symbol = self._safe_filename(symbol)

        path = (
            self.base_path /
            safe_symbol /
            f"year={year}" /
            f"month={month:02d}" /
            "metadata.json"
        )

        return path

    def ensure_directories(self, symbol: str, year: int, month: int) -> None:
        """
        Cria estrutura de diretórios se não existir.

        Args:
            symbol: Símbolo da ação
            year: Ano
            month: Mês
        """
        safe_symbol = self._safe_filename(symbol)

        # Diretório de dados
        data_dir = self.base_path / safe_symbol / f"year={year}" / f"month={month:02d}"
        data_dir.mkdir(parents=True, exist_ok=True)

        # Diretório de backups
        backup_dir = self.base_path / safe_symbol / "backups"
        backup_dir.mkdir(parents=True, exist_ok=True)

    def list_available_symbols(self) -> List[str]:
        """
        Lista todos os símbolos com dados armazenados.

        Returns:
            Lista de símbolos
        """
        if not self.base_path.exists():
            return []

        symbols = []
        for item in self.base_path.iterdir():
            if item.is_dir() and not item.name.startswith('.'):
                # Reverter normalização de nome
                symbols.append(self._restore_filename(item.name))

        return sorted(symbols)

    def list_available_periods(self, symbol: str) -> List[Tuple[int, int]]:
        """
        Lista todos os períodos (ano, mês) disponíveis para um símbolo.

        Args:
            symbol: Símbolo da ação

        Returns:
            Lista de tuplas (ano, mês) ordenadas
        """
        safe_symbol = self._safe_filename(symbol)
        symbol_path = self.base_path / safe_symbol

        if not symbol_path.exists():
            return []

        periods = []

        # Iterar por anos
        for year_dir in symbol_path.iterdir():
            if not year_dir.is_dir() or not year_dir.name.startswith('year='):
                continue

            year = int(year_dir.name.split('=')[1])

            # Iterar por meses
            for month_dir in year_dir.iterdir():
                if not month_dir.is_dir() or not month_dir.name.startswith('month='):
                    continue

                month = int(month_dir.name.split('=')[1])
                periods.append((year, month))

        return sorted(periods)

    def get_backups(self, symbol: str, data_type: str = 'corrected') -> List[Path]:
        """
        Lista todos os backups disponíveis para um símbolo.

        Args:
            symbol: Símbolo da ação
            data_type: 'original' ou 'corrected'

        Returns:
            Lista de paths de backup, ordenados por data (mais recente primeiro)
        """
        safe_symbol = self._safe_filename(symbol)
        backup_dir = self.base_path / safe_symbol / "backups"

        if not backup_dir.exists():
            return []

        # Encontrar backups do tipo especificado
        backups = list(backup_dir.glob(f"{data_type}_*.parquet"))

        # Ordenar por data (mais recente primeiro)
        backups.sort(reverse=True)

        return backups

    def cleanup_old_backups(
        self,
        symbol: str,
        max_backups: int = 4,
        data_type: str = 'corrected'
    ) -> int:
        """
        Remove backups antigos, mantendo apenas os mais recentes.

        Args:
            symbol: Símbolo da ação
            max_backups: Número máximo de backups a manter
            data_type: 'original' ou 'corrected'

        Returns:
            Número de backups removidos
        """
        backups = self.get_backups(symbol, data_type)

        # Se temos mais backups que o limite
        if len(backups) > max_backups:
            backups_to_remove = backups[max_backups:]

            for backup in backups_to_remove:
                backup.unlink()

            return len(backups_to_remove)

        return 0

    def _safe_filename(self, symbol: str) -> str:
        """
        Converte símbolo em nome de arquivo seguro.

        Args:
            symbol: Símbolo original (ex: 'ASML.AS')

        Returns:
            Nome seguro para filesystem (ex: 'ASML_AS')
        """
        # Substituir caracteres problemáticos
        return symbol.replace('.', '_').replace('/', '_').replace('\\', '_')

    def _restore_filename(self, safe_name: str) -> str:
        """
        Restaura símbolo original do nome de arquivo.

        Args:
            safe_name: Nome de arquivo seguro (ex: 'ASML_AS')

        Returns:
            Símbolo original (ex: 'ASML.AS')

        Note:
            Esta função faz "best effort". Símbolos europeus como ASML.AS
            funcionam, mas pode haver ambiguidade em casos raros.
        """
        # Heurística: se tem _AS, _PA, _L no final, provavelmente é símbolo europeu
        european_suffixes = ['_AS', '_PA', '_L', '_MI', '_MC', '_SW', '_DE']

        for suffix in european_suffixes:
            if safe_name.endswith(suffix):
                # Restaurar o ponto: ASML_AS -> ASML.AS
                return safe_name[:-len(suffix)] + '.' + suffix[1:]

        # Se não é europeu, retornar como está
        return safe_name
