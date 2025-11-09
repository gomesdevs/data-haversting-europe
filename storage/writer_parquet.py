"""
Writer para armazenamento de dados em Parquet e CSV.

Features:
- Salva dados originais e corrigidos separadamente
- Merge inteligente com dados existentes
- Deduplicação automática
- Backup automático (máx 4 backups)
- Metadata com estatísticas
- Compressão configurável
"""

import pandas as pd
import json
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime
import shutil

from storage.layout import StorageLayout
from utils.validation import ValidationResult
from core.logger import setup_logger


class ParquetWriter:
    """Writer para armazenamento de dados financeiros em Parquet/CSV"""

    def __init__(
        self,
        base_path: str = "data",
        compression: str = 'snappy',
        max_backups: int = 4
    ):
        """
        Args:
            base_path: Diretório base para armazenamento
            compression: Algoritmo de compressão Parquet ('snappy', 'gzip', 'zstd')
            max_backups: Número máximo de backups a manter
        """
        self.layout = StorageLayout(base_path)
        self.compression = compression
        self.max_backups = max_backups
        self.logger = setup_logger('scraper.storage')

        self.logger.info(
            "ParquetWriter inicializado",
            extra={
                "base_path": base_path,
                "compression": compression,
                "max_backups": max_backups
            }
        )

    def save(
        self,
        df_original: pd.DataFrame,
        df_corrected: Optional[pd.DataFrame],
        symbol: str,
        validation_result: ValidationResult,
        formats: List[str] = ['parquet'],
        backup: bool = True
    ) -> Dict[str, Any]:
        """
        Salva dados com merge, deduplicação e backup automático.

        Args:
            df_original: DataFrame com dados originais
            df_corrected: DataFrame com dados corrigidos (pode ser None)
            symbol: Símbolo da ação
            validation_result: Resultado da validação
            formats: Lista de formatos ['parquet', 'csv']
            backup: Se True, cria backup antes de merge

        Returns:
            Dict com estatísticas da operação

        Raises:
            ValueError: Se validation_result tem issues CRITICAL
        """
        # Rejeitar se houver issues críticas
        if validation_result.critical_issues:
            error_msg = f"Dados rejeitados para {symbol}: {len(validation_result.critical_issues)} issues CRITICAL"
            self.logger.error(
                error_msg,
                extra={
                    "symbol": symbol,
                    "critical_issues": [issue.description for issue in validation_result.critical_issues]
                }
            )
            raise ValueError(error_msg)

        self.logger.info(
            f"Iniciando gravação de dados para {symbol}",
            extra={
                "symbol": symbol,
                "records_original": len(df_original),
                "records_corrected": len(df_corrected) if df_corrected is not None else 0,
                "formats": formats
            }
        )

        # Extrair ano/mês dos dados
        if 'datetime' not in df_original.columns:
            raise ValueError("DataFrame deve ter coluna 'datetime'")

        # Usar a data mais recente para determinar partição
        latest_date = pd.to_datetime(df_original['datetime']).max()
        year = latest_date.year
        month = latest_date.month

        # Garantir que diretórios existem
        self.layout.ensure_directories(symbol, year, month)

        # Processar dados originais
        stats_original = self._save_dataset(
            df=df_original,
            symbol=symbol,
            year=year,
            month=month,
            data_type='original',
            formats=formats,
            backup=backup
        )

        # Processar dados corrigidos (se existirem)
        stats_corrected = None
        if df_corrected is not None:
            stats_corrected = self._save_dataset(
                df=df_corrected,
                symbol=symbol,
                year=year,
                month=month,
                data_type='corrected',
                formats=formats,
                backup=backup
            )

        # Criar metadata
        metadata = self._create_metadata(
            symbol=symbol,
            year=year,
            month=month,
            stats_original=stats_original,
            stats_corrected=stats_corrected,
            validation_result=validation_result
        )

        # Salvar metadata
        metadata_path = self.layout.get_metadata_path(symbol, year, month)
        with open(metadata_path, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, indent=2, ensure_ascii=False, default=str)

        self.logger.info(
            f"Dados salvos com sucesso para {symbol}",
            extra={
                "symbol": symbol,
                "year": year,
                "month": month,
                "metadata": metadata
            }
        )

        return metadata

    def _save_dataset(
        self,
        df: pd.DataFrame,
        symbol: str,
        year: int,
        month: int,
        data_type: str,
        formats: List[str],
        backup: bool
    ) -> Dict[str, Any]:
        """
        Salva um dataset (original ou corrected) com merge e deduplicação.

        Returns:
            Dict com estatísticas do dataset salvo
        """
        # Remover duplicatas do novo DataFrame
        df_dedup = self._deduplicate(df)

        removed_duplicates = len(df) - len(df_dedup)
        if removed_duplicates > 0:
            self.logger.info(
                f"Duplicatas removidas em {data_type}",
                extra={
                    "symbol": symbol,
                    "data_type": data_type,
                    "removed": removed_duplicates
                }
            )

        # Verificar se já existe arquivo
        parquet_path = self.layout.get_data_path(symbol, year, month, data_type, 'parquet')

        if parquet_path.exists():
            # Criar backup antes de merge
            if backup:
                self._create_backup(symbol, parquet_path, data_type)

            # Ler dados existentes
            df_existing = pd.read_parquet(parquet_path)

            # Merge
            df_merged = self._merge_with_existing(df_dedup, df_existing)

            self.logger.info(
                f"Merge realizado para {data_type}",
                extra={
                    "symbol": symbol,
                    "data_type": data_type,
                    "existing_records": len(df_existing),
                    "new_records": len(df_dedup),
                    "merged_records": len(df_merged)
                }
            )

            df_final = df_merged
        else:
            df_final = df_dedup

        # Salvar nos formatos solicitados
        file_sizes = {}

        if 'parquet' in formats:
            df_final.to_parquet(
                parquet_path,
                compression=self.compression,
                index=False
            )
            file_sizes['parquet'] = parquet_path.stat().st_size

        if 'csv' in formats:
            csv_path = self.layout.get_data_path(symbol, year, month, data_type, 'csv')
            df_final.to_csv(
                csv_path,
                index=False,
                encoding='utf-8'
            )
            file_sizes['csv'] = csv_path.stat().st_size

        # Retornar estatísticas
        return {
            "records": len(df_final),
            "duplicates_removed": removed_duplicates,
            "file_sizes": file_sizes,
            "date_range": {
                "start": str(df_final['datetime'].min()),
                "end": str(df_final['datetime'].max())
            }
        }

    def _deduplicate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Remove duplicatas mantendo o registro mais recente.

        Args:
            df: DataFrame a ser deduplicado

        Returns:
            DataFrame sem duplicatas
        """
        # Garantir tipos de dados corretos antes de salvar
        df = self._normalize_types(df)

        # Ordenar por datetime (para manter o mais recente em caso de duplicata por 'date')
        df_sorted = df.sort_values('datetime', ascending=True)

        # Remover duplicatas baseado na coluna 'date', mantendo o último (mais recente)
        df_dedup = df_sorted.drop_duplicates(subset=['date'], keep='last')

        # Reordenar por datetime
        df_dedup = df_dedup.sort_values('datetime').reset_index(drop=True)

        return df_dedup

    def _normalize_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Normaliza tipos de dados para garantir compatibilidade com Parquet.

        Args:
            df: DataFrame a ser normalizado

        Returns:
            DataFrame com tipos corretos
        """
        df = df.copy()

        # Garantir que datetime é datetime64
        if 'datetime' in df.columns:
            df['datetime'] = pd.to_datetime(df['datetime'])

        # Garantir que date é string
        if 'date' in df.columns:
            df['date'] = df['date'].astype(str)

        # Garantir que symbol, currency, exchange são strings
        for col in ['symbol', 'currency', 'exchange']:
            if col in df.columns:
                df[col] = df[col].astype(str)

        # Garantir que OHLC são float64
        for col in ['open', 'high', 'low', 'close', 'adj_close']:
            if col in df.columns:
                df[col] = df[col].astype('float64')

        # Garantir que volume é int64
        if 'volume' in df.columns:
            df['volume'] = df['volume'].fillna(0).astype('int64')

        return df

    def _merge_with_existing(
        self,
        df_new: pd.DataFrame,
        df_existing: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Faz merge inteligente entre dados novos e existentes.

        Strategy:
        1. Combinar ambos DataFrames
        2. Remover duplicatas (preferir novos dados)
        3. Ordenar por data

        Args:
            df_new: Novos dados
            df_existing: Dados já armazenados

        Returns:
            DataFrame merged e deduplicado
        """
        # Concatenar
        df_combined = pd.concat([df_existing, df_new], ignore_index=True)

        # Deduplicate (mantém o último, que são os novos dados)
        df_merged = self._deduplicate(df_combined)

        return df_merged

    def _create_backup(
        self,
        symbol: str,
        source_path: Path,
        data_type: str
    ) -> Optional[Path]:
        """
        Cria backup de arquivo existente.

        Args:
            symbol: Símbolo da ação
            source_path: Path do arquivo a fazer backup
            data_type: 'original' ou 'corrected'

        Returns:
            Path do backup criado, ou None se falhou
        """
        if not source_path.exists():
            return None

        # Criar path de backup
        backup_path = self.layout.get_backup_path(symbol, data_type=data_type)

        try:
            # Copiar arquivo
            shutil.copy2(source_path, backup_path)

            self.logger.info(
                f"Backup criado",
                extra={
                    "symbol": symbol,
                    "data_type": data_type,
                    "backup_path": str(backup_path),
                    "size_bytes": backup_path.stat().st_size
                }
            )

            # Limpar backups antigos
            removed = self.layout.cleanup_old_backups(symbol, self.max_backups, data_type)

            if removed > 0:
                self.logger.info(
                    f"Backups antigos removidos",
                    extra={
                        "symbol": symbol,
                        "data_type": data_type,
                        "removed_count": removed
                    }
                )

            return backup_path

        except Exception as e:
            self.logger.error(
                f"Falha ao criar backup",
                extra={
                    "symbol": symbol,
                    "data_type": data_type,
                    "error": str(e)
                }
            )
            return None

    def _create_metadata(
        self,
        symbol: str,
        year: int,
        month: int,
        stats_original: Dict[str, Any],
        stats_corrected: Optional[Dict[str, Any]],
        validation_result: ValidationResult
    ) -> Dict[str, Any]:
        """
        Cria metadata JSON com informações da gravação.

        Returns:
            Dict com metadata completa
        """
        metadata = {
            "symbol": symbol,
            "partition": {
                "year": year,
                "month": month
            },
            "last_updated": datetime.now().isoformat(),
            "compression": self.compression,
            "records": {
                "original": stats_original["records"],
                "original_duplicates_removed": stats_original["duplicates_removed"]
            },
            "file_sizes_bytes": {
                "original": stats_original["file_sizes"]
            },
            "date_range": stats_original["date_range"],
            "validation": {
                "is_valid": validation_result.is_valid,
                "total_issues": len(validation_result.issues),
                "critical_issues": len(validation_result.critical_issues),
                "warnings": len([i for i in validation_result.issues if i.severity.value == "WARNING"]),
                "info": len([i for i in validation_result.issues if i.severity.value == "INFO"])
            }
        }

        # Adicionar info de dados corrigidos se existir
        if stats_corrected is not None:
            metadata["records"]["corrected"] = stats_corrected["records"]
            metadata["records"]["corrected_duplicates_removed"] = stats_corrected["duplicates_removed"]
            metadata["records"]["added_by_correction"] = (
                stats_corrected["records"] - stats_original["records"]
            )
            metadata["file_sizes_bytes"]["corrected"] = stats_corrected["file_sizes"]

        return metadata

    def read(
        self,
        symbol: str,
        year: int,
        month: int,
        data_type: str = 'corrected'
    ) -> Optional[pd.DataFrame]:
        """
        Lê dados armazenados.

        Args:
            symbol: Símbolo da ação
            year: Ano
            month: Mês
            data_type: 'original' ou 'corrected'

        Returns:
            DataFrame com dados, ou None se não existir
        """
        path = self.layout.get_data_path(symbol, year, month, data_type, 'parquet')

        if not path.exists():
            return None

        return pd.read_parquet(path)

    def get_metadata(
        self,
        symbol: str,
        year: int,
        month: int
    ) -> Optional[Dict[str, Any]]:
        """
        Lê metadata de um período.

        Args:
            symbol: Símbolo da ação
            year: Ano
            month: Mês

        Returns:
            Dict com metadata, ou None se não existir
        """
        path = self.layout.get_metadata_path(symbol, year, month)

        if not path.exists():
            return None

        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
