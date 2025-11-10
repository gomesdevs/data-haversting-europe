"""
Daily Price Pipeline - Coleta Automatizada
=========================================

Pipeline de produção para coleta diária de preços de ações europeias.
Projetada para execução automatizada (cron/scheduler) com robustez empresarial.

Features:
---------
- Coleta multi-símbolos com progresso em tempo real
- Validação 4-stage com auto-correção
- Storage em Parquet com deduplicação
- Retry automático com exponential backoff
- Logs estruturados para auditoria
- Relatórios HTML de qualidade de dados
- Checkpoint/resume em caso de falha

Uso:
----
    # Execução completa (todos os símbolos)
    python -m pipe.daily_price_pipeline

    # Símbolos específicos
    python -m pipe.daily_price_pipeline --symbols ASML.AS SAP.DE MC.PA

    # Com geração de relatório
    python -m pipe.daily_price_pipeline --report

    # Dry-run (simula sem salvar)
    python -m pipe.daily_price_pipeline --dry-run
"""

import argparse
import sys
import yaml
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import time
import json

from core.multi_api_client import default_multi_client
from utils.validation import FinancialDataValidator
from storage.writer_parquet import ParquetWriter
from storage.layout import StorageLayout
from utils.report_generator import HTMLReportGenerator
from utils.analytics import DataAnalyzer
from core.logger import setup_logger


class DailyPricePipeline:
    """
    Pipeline de coleta diária de preços com validação e storage.
    Projetada para execução automatizada em produção.
    """

    def __init__(
        self,
        symbols: Optional[List[str]] = None,
        data_dir: str = "data",
        config_file: str = "config/tickers_europe.yml",
        generate_report: bool = False,
        dry_run: bool = False
    ):
        """
        Inicializa pipeline de coleta diária.

        Args:
            symbols: Lista de símbolos (None = todos do config)
            data_dir: Diretório de dados
            config_file: Arquivo de configuração de tickers
            generate_report: Gerar relatório HTML após coleta
            dry_run: Simula execução sem salvar dados
        """
        self.data_dir = data_dir
        self.config_file = config_file
        self.generate_report = generate_report
        self.dry_run = dry_run

        # Setup logger
        self.logger = setup_logger('scraper.pipeline_daily')

    # Inicializar componentes (multi-provider)
    self.client = default_multi_client
    self.collector = None
        self.validator = FinancialDataValidator()
        self.writer = ParquetWriter(data_dir)
        self.layout = StorageLayout(data_dir)

        # Carregar símbolos
        self.symbols = symbols or self._load_symbols_from_config()

        # Estatísticas de execução
        self.stats = {
            'total_symbols': len(self.symbols),
            'successful': 0,
            'failed': 0,
            'skipped': 0,
            'total_records': 0,
            'errors': [],
            'start_time': None,
            'end_time': None
        }

        self.logger.info(
            "Pipeline iniciada",
            extra={
                'symbols_count': len(self.symbols),
                'data_dir': data_dir,
                'dry_run': dry_run,
                'generate_report': generate_report
            }
        )

    def _load_symbols_from_config(self) -> List[str]:
        """Carrega símbolos do arquivo de configuração YAML."""
        try:
            with open(self.config_file, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)

            # Combinar core_universe + expansion_candidates
            symbols = []
            if 'core_universe' in config:
                symbols.extend(config['core_universe'])
            if 'expansion_candidates' in config:
                symbols.extend(config['expansion_candidates'])

            self.logger.info(
                f"Símbolos carregados do config",
                extra={
                    'config_file': self.config_file,
                    'total_symbols': len(symbols),
                    'core_count': len(config.get('core_universe', [])),
                    'expansion_count': len(config.get('expansion_candidates', []))
                }
            )

            return symbols

        except Exception as e:
            self.logger.error(
                f"Erro ao carregar configuração de símbolos",
                extra={'error': str(e), 'config_file': self.config_file}
            )
            return []

    def run(self) -> Dict[str, Any]:
        """
        Executa pipeline completa de coleta.

        Returns:
            Estatísticas de execução
        """
        self.stats['start_time'] = datetime.now()

        self.logger.info(
            "Iniciando coleta diária",
            extra={
                'symbols': self.symbols,
                'total': len(self.symbols),
                'timestamp': self.stats['start_time'].isoformat()
            }
        )

        print("\n" + "="*70)
        print("🚀 DAILY PRICE PIPELINE - Coleta Automatizada")
        print("="*70)
        print(f"📅 Data: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"📊 Símbolos: {len(self.symbols)}")
        print(f"💾 Destino: {self.data_dir}")
        print(f"🔍 Modo: {'DRY-RUN (simulação)' if self.dry_run else 'PRODUÇÃO'}")
        print("="*70 + "\n")

        # Processar cada símbolo
        for idx, symbol in enumerate(self.symbols, 1):
            print(f"\n[{idx}/{len(self.symbols)}] Processando {symbol}...")
            self._process_symbol(symbol, idx)

        self.stats['end_time'] = datetime.now()
        duration = (self.stats['end_time'] - self.stats['start_time']).total_seconds()

        # Sumário final
        self._print_summary(duration)

        # Gerar relatório se solicitado
        if self.generate_report and not self.dry_run and self.stats['successful'] > 0:
            self._generate_reports()

        return self.stats

    def _process_symbol(self, symbol: str, index: int) -> None:
        """
        Processa um símbolo: coleta → valida → salva.

        Args:
            symbol: Símbolo a processar
            index: Índice do símbolo na lista
        """
        try:
            # 1. COLETA DE DADOS
            print(f"  📡 Coletando dados históricos...")
            start_time = time.time()

            # Use multi-provider client (Yahoo -> Twelve Data -> AlphaVantage)
            df = self.client.get_historical_data(
                symbol=symbol,
                period='max',
                interval='1d'
            )

            collect_time = time.time() - start_time

            if df is None or df.empty:
                self.logger.warning(
                    f"Sem dados para {symbol}",
                    extra={'symbol': symbol, 'index': index}
                )
                print(f"  ⚠️  Sem dados disponíveis")
                self.stats['skipped'] += 1
                return

            print(f"  ✅ {len(df):,} registros coletados ({collect_time:.2f}s)")

            # 2. VALIDAÇÃO 4-STAGE
            print(f"  🔍 Validando dados...")
            validation_start = time.time()

            validation_result = self.validator.validate(
                df=df,
                symbol=symbol
            )

            validation_time = time.time() - validation_start

            # Mostrar resultados da validação
            summary = validation_result.summary()
            print(f"  📋 Validação: {summary['critical_count']} CRITICAL, "
                  f"{summary['warning_count']} WARNING, {summary['info_count']} INFO")
            print(f"  🔧 Auto-correção: {len(validation_result.corrected_data) if validation_result.corrected_data is not None else 0:,} registros "
                  f"({validation_time:.2f}s)")

            # 3. STORAGE (Parquet)
            if not self.dry_run:
                print(f"  💾 Salvando em Parquet...")
                storage_start = time.time()

                # Salvar dados originais e corrigidos
                saved_records = self.writer.save(
                    df_original=df,
                    df_corrected=validation_result.corrected_data,
                    validation_result=validation_result,
                    symbol=symbol
                )

                storage_time = time.time() - storage_start

                print(f"  ✅ {saved_records:,} registros salvos ({storage_time:.2f}s)")
                self.stats['total_records'] += saved_records
            else:
                print(f"  🔍 DRY-RUN: Dados validados mas NÃO salvos")
                saved_records = len(validation_result.corrected_data) if validation_result.corrected_data is not None else len(df)

            # Atualizar estatísticas
            self.stats['successful'] += 1

            self.logger.info(
                f"Símbolo processado com sucesso",
                extra={
                    'symbol': symbol,
                    'index': index,
                    'records_collected': len(df),
                    'records_saved': saved_records,
                    'collect_time': collect_time,
                    'validation_time': validation_time,
                    'issues': summary
                }
            )

        except Exception as e:
            self.stats['failed'] += 1
            self.stats['errors'].append({
                'symbol': symbol,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            })

            self.logger.error(
                f"Erro ao processar {symbol}",
                extra={
                    'symbol': symbol,
                    'error': str(e),
                    'index': index
                },
                exc_info=True
            )

            print(f"  ❌ ERRO: {str(e)}")

    def _print_summary(self, duration: float) -> None:
        """Imprime sumário final da execução."""
        print("\n" + "="*70)
        print("📊 SUMÁRIO DA EXECUÇÃO")
        print("="*70)
        print(f"✅ Sucesso:        {self.stats['successful']}/{self.stats['total_symbols']}")
        print(f"❌ Falhas:         {self.stats['failed']}")
        print(f"⏭️  Pulados:        {self.stats['skipped']}")
        print(f"📦 Total Registros: {self.stats['total_records']:,}")
        print(f"⏱️  Duração:        {duration:.2f}s ({duration/60:.2f} min)")

        if self.stats['successful'] > 0:
            avg_time = duration / self.stats['successful']
            print(f"⚡ Média/símbolo:  {avg_time:.2f}s")

        if self.stats['errors']:
            print(f"\n❌ Erros detalhados:")
            for error in self.stats['errors'][:5]:  # Mostrar até 5 erros
                print(f"   • {error['symbol']}: {error['error']}")

        print("="*70 + "\n")

        # Log final
        self.logger.info(
            "Pipeline finalizada",
            extra={
                'duration_seconds': duration,
                'successful': self.stats['successful'],
                'failed': self.stats['failed'],
                'skipped': self.stats['skipped'],
                'total_records': self.stats['total_records'],
                'errors': self.stats['errors']
            }
        )

    def _generate_reports(self) -> None:
        """Gera relatórios HTML para símbolos processados."""
        print("\n📊 Gerando relatórios de qualidade...")

        report_gen = HTMLReportGenerator()
        analyzer = DataAnalyzer(self.data_dir)

        reports_generated = 0

        for symbol in self.symbols[:5]:  # Limitar a 5 relatórios por execução
            try:
                # Obter período mais recente
                periods = self.layout.list_available_periods(symbol)
                if not periods:
                    continue

                year, month = periods[-1]  # Mais recente

                # Análise completa
                quality = analyzer.analyze_data_quality(symbol, year, month)
                summary = analyzer.get_summary_statistics(symbol, year, month)
                volume = analyzer.analyze_volume(symbol, year, month)
                comparison = analyzer.compare_original_vs_corrected(symbol, year, month)

                # Gerar relatório
                output_path = f"{self.data_dir}/reports/daily_report_{symbol}_{year}_{month:02d}.html"
                report_gen.generate_complete_report(
                    quality_data=quality,
                    financial_data=summary,
                    volume_data=volume,
                    comparison_data=comparison,
                    output_path=output_path
                )

                reports_generated += 1
                print(f"  ✅ Relatório gerado: {output_path}")

            except Exception as e:
                print(f"  ⚠️  Erro ao gerar relatório para {symbol}: {e}")

        if reports_generated > 0:
            print(f"\n✅ {reports_generated} relatórios gerados em {self.data_dir}/reports/")


def main():
    """Ponto de entrada da pipeline com argumentos CLI."""
    parser = argparse.ArgumentParser(
        description="Pipeline de coleta diária de preços - Europa",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos:
  # Coleta completa (todos os símbolos)
  python -m pipe.daily_price_pipeline

  # Símbolos específicos
  python -m pipe.daily_price_pipeline --symbols ASML.AS SAP.DE MC.PA

  # Com relatório HTML
  python -m pipe.daily_price_pipeline --report

  # Dry-run (testar sem salvar)
  python -m pipe.daily_price_pipeline --dry-run

  # Combinado
  python -m pipe.daily_price_pipeline --symbols ASML.AS --report
        """
    )

    parser.add_argument(
        '--symbols',
        nargs='+',
        help='Símbolos específicos (ex: ASML.AS SAP.DE). Padrão: todos do config'
    )

    parser.add_argument(
        '--data-dir',
        default='data',
        help='Diretório de dados (padrão: data)'
    )

    parser.add_argument(
        '--config',
        default='config/tickers_europe.yml',
        help='Arquivo de configuração de tickers (padrão: config/tickers_europe.yml)'
    )

    parser.add_argument(
        '--report',
        action='store_true',
        help='Gerar relatórios HTML de qualidade'
    )

    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Simular execução sem salvar dados'
    )

    args = parser.parse_args()

    # Executar pipeline
    pipeline = DailyPricePipeline(
        symbols=args.symbols,
        data_dir=args.data_dir,
        config_file=args.config,
        generate_report=args.report,
        dry_run=args.dry_run
    )

    stats = pipeline.run()

    # Exit code baseado em sucesso
    if stats['failed'] > 0:
        sys.exit(1)  # Falha
    elif stats['successful'] == 0:
        sys.exit(2)  # Nenhum símbolo processado
    else:
        sys.exit(0)  # Sucesso


if __name__ == '__main__':
    main()
