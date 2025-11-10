"""
Yahoo Finance Pipeline - Coleta GRATUITA e SEM LIMITES
=======================================================

Pipeline otimizada para Yahoo Finance (yfinance).
100% GRATUITA, SEM rate limits, 20+ anos de histórico.

Vantagens sobre Alpha Vantage:
-------------------------------
✅ GRÁTIS (sem API key)
✅ SEM rate limits severos
✅ Download em lote (multi-threading)
✅ 20+ anos de histórico
✅ Dados B3 + Europa + US + Ásia
✅ Dividendos e splits inclusos

Uso:
----
    # Execução completa
    python -m pipe.yahoo_pipeline

    # Símbolos específicos
    python -m pipe.yahoo_pipeline --symbols ASML.AS SAP.DE PETR4.SA

    # Com relatório
    python -m pipe.yahoo_pipeline --report

    # Dry-run
    python -m pipe.yahoo_pipeline --dry-run
"""

import argparse
import sys
import yaml
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime
import time

from core.yahoo_finance_client import YahooFinanceClient
from core.multi_api_client import default_multi_client
from utils.validation import FinancialDataValidator
from storage.writer_parquet import ParquetWriter
from storage.layout import StorageLayout
from utils.report_generator import HTMLReportGenerator
from utils.analytics import DataAnalyzer
from core.logger import setup_logger


class YahooPipeline:
    """
    Pipeline de coleta via Yahoo Finance.
    MUITO MAIS RÁPIDO que Alpha Vantage (download em lote).
    """

    def __init__(
        self,
        symbols: Optional[List[str]] = None,
        data_dir: str = "data",
        config_file: str = "config/tickers_europe.yml",
        generate_report: bool = False,
        dry_run: bool = False,
        batch_mode: bool = True  # Download em lote (recomendado)
    ):
        """
        Inicializa pipeline Yahoo Finance.

        Args:
            symbols: Lista de símbolos (None = todos do config)
            data_dir: Diretório de dados
            config_file: Arquivo de configuração de tickers
            generate_report: Gerar relatório HTML
            dry_run: Simula sem salvar
            batch_mode: Download em lote (muito mais rápido)
        """
        self.data_dir = data_dir
        self.config_file = config_file
        self.generate_report = generate_report
        self.dry_run = dry_run
        self.batch_mode = batch_mode

        # Setup logger
        self.logger = setup_logger('scraper.yahoo_pipeline')
        # Inicializar componentes (multi-provider: Yahoo -> TwelveData -> AlphaVantage)
        self.client = default_multi_client
        self.validator = FinancialDataValidator()
        self.writer = ParquetWriter(data_dir)
        self.layout = StorageLayout(data_dir)

        # Carregar símbolos
        self.symbols = symbols or self._load_symbols_from_config()

        # Estatísticas
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
            "Yahoo Pipeline inicializada",
            extra={
                'symbols_count': len(self.symbols),
                'data_dir': data_dir,
                'batch_mode': batch_mode,
                'dry_run': dry_run
            }
        )

    def _load_symbols_from_config(self) -> List[str]:
        """Carrega símbolos do config YAML."""
        try:
            with open(self.config_file, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)

            symbols = []
            if 'core_universe' in config:
                symbols.extend(config['core_universe'])
            if 'expansion_candidates' in config:
                symbols.extend(config['expansion_candidates'])

            self.logger.info(
                f"Símbolos carregados",
                extra={'total': len(symbols), 'config_file': self.config_file}
            )

            return symbols

        except Exception as e:
            self.logger.error(f"Erro ao carregar config: {e}")
            return []

    def run(self) -> Dict[str, Any]:
        """
        Executa pipeline completa.

        Returns:
            Estatísticas de execução
        """
        self.stats['start_time'] = datetime.now()

        print("\n" + "="*70)
        print("🚀 YAHOO FINANCE PIPELINE - 100% GRÁTIS, SEM LIMITES")
        print("="*70)
        print(f"📅 Data: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"📊 Símbolos: {len(self.symbols)}")
        print(f"💾 Destino: {self.data_dir}")
        print(f"⚡ Modo: {'LOTE (rápido)' if self.batch_mode else 'Individual'}")
        print(f"🔍 Dry-run: {'SIM' if self.dry_run else 'NÃO'}")
        print("="*70 + "\n")

        if self.batch_mode:
            self._run_batch()
        else:
            self._run_individual()

        self.stats['end_time'] = datetime.now()
        duration = (self.stats['end_time'] - self.stats['start_time']).total_seconds()

        self._print_summary(duration)

        if self.generate_report and not self.dry_run and self.stats['successful'] > 0:
            self._generate_reports()

        return self.stats

    def _run_batch(self) -> None:
        """Execução em lote (muito mais rápido)."""
        print("⚡ Modo LOTE ativado - Download multi-threading\n")

        try:
            print(f"📡 Baixando {len(self.symbols)} símbolos em lote...")
            start_time = time.time()

            # Download em lote (MUITO MAIS RÁPIDO!)
            results = self.client.download_multiple(
                symbols=self.symbols,
                period='max',
                interval='1d'
            )

            download_time = time.time() - start_time

            print(f"✅ Download concluído: {len(results)}/{len(self.symbols)} símbolos ({download_time:.2f}s)")
            print(f"⚡ Velocidade: {download_time/len(results):.2f}s por símbolo\n")

            # Processar cada símbolo
            for idx, symbol in enumerate(self.symbols, 1):
                df = results.get(symbol)

                if df is None or df.empty:
                    print(f"[{idx}/{len(self.symbols)}] {symbol}: ⚠️  Sem dados")
                    self.stats['skipped'] += 1
                    continue

                print(f"[{idx}/{len(self.symbols)}] Processando {symbol}...")
                self._process_dataframe(df, symbol, idx)

        except Exception as e:
            self.logger.error(f"Erro no modo lote: {e}", exc_info=True)
            print(f"❌ Erro no download em lote: {e}")
            print("🔄 Alternando para modo individual...")
            self._run_individual()

    def _run_individual(self) -> None:
        """Execução individual (mais lento, mas mais robusto)."""
        for idx, symbol in enumerate(self.symbols, 1):
            print(f"\n[{idx}/{len(self.symbols)}] Processando {symbol}...")

            try:
                # Download individual
                print(f"  📡 Baixando dados...")
                start_time = time.time()

                df = self.client.get_historical_data(
                    symbol=symbol,
                    period='max',
                    interval='1d'
                )

                download_time = time.time() - start_time

                if df is None or df.empty:
                    print(f"  ⚠️  Sem dados disponíveis")
                    self.stats['skipped'] += 1
                    continue

                print(f"  ✅ {len(df):,} registros baixados ({download_time:.2f}s)")

                self._process_dataframe(df, symbol, idx)

            except Exception as e:
                self.stats['failed'] += 1
                self.stats['errors'].append({
                    'symbol': symbol,
                    'error': str(e),
                    'timestamp': datetime.now().isoformat()
                })
                print(f"  ❌ ERRO: {str(e)}")
                self.logger.error(f"Erro em {symbol}: {e}", exc_info=True)

    def _process_dataframe(self, df, symbol: str, index: int) -> None:
        """Processa DataFrame: valida e salva."""
        try:
            # Validação
            print(f"  🔍 Validando...")
            validation_start = time.time()

            validation_result = self.validator.validate(df=df, symbol=symbol)
            summary = validation_result.summary()

            validation_time = time.time() - validation_start

            print(f"  📋 Issues: {summary['critical_count']} CRITICAL, "
                  f"{summary['warning_count']} WARNING, {summary['info_count']} INFO")

            # Storage
            if not self.dry_run:
                print(f"  💾 Salvando...")
                storage_start = time.time()

                saved_records = self.writer.save(
                    df_original=df,
                    df_corrected=validation_result.corrected_data if validation_result.corrected_data is not None else df,
                    validation_result=validation_result,
                    symbol=symbol
                )

                storage_time = time.time() - storage_start
                print(f"  ✅ {saved_records:,} registros salvos ({storage_time:.2f}s)")
                self.stats['total_records'] += saved_records
            else:
                print(f"  🔍 DRY-RUN: Dados validados mas NÃO salvos")
                saved_records = len(validation_result.corrected_data) if validation_result.corrected_data is not None else len(df)

            self.stats['successful'] += 1

            self.logger.info(
                f"Símbolo processado",
                extra={
                    'symbol': symbol,
                    'records': len(df),
                    'saved': saved_records,
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
            print(f"  ❌ ERRO: {str(e)}")
            self.logger.error(f"Erro processando {symbol}: {e}", exc_info=True)

    def _print_summary(self, duration: float) -> None:
        """Imprime sumário final."""
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
            print(f"\n❌ Erros:")
            for error in self.stats['errors'][:5]:
                print(f"   • {error['symbol']}: {error['error']}")

        print("="*70 + "\n")

    def _generate_reports(self) -> None:
        """Gera relatórios HTML."""
        print("\n📊 Gerando relatórios...")

        report_gen = HTMLReportGenerator()
        analyzer = DataAnalyzer(self.data_dir)

        for symbol in self.symbols[:5]:
            try:
                periods = self.layout.list_available_periods(symbol)
                if not periods:
                    continue

                year, month = periods[-1]

                quality = analyzer.analyze_data_quality(symbol, year, month)
                summary = analyzer.get_summary_statistics(symbol, year, month)
                volume = analyzer.analyze_volume(symbol, year, month)
                comparison = analyzer.compare_original_vs_corrected(symbol, year, month)

                output_path = f"{self.data_dir}/reports/yahoo_report_{symbol}_{year}_{month:02d}.html"
                report_gen.generate_complete_report(
                    quality_data=quality,
                    financial_data=summary,
                    volume_data=volume,
                    comparison_data=comparison,
                    output_path=output_path
                )

                print(f"  ✅ {output_path}")

            except Exception as e:
                print(f"  ⚠️  Erro em {symbol}: {e}")


def main():
    """CLI para Yahoo Pipeline."""
    parser = argparse.ArgumentParser(
        description="Yahoo Finance Pipeline - 100% GRÁTIS, SEM LIMITES"
    )

    parser.add_argument(
        '--symbols',
        nargs='+',
        help='Símbolos específicos'
    )

    parser.add_argument(
        '--data-dir',
        default='data',
        help='Diretório de dados'
    )

    parser.add_argument(
        '--config',
        default='config/tickers_europe.yml',
        help='Arquivo de configuração'
    )

    parser.add_argument(
        '--report',
        action='store_true',
        help='Gerar relatórios HTML'
    )

    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Simular sem salvar'
    )

    parser.add_argument(
        '--individual',
        action='store_true',
        help='Modo individual (mais lento, desabilita lote)'
    )

    args = parser.parse_args()

    pipeline = YahooPipeline(
        symbols=args.symbols,
        data_dir=args.data_dir,
        config_file=args.config,
        generate_report=args.report,
        dry_run=args.dry_run,
        batch_mode=not args.individual
    )

    stats = pipeline.run()

    # Exit code
    if stats['failed'] > 0:
        sys.exit(1)
    elif stats['successful'] == 0:
        sys.exit(2)
    else:
        sys.exit(0)


if __name__ == '__main__':
    main()
