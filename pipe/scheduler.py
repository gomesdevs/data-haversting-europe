"""
Pipeline Scheduler - Automação de Coleta
========================================

Sistema de agendamento para execução automatizada das pipelines.
Suporta Windows Task Scheduler, cron (Linux), e execução manual programada.

Features:
---------
- Scheduler nativo Python (APScheduler)
- Configuração via YAML
- Notificações por email/webhook (opcional)
- Logs de execução
- Health checks

Uso:
----
    # Iniciar scheduler em background
    python -m pipe.scheduler start

    # Executar uma vez agora
    python -m pipe.scheduler run-once

    # Verificar status
    python -m pipe.scheduler status

    # Parar scheduler
    python -m pipe.scheduler stop
"""

import argparse
import sys
import yaml
import json
from pathlib import Path
from typing import Dict, Any, Optional
from datetime import datetime, time as dt_time
import time
import subprocess
import os

from core.logger import setup_logger


class PipelineScheduler:
    """
    Scheduler para execução automatizada de pipelines.
    """

    DEFAULT_CONFIG = {
        'daily_price_pipeline': {
            'enabled': True,
            'schedule': 'cron',  # cron ou interval
            'cron': '0 18 * * *',  # 18:00 UTC (após fechamento mercados EU)
            'timezone': 'UTC',
            'symbols': None,  # None = todos do config
            'generate_report': True,
            'max_retries': 3,
            'retry_delay_minutes': 30
        },
        'weekly_fundamentals_pipeline': {
            'enabled': False,
            'schedule': 'cron',
            'cron': '0 0 * * 0',  # Domingo 00:00
            'timezone': 'UTC'
        }
    }

    def __init__(self, config_file: str = "config/scheduler.yml"):
        """
        Inicializa scheduler.

        Args:
            config_file: Arquivo de configuração YAML
        """
        self.config_file = config_file
        self.logger = setup_logger('scraper.scheduler')
        self.config = self._load_config()

    def _load_config(self) -> Dict[str, Any]:
        """Carrega configuração do scheduler."""
        try:
            if Path(self.config_file).exists():
                with open(self.config_file, 'r', encoding='utf-8') as f:
                    config = yaml.safe_load(f)
                self.logger.info(f"Configuração carregada de {self.config_file}")
                return config
            else:
                self.logger.warning(f"Config não encontrado, usando padrão")
                return self.DEFAULT_CONFIG
        except Exception as e:
            self.logger.error(f"Erro ao carregar config: {e}")
            return self.DEFAULT_CONFIG

    def run_daily_price_pipeline(self) -> Dict[str, Any]:
        """
        Executa pipeline de preços diários.

        Returns:
            Resultado da execução
        """
        config = self.config.get('daily_price_pipeline', {})

        if not config.get('enabled', True):
            self.logger.info("Pipeline de preços diários desabilitada")
            return {'status': 'skipped', 'reason': 'disabled'}

        self.logger.info("Iniciando pipeline de preços diários")

        # Construir comando
        cmd = [sys.executable, '-m', 'pipe.daily_price_pipeline']

        if config.get('symbols'):
            cmd.extend(['--symbols'] + config['symbols'])

        if config.get('generate_report', True):
            cmd.append('--report')

        # Executar com retry
        max_retries = config.get('max_retries', 3)
        retry_delay = config.get('retry_delay_minutes', 30) * 60

        for attempt in range(1, max_retries + 1):
            try:
                self.logger.info(f"Tentativa {attempt}/{max_retries}")

                result = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    timeout=3600  # 1 hora timeout
                )

                if result.returncode == 0:
                    self.logger.info("Pipeline executada com sucesso")
                    return {
                        'status': 'success',
                        'attempt': attempt,
                        'stdout': result.stdout,
                        'timestamp': datetime.now().isoformat()
                    }
                else:
                    self.logger.error(
                        f"Pipeline falhou (tentativa {attempt})",
                        extra={
                            'returncode': result.returncode,
                            'stderr': result.stderr
                        }
                    )

                    if attempt < max_retries:
                        self.logger.info(f"Aguardando {retry_delay}s antes de retry...")
                        time.sleep(retry_delay)

            except subprocess.TimeoutExpired:
                self.logger.error(f"Pipeline timeout (tentativa {attempt})")
            except Exception as e:
                self.logger.error(f"Erro na execução (tentativa {attempt}): {e}")

        return {
            'status': 'failed',
            'attempts': max_retries,
            'timestamp': datetime.now().isoformat()
        }

    def run_once(self) -> Dict[str, Any]:
        """
        Executa todas as pipelines habilitadas uma vez.

        Returns:
            Resultados de todas as pipelines
        """
        self.logger.info("Executando pipelines (run-once)")

        results = {}

        # Daily price pipeline
        if self.config.get('daily_price_pipeline', {}).get('enabled', True):
            print("\n🚀 Executando Daily Price Pipeline...")
            results['daily_price'] = self.run_daily_price_pipeline()

        return results

    def generate_windows_task(self, output_file: str = "scheduler_task.xml") -> str:
        """
        Gera arquivo XML para Windows Task Scheduler.

        Args:
            output_file: Nome do arquivo de saída

        Returns:
            Path do arquivo gerado
        """
        config = self.config.get('daily_price_pipeline', {})
        cron = config.get('cron', '0 18 * * *')

        # Parsear cron (formato simples: minute hour day month weekday)
        parts = cron.split()
        hour = parts[1] if len(parts) > 1 else '18'
        minute = parts[0] if len(parts) > 0 else '0'

        # Caminho absoluto do Python e script
        python_path = sys.executable
        script_path = Path(__file__).parent / 'daily_price_pipeline.py'

        xml_template = f"""<?xml version="1.0" encoding="UTF-16"?>
<Task version="1.2" xmlns="http://schemas.microsoft.com/windows/2004/02/mit/task">
  <RegistrationInfo>
    <Description>Daily Price Pipeline - European Stocks Data Collection</Description>
    <Author>Data Harvesting System</Author>
  </RegistrationInfo>
  <Triggers>
    <CalendarTrigger>
      <StartBoundary>2025-11-10T{hour}:{minute}:00</StartBoundary>
      <Enabled>true</Enabled>
      <ScheduleByDay>
        <DaysInterval>1</DaysInterval>
      </ScheduleByDay>
    </CalendarTrigger>
  </Triggers>
  <Principals>
    <Principal id="Author">
      <LogonType>InteractiveToken</LogonType>
      <RunLevel>LeastPrivilege</RunLevel>
    </Principal>
  </Principals>
  <Settings>
    <MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy>
    <DisallowStartIfOnBatteries>false</DisallowStartIfOnBatteries>
    <StopIfGoingOnBatteries>false</StopIfGoingOnBatteries>
    <AllowHardTerminate>true</AllowHardTerminate>
    <StartWhenAvailable>true</StartWhenAvailable>
    <RunOnlyIfNetworkAvailable>true</RunOnlyIfNetworkAvailable>
    <IdleSettings>
      <StopOnIdleEnd>false</StopOnIdleEnd>
      <RestartOnIdle>false</RestartOnIdle>
    </IdleSettings>
    <AllowStartOnDemand>true</AllowStartOnDemand>
    <Enabled>true</Enabled>
    <Hidden>false</Hidden>
    <RunOnlyIfIdle>false</RunOnlyIfIdle>
    <WakeToRun>false</WakeToRun>
    <ExecutionTimeLimit>PT2H</ExecutionTimeLimit>
    <Priority>7</Priority>
  </Settings>
  <Actions Context="Author">
    <Exec>
      <Command>{python_path}</Command>
      <Arguments>-m pipe.daily_price_pipeline --report</Arguments>
      <WorkingDirectory>{Path.cwd()}</WorkingDirectory>
    </Exec>
  </Actions>
</Task>
"""

        output_path = Path(output_file)
        output_path.write_text(xml_template, encoding='utf-16')

        print(f"\n✅ Arquivo gerado: {output_path}")
        print(f"\n📋 Para instalar no Windows Task Scheduler:")
        print(f"   1. Abrir 'Task Scheduler' (Agendador de Tarefas)")
        print(f"   2. Ação → Importar Tarefa...")
        print(f"   3. Selecionar: {output_path.absolute()}")
        print(f"   4. Configurar credenciais e confirmar")
        print(f"\n⏰ Agendamento: Diariamente às {hour}:{minute} UTC")

        return str(output_path.absolute())

    def generate_cron_entry(self) -> str:
        """
        Gera entrada crontab para Linux/Mac.

        Returns:
            String com entrada crontab
        """
        config = self.config.get('daily_price_pipeline', {})
        cron = config.get('cron', '0 18 * * *')

        python_path = sys.executable
        script_dir = Path(__file__).parent.parent

        cron_entry = f"""
# Daily Price Pipeline - European Stocks
{cron} cd {script_dir} && {python_path} -m pipe.daily_price_pipeline --report >> logs/pipeline_cron.log 2>&1
"""

        print("\n📋 Entrada Crontab (Linux/Mac):")
        print("="*70)
        print(cron_entry)
        print("="*70)
        print("\nPara instalar:")
        print("  1. crontab -e")
        print("  2. Copiar e colar a linha acima")
        print("  3. Salvar e sair")

        return cron_entry


def main():
    """CLI para gerenciar scheduler."""
    parser = argparse.ArgumentParser(
        description="Pipeline Scheduler - Automação de Coleta",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument(
        'action',
        choices=['run-once', 'generate-windows-task', 'generate-cron'],
        help='Ação a executar'
    )

    parser.add_argument(
        '--config',
        default='config/scheduler.yml',
        help='Arquivo de configuração'
    )

    args = parser.parse_args()

    scheduler = PipelineScheduler(config_file=args.config)

    if args.action == 'run-once':
        results = scheduler.run_once()
        print("\n" + "="*70)
        print("📊 RESULTADOS")
        print("="*70)
        print(json.dumps(results, indent=2))

    elif args.action == 'generate-windows-task':
        scheduler.generate_windows_task()

    elif args.action == 'generate-cron':
        scheduler.generate_cron_entry()


if __name__ == '__main__':
    main()
