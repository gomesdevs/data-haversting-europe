import logging
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any

class JSONFormatter(logging.Formatter):
    """Formatter personalizado para output JSON estruturado"""

    def format(self, record: logging.LogRecord) -> str:
        log_entry = {
            "timestamp": datetime.fromtimestamp(record.created).isoformat() + "Z",
            "level": record.levelname,
            "module": record.name,
            "message": record.getMessage()
        }

        # Adiciona campos extras se existirem
        if hasattr(record, 'extra') and record.extra:
            log_entry["extra"] = record.extra

        return json.dumps(log_entry, ensure_ascii=False)

def setup_logger(name: str = "scraper", level: str = "INFO", log_dir: str = "logs") -> logging.Logger:
      # A fazer:
      # 1. Usar JSONFormatter
      # 2. Arquivo com data no nome (ex: scraper_2025-09-13.json)
      # 3. Evitar handlers duplicados
    pass

# Logger global
logger = setup_logger()