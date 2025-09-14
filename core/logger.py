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
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level.upper()))  # Define o nível mínimo (mensagens abaixo são ignoradas)
    logger.propagate = False  # Evita mensagens duplicadas se root logger também estiver configurado
    log_path = Path(log_dir)
    log_path.mkdir(parents=True, exist_ok=True)
    log_file = log_path / f"{name}_{datetime.now().strftime('%Y-%m-%d')}.json"
    if not any(isinstance(h, logging.FileHandler) and h.baseFilename == str(log_file) for h in logger.handlers):
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setFormatter(JSONFormatter())
        logger.addHandler(file_handler)
    return logger

# Logger global
logger = setup_logger()