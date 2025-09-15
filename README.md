# 🇪🇺 Data Harvesting Europe

A robust Python-based financial data collection system specifically designed for European stock markets, with advanced features for data forecasting and analysis.

![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)
![License](https://img.shields.io/badge/License-Other-green.svg)
![Status](https://img.shields.io/badge/Status-Active-brightgreen.svg)

## 📋 Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Architecture](#architecture)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Usage Examples](#usage-examples)
- [Configuration](#configuration)
- [Testing](#testing)
- [Project Structure](#project-structure)
- [Contributing](#contributing)
- [License](#license)

## 🎯 Overview

**Data Harvesting Europe** is a comprehensive financial data collection and processing system tailored for European stock markets. Built with Python, it provides reliable, efficient, and scalable data harvesting capabilities with built-in support for forecasting analysis.

### Key Objectives

- 📈 **Financial Data Collection**: Harvest historical and real-time data from European stock exchanges
- 🔄 **Rate-Limited Processing**: Intelligent rate limiting to respect API constraints
- 📊 **Forecasting-Ready Data**: Clean, validated datasets optimized for predictive analysis
- 🚀 **Async/Sync Support**: Both synchronous and asynchronous data collection methods
- 🛡️ **Robust Error Handling**: Comprehensive retry mechanisms and error recovery

## ✨ Features

### 🎯 Core Features

- **Multi-Exchange Support**: European stock exchanges (Amsterdam, etc.)
- **Historical Data Collection**: Configurable periods (1d, 1wk, 1mo, 1y, 2y, 5y)
- **Real-time Price Tracking**: Latest price and volume data
- **Bulk Data Processing**: Efficient collection for multiple symbols
- **Data Validation**: Built-in data quality checks for forecasting

### 🔧 Technical Features

- **Rate Limiting**: Intelligent request throttling
- **Retry Logic**: Automatic retry with exponential backoff
- **Async Processing**: High-performance asynchronous operations
- **Structured Logging**: JSON-based logging with rich formatting
- **Storage Options**: Parquet format for efficient data storage
- **Progress Tracking**: Visual progress bars with tqdm

### 📊 Data Quality

- **Validation Engine**: Comprehensive data validation for forecasting
- **Missing Data Detection**: Automated gap analysis
- **Price Consistency Checks**: Validation of OHLC data integrity
- **Temporal Continuity**: Timeline gap detection and reporting

## 🏗️ Architecture

```
data-harvesting-europe/
├── core/           # Core functionality (logging, rate limiting)
├── endpoints/      # Data source endpoints (chart data, APIs)
├── pipe/          # Data processing pipelines
├── config/        # Configuration files
├── storage/       # Data storage utilities
├── utils/         # Utility functions
└── tests/         # Test files
```

## 🚀 Installation

### Prerequisites

- Python 3.8 or higher
- pip package manager

### Clone Repository

```bash
git clone https://github.com/gomesdevs/data-haversting-europe.git
cd data-haversting-europe
```

### Install Dependencies

```bash
pip install -r requirements.txt
```

### Dependencies Overview

- **Data Processing**: `pandas`, `numpy`, `pyarrow`
- **HTTP Clients**: `requests` (sync), `aiohttp` (async)
- **Data Formats**: `PyYAML`, `orjson`
- **Progress & Logging**: `tqdm`, `rich`
- **Development**: `pytest`, `mypy`, `black`, `isort`

## ⚡ Quick Start

### Basic Usage

```python
from endpoints.chart import ChartDataCollector

# Initialize the collector
collector = ChartDataCollector()

# Get historical data for a European stock
df = collector.get_historical_data(
    symbol="ASML.AS",  # ASML (Amsterdam)
    period="1y",       # 1 year of data
    interval="1d"      # Daily intervals
)

print(f"Collected {len(df)} records for ASML")
print(df.head())
```

### Real-time Price Data

```python
# Get latest price information
price_data = collector.get_latest_price("ASML.AS")

print(f"Current price: {price_data['price']:.2f} {price_data['currency']}")
print(f"Volume: {price_data['volume']:,}")
print(f"Exchange: {price_data['exchange']}")
```

## 📖 Usage Examples

### 1. Historical Data Collection

```python
from endpoints.chart import ChartDataCollector

collector = ChartDataCollector()

# Different time periods for analysis
configs = [
    {"period": "1y", "interval": "1d", "desc": "Daily data for 1 year"},
    {"period": "6mo", "interval": "1wk", "desc": "Weekly data for 6 months"},
    {"period": "5y", "interval": "1mo", "desc": "Monthly data for 5 years"}
]

symbol = "INGA.AS"  # ING Group

for config in configs:
    df = collector.get_historical_data(
        symbol=symbol,
        period=config["period"],
        interval=config["interval"]
    )
    print(f"{config['desc']}: {len(df)} records")
```

### 2. Bulk Data Collection

```python
# Collect data for multiple European stocks
symbols = ["ASML.AS", "INGA.AS", "HEIA.AS"]

results = collector.bulk_collect(
    symbols=symbols,
    period="3mo",
    interval="1d"
)

for symbol, df in results.items():
    print(f"{symbol}: {len(df)} records collected")
```

### 3. Data Validation for Forecasting

```python
# Collect with validation enabled
df = collector.get_historical_data(
    symbol="ASML.AS",
    period="2y",
    interval="1d",
    validate=True
)

# Check data quality
print(f"Data points: {len(df)}")
print(f"Date range: {df['date'].min()} to {df['date'].max()}")
print(f"Missing values: {df.isnull().sum().sum()}")
```

## ⚙️ Configuration

### Rate Limiting

The system includes intelligent rate limiting to respect API constraints:

```python
from core.rate_limiter import RateLimiter

# Configure rate limiting
rate_limiter = RateLimiter(
    requests_per_second=2,
    burst_limit=10
)
```

### Logging Configuration

Structured logging with JSON output:

```python
from core.logger import setup_logger, logger

# Setup logging
setup_logger()

# Use structured logging
logger.info(
    "Data collection started",
    extra={
        "symbol": "ASML.AS",
        "period": "1y",
        "request_id": "req-123"
    }
)
```

## 🧪 Testing

The project includes comprehensive tests for all components:

### Run All Tests

```bash
# Run basic functionality tests
python test.py

# Run chart collector tests
python test_chart.py

# Run rate limiter tests
python test_rate_limiter.py

# Run HTTP client tests
python test_http_client.py

# Run retry mechanism tests
python test_retry.py
```

### Test Coverage

- **Basic Logging**: `test.py`
- **Chart Data Collection**: `test_chart.py`
- **Rate Limiting**: `test_rate_limiter.py`
- **HTTP Clients**: `test_http_client.py`
- **Retry Logic**: `test_retry.py`
- **API Key Management**: `test_api_key.py`

### Example Test Output

```bash
🚀 TESTING CHART DATA COLLECTOR FOR FORECASTING

=== Test: Chart Collector Basic ===
✅ Collector initialized with forecasting configurations

=== Test: Historical Data Collection ===
🔄 Testing 1 year daily for ASML.AS...
   ✅ Success: 252 records collected
   📅 Period: 2023-09-14 to 2024-09-14
   💰 Initial price: 652.30
   💰 Final price: 698.50
```

## 📁 Project Structure

```
data-haversting-europe/
│
├── 📁 core/                   # Core system components
│   ├── logger.py             # Structured logging system
│   └── rate_limiter.py       # Rate limiting utilities
│
├── 📁 endpoints/              # Data source endpoints
│   └── chart.py              # Chart data collection
│
├── 📁 pipe/                   # Data processing pipelines
│   └── ...                   # Pipeline modules
│
├── 📁 config/                 # Configuration files
│   └── ...                   # YAML configurations
│
├── 📁 storage/                # Data storage utilities
│   └── ...                   # Storage modules
│
├── 📁 utils/                  # Utility functions
│   └── ...                   # Helper utilities
│
├── 📄 requirements.txt        # Python dependencies
├── 📄 LICENSE                # Project license
└── 📄 README.md              # This file
```

## 🎯 Supported Markets & Symbols

### European Exchanges

- **Euronext Amsterdam**: `.AS` suffix (e.g., `ASML.AS`, `INGA.AS`)
- **Euronext Paris**: `.PA` suffix
- **Euronext Brussels**: `.BR` suffix
- **London Stock Exchange**: `.L` suffix

### Example Symbols

```python
european_stocks = [
    "ASML.AS",    # ASML Holding (Tech)
    "INGA.AS",    # ING Group (Banking)
    "HEIA.AS",    # Heineken (Consumer)
    "RDSA.AS",    # Royal Dutch Shell (Energy)
    "UNA.AS"      # Unilever (Consumer)
]
```

## 🔧 Advanced Features

### Async Data Collection

```python
import asyncio
from endpoints.chart import AsyncChartCollector

async def collect_async():
    collector = AsyncChartCollector()
    
    symbols = ["ASML.AS", "INGA.AS", "HEIA.AS"]
    tasks = [
        collector.get_historical_data_async(symbol, "1y", "1d")
        for symbol in symbols
    ]
    
    results = await asyncio.gather(*tasks)
    return dict(zip(symbols, results))

# Run async collection
data = asyncio.run(collect_async())
```

### Data Export

```python
# Export to Parquet for efficient storage
df.to_parquet(f"data/{symbol}_{period}.parquet", index=False)

# Export to CSV for analysis
df.to_csv(f"data/{symbol}_{period}.csv", index=False)
```

## 🤝 Contributing

Contributions are welcome! Please follow these steps:

1. **Fork** the repository
2. **Create** a feature branch (`git checkout -b feature/amazing-feature`)
3. **Commit** your changes (`git commit -m 'Add amazing feature'`)
4. **Push** to the branch (`git push origin feature/amazing-feature`)
5. **Open** a Pull Request

### Development Setup

```bash
# Install development dependencies
pip install -r requirements.txt

# Install pre-commit hooks
pre-commit install

# Run linting
black .
isort .
mypy .

# Run tests
pytest
```

## 📜 License

This project is licensed under the **Creative Commons Attribution-NonCommercial 4.0 International License
** License. See the [LICENSE](LICENSE) file for details.

## 📞 Support

- **Issues**: [GitHub Issues](https://github.com/gomesdevs/data-haversting-europe/issues)
- **Discussions**: [GitHub Discussions](https://github.com/gomesdevs/data-haversting-europe/discussions)
- **Email**: Contact the maintainer through GitHub

## 🙏 Acknowledgments

- Built for European financial markets analysis
- Designed with forecasting and data science in mind
- Optimized for both research and production use

---

*Last updated: September 2025*
