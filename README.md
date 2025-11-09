# 🚀 European Stock Data Scraper - Production Ready

Sistema completo de coleta, validação e análise de dados financeiros de ações europeias, **production-ready** para projetos de **forecasting** e análise quantitativa.

![Status](https://img.shields.io/badge/status-production--ready-green)
![Python](https://img.shields.io/badge/python-3.8+-blue)
![API](https://img.shields.io/badge/API-AlphaVantage-orange)
![License](https://img.shields.io/badge/license-MIT-blue)

---

## 📋 Índice

1. [Visão Geral](#-visão-geral)
2. [Features Principais](#-features-principais)
3. [Instalação Rápida](#-instalação-rápida)
4. [Uso Rápido](#-uso-rápido)
5. [Pipelines Automatizadas](#-pipelines-automatizadas)
6. [Analytics e Relatórios](#-analytics-e-relatórios)
7. [Estrutura de Dados](#-estrutura-de-dados)
8. [Documentação Completa](#-documentação)

---

## 🎯 Visão Geral

Sistema **production-ready** para coleta automatizada de dados financeiros de ações europeias via **Alpha Vantage API**.

### ⚡ Por que usar este sistema?

✅ **20+ anos de histórico** para forecasting robusto  
✅ **Validação 4-stage** com auto-correção inteligente  
✅ **Storage otimizado** (Parquet + Snappy ~50% compressão)  
✅ **Scheduler automático** (Windows/Linux)  
✅ **Analytics engine** com métricas financeiras completas  
✅ **Relatórios HTML** interativos e profissionais  

### � Cobertura

**65+ símbolos europeus**:
- 🇩🇪 Alemanha (DAX): SAP, Siemens, Deutsche Bank, BMW
- 🇳🇱 Holanda (Euronext): ASML, Philips, Adyen
- 🇫🇷 França (CAC 40): LVMH, Airbus, L'Oréal, BNP
- 🇬🇧 UK (LSE): Shell, BP
- 🇨🇭 Suíça: Nestlé, Novartis
- 🇪🇸 Espanha (IBEX): Santander, Iberdrola
- 🇮🇹 Itália (FTSE MIB): Enel, ENI
- 🇸🇪 🇫🇮 🇩🇰 Nordic: Volvo, Ericsson, Nokia, Ørsted

---
- **Data Validation**: Built-in data quality checks for financial and temporal validation

### 🔧 Technical Features

- **Rate Limiting**: Intelligent request throttling (configurable RPS with burst support)
- **Retry Logic**: Automatic retry with exponential backoff and jitter
- **Async Processing**: High-performance asynchronous operations with aiohttp
- **Structured Logging**: JSON-based logging with rich formatting and request tracking
- **Multiple Storage**: Parquet files and PostgreSQL database support
- **Progress Tracking**: Visual progress bars with tqdm and rich console output
- **Configuration Management**: YAML-based policies and ticker management

### 📊 Data Quality & Validation

- **Financial Validation**: OHLC consistency, price range validation, volume checks
- **Temporal Validation**: Timeline continuity, trading hours validation, holiday detection  
- **Basic Validation**: Data type validation, null/missing value detection
- **Hashing Utilities**: Data integrity verification and deduplication
- **Timezone Handling**: Proper timezone conversion for European markets

## 🏗️ Architecture

```
data-harvesting-europe/
├── 📁 core/                  # Core system functionality
│   ├── alphavantage_client.py    # Alpha Vantage API client
│   ├── base-endpoints.py         # Base endpoint definitions
│   ├── http_client.py           # HTTP client utilities
│   ├── logger.py               # Structured logging system
│   ├── rate_limiter.py         # Rate limiting implementation
│   └── retry.py                # Retry logic with backoff
│
├── 📁 endpoints/             # Data source endpoints
│   ├── batch-universe.py        # Batch universe operations
│   ├── chart.py                # Chart data collection
│   ├── options.py              # Options data endpoints
│   ├── quotes-summary.py       # Quote summaries
│   └── quotes.py               # Real-time quotes
│
├── 📁 pipe/                  # Data processing pipelines
│   ├── daily_price_pipeline.py  # Daily price data pipeline
│   ├── earnings_pipeline.py     # Earnings data pipeline
│   └── fundamentals_pipeline.py # Fundamentals pipeline
│
├── 📁 config/                # Configuration management
│   ├── alphavantage_config.py   # Alpha Vantage configuration
│   ├── fetch_policies.yml       # Data fetching policies
│   └── tickers_europe.yml       # European ticker definitions
│
├── 📁 storage/               # Data storage utilities
│   ├── layout.py               # Data layout definitions
│   ├── writer_parquet.py       # Parquet file writer
│   └── writer_postgres.py      # PostgreSQL writer
│
├── 📁 utils/                 # Utility functions
│   ├── hashing.py              # Data integrity utilities
│   ├── timezones.py            # Timezone handling
│   └── validation.py           # Data validation engine
│
└── 📁 tests/                 # Comprehensive test suite
    ├── test_*.py               # Individual component tests
    └── ...                     # Validation and integration tests
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

- **Data Processing**: `pandas==2.2.2`, `numpy==1.26.4`, `pyarrow==16.1.0`
- **HTTP Clients**: `requests==2.32.3` (sync), `aiohttp==3.9.5` (async)
- **Data Formats**: `PyYAML==6.0.1`, `orjson==3.10.6`
- **Progress & Logging**: `tqdm==4.66.4`, `rich==13.7.1`
- **Time Handling**: `python-dateutil==2.9.0.post0`, `pytz==2024.1`
- **Development**: `pytest==8.2.2`, `mypy==1.10.0`, `black==24.4.2`, `isort==5.13.2`
- **Testing**: `pytest-asyncio==0.23.7`, `pre-commit==3.7.1`

## ⚙️ Configuration

### Alpha Vantage API Setup

1. **Get API Key**: Register at [Alpha Vantage](https://www.alphavantage.co/support/#api-key) to get your free API key

2. **Set Environment Variable**:
```bash
export ALPHA_VANTAGE_API_KEY="your_api_key_here"
```

3. **Configuration File**: The system uses `config/alphavantage_config.py` for API settings:
```python
from config.alphavantage_config import AlphaVantageConfig

# Rate limits (free tier)
RATE_LIMIT_CALLS_PER_MINUTE = 5    # 5 calls/minute
RATE_LIMIT_CALLS_PER_DAY = 500     # 500 calls/day
DEFAULT_TIMEOUT = 30               # 30 seconds timeout
```

### Data Fetching Policies

Configure data collection behavior via `config/fetch_policies.yml`:

```yaml
# Example policy configuration
daily_prices:
  enabled: true
  frequency: daily
  schedule_window: "06:00-08:00"  # UTC
  batch:
    size: 50
    concurrency: 2
  retry:
    max_attempts: 3
    backoff_strategy: exponential
  rate_limit_rps: 8
```

### European Tickers Configuration

Manage ticker lists in `config/tickers_europe.yml`:
- Euronext Amsterdam (`.AS`)
- Euronext Paris (`.PA`) 
- Euronext Brussels (`.BR`)
- London Stock Exchange (`.L`)

### Rate Limiting Configuration

```python
from core.rate_limiter import RateLimiter

# Configure rate limiting
rate_limiter = RateLimiter(
    requests_per_second=2,
    burst_limit=10,
    alpha_vantage_mode=True  # Respects Alpha Vantage limits
)
```

### Logging Configuration

```python
from core.logger import setup_logger, logger

# Setup structured logging
setup_logger(level="INFO", format="json")

# Use with request tracking
logger.info(
    "Data collection started",
    extra={
        "symbol": "ASML.AS",
        "period": "1y", 
        "api_key_prefix": "ABC***",
        "request_id": "req-123"
    }
)
```

## ⚡ Quick Start

### 1. Set up API Key

```bash
# Set your Alpha Vantage API key
export ALPHA_VANTAGE_API_KEY="your_api_key_here"
```

### 2. Basic Usage with Alpha Vantage

```python
from core.alphavantage_client import AlphaVantageClient
from endpoints.chart import ChartDataCollector

# Initialize the Alpha Vantage client
av_client = AlphaVantageClient()

# Initialize the collector
collector = ChartDataCollector(client=av_client)

# Get historical data for a European stock
df = collector.get_daily_data(
    symbol="ASML.AS",     # ASML (Amsterdam)
    outputsize="compact"   # Last 100 data points
)

print(f"Collected {len(df)} records for ASML")
print(f"Date range: {df['date'].min()} to {df['date'].max()}")
print(df.head())
```

### 3. Real-time Quote Data

```python
# Get latest quote information
quote_data = av_client.get_quote(symbol="ASML.AS")

print(f"Symbol: {quote_data['symbol']}")
print(f"Current price: {quote_data['price']:.2f}")
print(f"Change: {quote_data['change']:.2f} ({quote_data['change_percent']:.2f}%)")
print(f"Volume: {quote_data['volume']:,}")
print(f"Last updated: {quote_data['latest_trading_day']}")
```

### 4. Using Data Pipelines

```python
from pipe.daily_price_pipeline import DailyPricePipeline

# Initialize pipeline
pipeline = DailyPricePipeline(
    symbols=["ASML.AS", "INGA.AS", "HEIA.AS"],
    output_format="parquet"
)

# Run data collection pipeline
results = pipeline.run()
print(f"Processed {len(results)} symbols")
```

## 📖 Usage Examples

### 1. Historical Data Collection with Alpha Vantage

```python
from core.alphavantage_client import AlphaVantageClient
from endpoints.chart import ChartDataCollector

# Initialize client and collector
av_client = AlphaVantageClient()
collector = ChartDataCollector(client=av_client)

symbol = "INGA.AS"  # ING Group

# Different data types available
data_types = [
    {"func": "get_daily_data", "desc": "Daily OHLCV data"},
    {"func": "get_weekly_data", "desc": "Weekly OHLCV data"}, 
    {"func": "get_monthly_data", "desc": "Monthly OHLCV data"},
    {"func": "get_intraday_data", "desc": "Intraday data (5min intervals)", "interval": "5min"}
]

for data_type in data_types:
    try:
        if "interval" in data_type:
            df = getattr(collector, data_type["func"])(symbol=symbol, interval=data_type["interval"])
        else:
            df = getattr(collector, data_type["func"])(symbol=symbol)
        
        print(f"{data_type['desc']}: {len(df)} records")
        print(f"  Date range: {df['date'].min()} to {df['date'].max()}")
    except Exception as e:
        print(f"Error collecting {data_type['desc']}: {e}")
```

### 2. Bulk Data Collection with Batch Processing

```python
from pipe.daily_price_pipeline import DailyPricePipeline
from config.alphavantage_config import AlphaVantageConfig

# European stocks selection
symbols = ["ASML.AS", "INGA.AS", "HEIA.AS", "RDSA.AS", "UNA.AS"]

# Initialize pipeline with rate limiting
pipeline = DailyPricePipeline(
    symbols=symbols,
    rate_limiter=AlphaVantageConfig.get_rate_limiter(),
    batch_size=5,  # Process 5 symbols at a time
    output_format="parquet"
)

# Run bulk collection
results = pipeline.run_batch_collection()

for symbol, status in results.items():
    if status["success"]:
        df = status["data"]
        print(f"{symbol}: {len(df)} records collected")
        print(f"  Latest price: {df['close'].iloc[-1]:.2f}")
    else:
        print(f"{symbol}: Error - {status['error']}")
```

### 3. Data Validation and Quality Checks

```python
from utils.validation import FinancialDataValidator, TemporalDataValidator
from core.alphavantage_client import AlphaVantageClient

# Collect data with validation
av_client = AlphaVantageClient()
df = av_client.get_daily_adjusted(symbol="ASML.AS", outputsize="full")

# Initialize validators
financial_validator = FinancialDataValidator()
temporal_validator = TemporalDataValidator()

# Run financial validation
financial_results = financial_validator.validate(df)
print("Financial Validation Results:")
print(f"  OHLC consistency: {financial_results['ohlc_valid']}")
print(f"  Price range valid: {financial_results['price_range_valid']}")  
print(f"  Volume consistency: {financial_results['volume_valid']}")

# Run temporal validation
temporal_results = temporal_validator.validate(df)
print("\nTemporal Validation Results:")
print(f"  Timeline continuity: {temporal_results['timeline_continuous']}")
print(f"  Trading hours valid: {temporal_results['trading_hours_valid']}")
print(f"  Holiday detection: {temporal_results['holidays_detected']}")

# Data quality summary
print(f"\nData Quality Summary:")
print(f"  Total records: {len(df)}")
print(f"  Date range: {df['date'].min()} to {df['date'].max()}")
print(f"  Missing values: {df.isnull().sum().sum()}")
print(f"  Duplicate records: {df.duplicated().sum()}")
```

## 🔄 Data Pipelines

The system includes specialized data pipelines for different types of financial data:

### Daily Price Pipeline

```python
from pipe.daily_price_pipeline import DailyPricePipeline

# Configure daily price collection
pipeline = DailyPricePipeline(
    symbols=["ASML.AS", "INGA.AS"],
    output_dir="data/daily_prices",
    format="parquet",
    validation_enabled=True
)

# Run pipeline
results = pipeline.execute()
```

### Earnings Pipeline

```python
from pipe.earnings_pipeline import EarningsPipeline

# Configure earnings data collection
earnings_pipeline = EarningsPipeline(
    symbols=["ASML.AS", "RDSA.AS"],
    quarters_back=8,  # Last 8 quarters
    output_format="postgres"
)

# Execute earnings collection
earnings_data = earnings_pipeline.run()
```

### Fundamentals Pipeline

```python
from pipe.fundamentals_pipeline import FundamentalsPipeline

# Collect company fundamentals
fundamentals_pipeline = FundamentalsPipeline(
    symbols=["INGA.AS", "UNA.AS"],
    metrics=["revenue", "eps", "pe_ratio", "market_cap"],
    annual_reports=True
)

# Run fundamentals collection
fundamentals_data = fundamentals_pipeline.execute()
```

## 💾 Storage Options

### Parquet File Storage

```python
from storage.writer_parquet import ParquetWriter

# Initialize Parquet writer
writer = ParquetWriter(
    base_path="data/parquet",
    partition_by=["year", "month"],
    compression="snappy"
)

# Write data with partitioning
writer.write_dataframe(
    df=price_data,
    table_name="daily_prices",
    symbol="ASML.AS"
)
```

### PostgreSQL Storage

```python
from storage.writer_postgres import PostgresWriter

# Configure PostgreSQL connection
pg_writer = PostgresWriter(
    host="localhost",
    database="financial_data",
    schema="european_markets"
)

# Write data to PostgreSQL
pg_writer.write_batch(
    data=earnings_data,
    table="earnings",
    if_exists="append"
)
```

### Data Layout Management

```python
from storage.layout import DataLayout

# Define data layout structure
layout = DataLayout(
    bronze_layer="raw_data",      # Raw API responses
    silver_layer="processed",     # Cleaned and validated
    gold_layer="analytics"        # Analysis-ready datasets
)

# Apply layout to storage
layout.organize_data(source_path="data/raw", target_structure="medallion")
```

## ⚙️ Advanced Configuration

### Rate Limiting Integration

```python
from core.rate_limiter import RateLimiter
from config.alphavantage_config import AlphaVantageConfig

# Configure rate limiting for Alpha Vantage
rate_limiter = RateLimiter(
    requests_per_minute=AlphaVantageConfig.RATE_LIMIT_CALLS_PER_MINUTE,  # 5 for free tier
    requests_per_day=AlphaVantageConfig.RATE_LIMIT_CALLS_PER_DAY,        # 500 for free tier
    burst_limit=2
)
```

### Advanced HTTP Client Configuration

```python
from core.http_client import HTTPClient
from core.retry import RetryConfig

# Configure HTTP client with retry logic
http_client = HTTPClient(
    timeout=30,
    retry_config=RetryConfig(
        max_attempts=3,
        backoff_strategy="exponential",
        base_delay=1.0,
        max_delay=60.0,
        jitter=True
    )
)

# Use with Alpha Vantage client
av_client = AlphaVantageClient(http_client=http_client)
```

### Utilities Integration

```python
from utils.hashing import DataHasher
from utils.timezones import EuropeanTimezones
from utils.validation import BasicDataValidator

# Data integrity checking
hasher = DataHasher()
data_hash = hasher.hash_dataframe(df)
print(f"Data hash: {data_hash}")

# Timezone conversion for European markets
tz_handler = EuropeanTimezones()
amsterdam_time = tz_handler.convert_to_market_time("ASML.AS", utc_timestamp)

# Basic data validation
validator = BasicDataValidator()
validation_report = validator.validate_dataframe(df)
```

## 🧪 Testing

The project includes comprehensive tests for all components and data validation:

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

# Run API key management tests
python test_api_key.py
```

### Data Validation Tests

```bash
# Run data validation tests
python test_validation_basic.py      # Basic data validation
python test_validation_financial.py  # Financial data validation  
python test_validation_temporal.py   # Temporal data validation
```

### Test Coverage

- **Core Functionality**: `test.py` - Basic logging and core features
- **Data Collection**: `test_chart.py` - Chart data collection endpoints
- **Rate Limiting**: `test_rate_limiter.py` - Rate limiting mechanisms
- **HTTP Clients**: `test_http_client.py` - HTTP client functionality
- **Retry Logic**: `test_retry.py` - Retry mechanisms and backoff
- **API Management**: `test_api_key.py` - API key handling and validation
- **Basic Validation**: `test_validation_basic.py` - Basic data validation rules
- **Financial Validation**: `test_validation_financial.py` - OHLCV data validation
- **Temporal Validation**: `test_validation_temporal.py` - Time series validation

### Example Test Output

```bash
🚀 TESTING CHART DATA COLLECTOR FOR FORECASTING

=== Test: Alpha Vantage Client ===
✅ API key validation successful
✅ Rate limiting configured: 5 calls/minute

=== Test: Historical Data Collection ===
🔄 Testing daily data for ASML.AS...
   ✅ Success: 252 records collected
   📅 Period: 2023-09-24 to 2024-09-24
   💰 Initial price: 652.30
   💰 Final price: 698.50

=== Test: Data Validation ===
🔄 Running financial validation...
   ✅ OHLC consistency: 100% valid
   ✅ Price range validation: Pass
   ✅ Volume consistency: 98% valid
   
🔄 Running temporal validation...
   ✅ Timeline continuity: Pass
   ✅ Trading hours validation: Pass
   ⚠️ Holiday gaps detected: 8 (expected)
```

### Running Tests with pytest

```bash
# Run all tests with pytest
pytest

# Run specific test categories
pytest -k "validation"     # Run validation tests
pytest -k "alpha_vantage"  # Run Alpha Vantage tests
pytest -k "rate_limit"     # Run rate limiting tests

# Run with coverage
pytest --cov=. --cov-report=html
```

## 📁 Project Structure

```
data-haversting-europe/
│
├── 📁 core/                   # Core system components
│   ├── alphavantage_client.py # Alpha Vantage API client implementation
│   ├── base-endpoints.py      # Base endpoint definitions and interfaces
│   ├── http_client.py        # HTTP client with retry and timeout handling
│   ├── logger.py             # Structured JSON logging with rich formatting
│   ├── rate_limiter.py       # Rate limiting for API compliance
│   └── retry.py              # Retry logic with exponential backoff
│
├── 📁 endpoints/              # Data source endpoints
│   ├── batch-universe.py      # Batch operations for multiple symbols
│   ├── chart.py              # Chart/OHLCV data collection
│   ├── options.py            # Options data endpoints
│   ├── quotes-summary.py     # Quote summary endpoints
│   └── quotes.py             # Real-time quote data
│
├── 📁 pipe/                   # Data processing pipelines
│   ├── daily_price_pipeline.py   # Daily price data collection pipeline
│   ├── earnings_pipeline.py      # Earnings data collection pipeline
│   └── fundamentals_pipeline.py  # Company fundamentals pipeline
│
├── 📁 config/                 # Configuration files
│   ├── alphavantage_config.py # Alpha Vantage API configuration
│   ├── fetch_policies.yml     # Data fetching policies and schedules
│   └── tickers_europe.yml     # European ticker symbol definitions
│
├── 📁 storage/                # Data storage utilities
│   ├── layout.py             # Data layout and medallion architecture
│   ├── writer_parquet.py     # Parquet file writer with partitioning
│   └── writer_postgres.py    # PostgreSQL database writer
│
├── 📁 utils/                  # Utility functions
│   ├── hashing.py            # Data integrity and deduplication utilities
│   ├── timezones.py          # European market timezone handling
│   └── validation.py         # Comprehensive data validation engine
│
├── 📁 tests/                  # Comprehensive test suite
│   ├── test.py                      # Basic functionality tests
│   ├── test_api_key.py             # API key management tests
│   ├── test_chart.py               # Chart data collection tests
│   ├── test_http_client.py         # HTTP client functionality tests
│   ├── test_rate_limiter.py        # Rate limiting mechanism tests
│   ├── test_retry.py               # Retry logic and backoff tests
│   ├── test_validation_basic.py    # Basic data validation tests
│   ├── test_validation_financial.py # Financial data validation tests
│   └── test_validation_temporal.py  # Temporal data validation tests
│
├── 📄 requirements.txt        # Python dependencies with pinned versions
├── 📄 LICENSE                # Creative Commons Attribution-NonCommercial 4.0
└── 📄 README.md              # This comprehensive documentation
```

## 🎯 Supported Markets & Symbols

### European Exchanges

- **Euronext Amsterdam**: `.AS` suffix (e.g., `ASML.AS`, `INGA.AS`)
- **Euronext Paris**: `.PA` suffix
- **Euronext Brussels**: `.BR` suffix
- **London Stock Exchange**: `.L` suffix

### Example Symbols

```python
# Amsterdam Exchange (Euronext)
amsterdam_stocks = [
    "ASML.AS",    # ASML Holding (Semiconductor Technology)
    "INGA.AS",    # ING Group (Banking & Financial Services)  
    "HEIA.AS",    # Heineken (Beverages & Consumer Goods)
    "RDSA.AS",    # Royal Dutch Shell (Oil & Gas)
    "UNA.AS",     # Unilever (Consumer Goods)
    "ADYEN.AS",   # Adyen (Financial Technology)
    "DSM.AS"      # Royal DSM (Materials & Nutrition)
]

# Other European Exchanges  
european_stocks = [
    "SAP.DE",     # SAP (Germany - Technology)
    "NESN.SW",    # Nestlé (Switzerland - Consumer Goods)
    "ASME.L",     # ASML (London listing)
    "OR.PA"       # L'Oréal (France - Consumer Goods)
]
```

## 🔧 Advanced Features

### Async Data Collection

```python
import asyncio
from core.alphavantage_client import AlphaVantageClient
from endpoints.chart import AsyncChartCollector

async def collect_async():
    # Initialize async client
    av_client = AlphaVantageClient(async_mode=True)
    collector = AsyncChartCollector(client=av_client)
    
    symbols = ["ASML.AS", "INGA.AS", "HEIA.AS"]
    
    # Create async tasks with rate limiting
    tasks = []
    for symbol in symbols:
        task = collector.get_daily_data_async(
            symbol=symbol,
            outputsize="compact"
        )
        tasks.append(task)
    
    # Execute with controlled concurrency
    results = await asyncio.gather(*tasks, return_exceptions=True)
    return dict(zip(symbols, results))

# Run async collection
async def main():
    data = await collect_async()
    for symbol, result in data.items():
        if isinstance(result, Exception):
            print(f"Error for {symbol}: {result}")
        else:
            print(f"{symbol}: {len(result)} records collected")

asyncio.run(main())
```

### Enterprise Features

```python
# Batch processing with monitoring
from pipe.daily_price_pipeline import DailyPricePipeline
from core.logger import logger

# Configure enterprise pipeline
pipeline = DailyPricePipeline(
    symbols=amsterdam_stocks,
    batch_size=5,
    parallel_batches=2,
    monitoring_enabled=True,
    failure_recovery=True
)

# Execute with full monitoring
results = pipeline.execute_with_monitoring()

# Check execution metrics
metrics = pipeline.get_execution_metrics()
logger.info("Pipeline completed", extra={
    "total_symbols": metrics["total_symbols"],
    "success_rate": metrics["success_rate"],
    "total_records": metrics["total_records"],
    "execution_time": metrics["duration_seconds"]
})
```

### Data Export Options

```python
from storage.writer_parquet import ParquetWriter
from storage.writer_postgres import PostgresWriter

# Export to Parquet with partitioning (recommended)
parquet_writer = ParquetWriter(compression="snappy")
parquet_writer.write_dataframe(
    df=df,
    path=f"data/parquet/{symbol}/",
    partition_cols=["year", "month"]
)

# Export to PostgreSQL for analytics
postgres_writer = PostgresWriter()
postgres_writer.write_batch(
    data=df,
    table=f"daily_prices_{symbol.lower().replace('.', '_')}",
    schema="european_markets"
)

# Traditional exports
df.to_parquet(f"data/{symbol}_daily.parquet", index=False)
df.to_csv(f"data/{symbol}_daily.csv", index=False)
```

## 📚 API Documentation

### Alpha Vantage Client

The core `AlphaVantageClient` provides access to all Alpha Vantage API functions:

#### Core Methods

```python
from core.alphavantage_client import AlphaVantageClient

client = AlphaVantageClient()

# Time Series Functions
daily_data = client.get_daily(symbol="ASML.AS", outputsize="full")
weekly_data = client.get_weekly(symbol="ASML.AS")  
monthly_data = client.get_monthly(symbol="ASML.AS")
intraday_data = client.get_intraday(symbol="ASML.AS", interval="5min")

# Quote and Search Functions  
quote = client.get_quote(symbol="ASML.AS")
search_results = client.search_symbols(keywords="ASML")

# Company Overview
company_overview = client.get_company_overview(symbol="ASML.AS")

# Earnings and Fundamentals
earnings = client.get_earnings(symbol="ASML.AS")
income_statement = client.get_income_statement(symbol="ASML.AS")
balance_sheet = client.get_balance_sheet(symbol="ASML.AS")
cash_flow = client.get_cash_flow(symbol="ASML.AS")
```

#### Response Format

All methods return structured data:

```python
# Example daily data response
{
    'metadata': {
        'symbol': 'ASML.AS',
        'last_refreshed': '2024-12-24',
        'output_size': 'Compact',
        'timezone': 'US/Eastern'
    },
    'data': pandas.DataFrame([
        {
            'date': '2024-12-24',
            'open': 698.50,
            'high': 705.20, 
            'low': 695.10,
            'close': 702.30,
            'volume': 1234567
        },
        # ... more records
    ])
}
```

### Data Pipeline APIs

#### Daily Price Pipeline

```python
from pipe.daily_price_pipeline import DailyPricePipeline

# Initialize pipeline
pipeline = DailyPricePipeline(
    symbols=["ASML.AS", "INGA.AS"],
    start_date="2024-01-01",
    end_date="2024-12-24", 
    output_format="parquet",
    validation_level="strict"
)

# Execute pipeline
results = pipeline.execute()
# Returns: Dict[str, PipelineResult]
```

#### Validation Engine APIs

```python
from utils.validation import (
    FinancialDataValidator,
    TemporalDataValidator, 
    BasicDataValidator
)

# Financial validation
validator = FinancialDataValidator(
    price_range_check=True,
    ohlc_consistency=True,
    volume_validation=True
)

validation_result = validator.validate(dataframe)
# Returns: ValidationResult with detailed findings
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

## 📈 Performance & Scalability

- **Rate Limiting**: Automatic compliance with Alpha Vantage API limits
- **Concurrent Processing**: Configurable batch processing with async support
- **Memory Optimization**: Streaming data processing for large datasets
- **Storage Efficiency**: Parquet format with compression and partitioning
- **Monitoring**: Built-in metrics and logging for production deployments

## 🔒 Security & Best Practices

- **API Key Management**: Environment variable based configuration
- **Data Validation**: Multi-layer validation (basic, financial, temporal)
- **Error Handling**: Comprehensive exception handling with retry logic
- **Logging**: Structured logging with request tracking and audit trails
- **Data Integrity**: Hashing and deduplication utilities

---

*Last updated: December 2024*
*API Version: Alpha Vantage Standard*
*Python Version: 3.8+*
