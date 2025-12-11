# ETL Pipeline for Trading Data
ETL pipeline for extract data from CSV file, transform to aggegate data and analyze them, load all prepared data to csv files

## Requirements

- Python 3.8+
- Dependencies from `requirements.txt`

## Installation

```bash
# Clone repository
git clone <repository-url>
cd test_etl

# Install dependencies
pip install -r requirements.txt
```

## Structure

```
test_etl/
├── main.py                 # ETL pipeline entry point
├── config/
│   └── config.py          # Path and parameter configuration
├── src/
│   ├── extract.py         # Data extraction from CSV
│   ├── transform.py       # Transformation(aggregating data) and PnL calculation
│   ├── load.py           # Save results
│   └── analytics.py      # Top clients analytics
├── data/
│   ├── input/            # Input data (trades.csv)
│   └── output/           # Processing results
└── logs/                 # Execution logs
```

## Local manual run the ETL

```bash
python3 main.py
```

## ETL description

### 1. **Extract** 
- Reads trading data from `data/input/trades.csv`
- Validates file existence

### 2. **Transform** 
- Cleans data (duplicates, missing values)
- Parses dates and groups by week (Monday start)
- Aggregates trades by: week, user, client type, symbol
- Calculates total volume, total trades, cumulative volume, unrealized/realized/total PnL

### 3. **Load** 
Creates 3 output files:
- `agg_trades_weekly.csv` - weekly aggregation of all trades
- `top_clients_by_volume.csv` - top 3 bronze clients by volume
- `top_clients_by_pnl.csv` - top 3 bronze clients by profit



### Automation

This ETL pipeline can run automatically via GitHub Actions:

### Schedule
- **Weekly**: Every Monday at 00:00 UTC
- **Manual**: Trigger via GitHub Actions UI

### Manual Trigger
1. Go to **Actions** tab in GitHub
2. Select **ETL Pipeline** workflow
3. Click **Run workflow**

### View output data
Download artifacts (logs & output files) from the workflow run page.

### Setup
1. Create `.github/workflows/etl-pipeline.yml`
3. Commit and push

## Как бы вы адаптировали решение под 100+ млн строк:
    Какие технологии замените/добавите?
        AirFlow (cost efficiency Step Functions)
        Spark/Pyspark instead pure Python
        SQL/PosgreSQL for modeling/storage data
    Какую архитектуру ETL предложите?
    Crypto Exchanges (APIs)
         |
    Orchestration  (Step Functions /managed Airflow)
         |
    PySpark (EMR Serverless/AWS Glue)                
    - Validate & Clean                    
    - Transform & Aggregate               
    - Calculate metrics                                                            
        if DEBUG_MODE: ──→ S3 /debug/(24h lifecycle)           
         |
    PostgreSQL / TimescaleDB (aggregated data only) as option Clickhouse

    Какие метрики мониторинга ETL вы бы внедрили?
    ETL metrics: 
        execution runtime, 
        row counts(input/output)
    Bussiness metrics: 
        anomaly trade volume long/short side,
        position inbalance,
        pnl outlayeries

## Dependencies

```
pandas==2.1.4
numpy==1.26.2
python-dateutil==2.8.2
```

## License

MIT
