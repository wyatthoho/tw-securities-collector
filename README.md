# TW Securities Collector

Crawls TWSE-listed securities, their daily trading bars, and corporate action events (ex-rights/ex-dividend, stock splits) into PostgreSQL.

## Requirements
- Python >= 3.12
- PostgreSQL

## Installation

```bash
pip install -e .
```

## Configuration

Set `POSTGRES_URL` in a `.env` file:

```
POSTGRES_URL=postgresql://user:password@host:port/dbname
```

## Usage

```bash
trading-collector    # securities + daily trading bars
events-collector     # corporate action events: ex-rights/ex-dividend + splits
```

## GitHub Actions

- `trading-collector` runs daily via [.github/workflows/trading.yml](.github/workflows/trading.yml).
- `events-collector` runs daily via [.github/workflows/events.yml](.github/workflows/events.yml).

Both workflows read `POSTGRES_URL` from a repository secret:

Settings > Secrets and variables > Actions > New repository secret
