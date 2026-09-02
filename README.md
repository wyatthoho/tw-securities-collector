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
collector                      # securities + daily bars
corporate-actions-collector    # ex-rights/ex-dividend + split events
```

## GitHub Actions

- `daily-collector` runs daily via [.github/workflows/daily.yml](.github/workflows/main.yml).
- `corporate-actions-collector` runs on its own schedule via [.github/workflows/corporate_actions.yml](.github/workflows/corporate_actions.yml).

Both workflows read `POSTGRES_URL` from a repository secret:

Settings > Secrets and variables > Actions > New repository secret
