# TW Securities Collector

Crawls TWSE-listed securities and their daily trading bars into PostgreSQL.

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
collector
```

## GitHub Actions

Runs daily via [.github/workflows/main.yml](.github/workflows/main.yml). Add `POSTGRES_URL` as a repository secret:

Settings > Secrets and variables > Actions > New repository secret
