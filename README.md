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
