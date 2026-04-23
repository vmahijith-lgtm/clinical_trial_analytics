# Clinical Trial Analytics Platform

Streamlit application for ingesting and analyzing clinical trial Excel data with quality checks, interactive analytics, and optional AI-assisted insights.

## Features

- Data ingestion from Excel workbooks (`.xlsx`, `.xls`)
- Data quality dashboard and summary metrics
- Analytics and statistical exploration views
- Chat-style data interaction and multi-format exports
- SQLite-backed persistence across restarts

## Prerequisites

- Python 3.11+ recommended
- pip

## Quick Start (Local)

1. Clone and enter the repository.
2. Create and activate a virtual environment.
3. Install dependencies.
4. Copy environment template.
5. Start Streamlit.

```bash
git clone https://github.com/vmahijith-lgtm/clinical_trial_analytics.git
cd clinical_trial_analytics

python -m venv .venv
source .venv/bin/activate

pip install -r requirements.txt
cp .env.example .env

streamlit run Home.py
```

The app is available at `http://localhost:8501`.

## Data Directory

By default, the app scans `data/` for Excel files.

- Local default: `./data`
- Override with environment variable: `DATA_DIR=/absolute/path/to/data`

## Environment Variables

| Variable | Required | Purpose |
|---|---|---|
| `ANTHROPIC_API_KEY` | No | Enables AI-powered chat/insights features |
| `DATA_DIR` | No | Overrides default input data directory |

If `ANTHROPIC_API_KEY` is not set, the rest of the platform still works.

## Deployment

### Option 1: Docker

```bash
docker build -t clinical-trial-analytics .
docker run --rm -p 8501:8501 \
	-e PORT=8501 \
	-e DATA_DIR=/app/data \
	-e ANTHROPIC_API_KEY=your_key_if_needed \
	clinical-trial-analytics
```

Notes:

- Mount a host data directory if needed: `-v /host/data:/app/data`
- SQLite database is written to `database/analytics.db` in the container filesystem unless you mount persistent storage.

### Option 2: PaaS / Buildpack-style Deployments

This repository includes a `Procfile`:

```text
web: streamlit run Home.py --server.address=0.0.0.0 --server.port=$PORT
```

Use this for platforms like Render, Railway, or other process-based Python deploy targets.

### Option 3: Streamlit Community Cloud

1. Connect the repository in Streamlit Cloud.
2. Set main file to `Home.py`.
3. Add secrets (optional) for `ANTHROPIC_API_KEY`.
4. Ensure data source strategy is defined (uploaded files, external storage, or mounted volume pattern).

## Storage Model

- Metadata and dataset payloads are persisted in SQLite (`database/analytics.db`).
- Runtime cache folders may be used for temporary processing artifacts.

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE).
