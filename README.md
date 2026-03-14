# Clinical Trial Analytics Platform

A **Streamlit-powered web application** for processing, analyzing, and exploring clinical trial data stored in Excel format. The platform provides quality dashboards, statistical analytics, and a natural-language "Chat with Data" interface backed by AI.

---

## Features

| Feature | Description |
|---------|-------------|
| 📂 **Data Overview** | Browse, search, and explore all ingested datasets |
| ✅ **Quality Dashboard** | Automated quality scoring, metrics, and flagging |
| 📊 **Analytics** | Statistical analysis, correlation, and interactive charts |
| 💬 **Chat with Data** | Ask questions in plain English and get instant results |
| 💾 **Data Export** | Download query results as CSV, Excel, or JSON |

---

## Architecture

```
clinical_trial_analytics/
├── Home.py                  # Entry point – file processing & database management
├── pages/
│   ├── 1_Data_Overview.py   # Dataset browser
│   ├── 2_Quality_Dashboard.py  # Quality metrics & scoring
│   ├── 3_Analytics.py       # Statistical analysis & visualizations
│   └── 5_Chat_with_Data.py  # Natural language query interface
├── src/
│   ├── data_ingestion.py    # Excel file reading & preprocessing
│   ├── data_harmonization.py  # Data cleaning & standardization
│   ├── quality_checks.py    # Quality metrics & scoring engine
│   ├── analytics_engine.py  # Statistical analysis functions
│   └── ai_insights.py       # AI-powered analysis tools
├── utils/
│   ├── database.py          # SQLite interface
│   ├── disk_cache.py        # Cache key generation
│   ├── helpers.py           # Shared helper functions
│   ├── memory_manager.py    # Memory optimization utilities
│   └── config.py            # App-wide configuration
├── database/
│   └── analytics.db         # SQLite database (auto-generated, gitignored)
├── data/                    # Raw Excel study files (gitignored)
├── .streamlit/
│   └── config.toml          # Streamlit UI configuration
├── requirements.txt
└── .env.example             # Environment variable template
```

---

## Prerequisites

- Python **3.9+**
- pip

---

## Setup

### 1. Clone the repository

```bash
git clone https://github.com/vmahijith-lgtm/clinical_trial_analytics.git
cd clinical_trial_analytics
```

### 2. Create and activate a virtual environment

```bash
python -m venv venv
# macOS / Linux
source venv/bin/activate
# Windows
venv\Scripts\activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure environment variables

```bash
cp .env.example .env
```

Open `.env` and set your Anthropic API key:

```env
ANTHROPIC_API_KEY=your_anthropic_api_key_here
```

> **Note**: The AI "Chat with Data" feature requires a valid [Anthropic API key](https://console.anthropic.com/). All other features work without it.

### 5. Add your data

Place your clinical trial Excel (`.xlsx` / `.xls`) files inside the `data/` directory. The app will auto-detect and process them on startup.

### 6. Run the app

```bash
streamlit run Home.py
```

The app will open at **http://localhost:8501** in your browser.

---

## How It Works

1. **Ingest** – Excel files are read sheet-by-sheet into memory, validated, and cleaned.
2. **Store** – Each dataset is pickled and stored as a BLOB in a local SQLite database (`database/analytics.db`), enabling persistence across app restarts.
3. **Explore** – Pages provide interactive views: data browsing, quality scoring, advanced analytics, and AI chat.
4. **Export** – Query results can be downloaded in CSV, Excel, or JSON format at any time.

### Performance Characteristics
- Processing: ~1–2 seconds per Excel sheet
- Memory peak: ~300–500 MB during ingestion
- Storage: ~50–100 MB per 100,000 rows in the database

---

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `ANTHROPIC_API_KEY` | Optional* | Powers the "Chat with Data" AI feature |

\* The app runs fully without this key; only the Chat feature will be disabled.

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Frontend | [Streamlit](https://streamlit.io/) |
| Data Processing | [Pandas](https://pandas.pydata.org/), [NumPy](https://numpy.org/) |
| Analytics | [SciPy](https://scipy.org/), [Scikit-learn](https://scikit-learn.org/), [Seaborn](https://seaborn.pydata.org/) |
| Visualization | [Plotly](https://plotly.com/python/) |
| Storage | SQLite (via Python `sqlite3`) |
| AI | [Anthropic Claude](https://www.anthropic.com/) (`anthropic` SDK) |
| Excel I/O | [openpyxl](https://openpyxl.readthedocs.io/), [xlrd](https://xlrd.readthedocs.io/) |

---

## Contributing

1. Fork the repo
2. Create a feature branch: `git checkout -b feature/your-feature`
3. Commit your changes: `git commit -m "feat: add your feature"`
4. Push to the branch: `git push origin feature/your-feature`
5. Open a Pull Request

---

## License

This project is licensed under the [MIT License](LICENSE).
