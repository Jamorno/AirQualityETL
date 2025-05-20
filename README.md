# AirQualityETL 🌫️

A simple ETL pipeline built with Python to extract, transform, and load air quality (PM2.5) data from a nested JSON file into a SQLite database. Includes logging and CSV summary export.

## 🔧 Features

- Extract PM2.5 data from a nested JSON structure
- Transform data: parse timestamps, flatten nested fields
- Load into SQLite (`air_quality` table)
- Export summary (average PM2.5 per city) to `pm25_summary.csv`
- Log ETL process to `air_quality_etl.log`

## ▶️ How to Run

```bash
python main.py