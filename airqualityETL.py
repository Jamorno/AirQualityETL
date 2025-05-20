import json, sqlite3, logging
from datetime import datetime
import pandas as pd

logging.basicConfig(
    filename="air_quality-log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

class AirQualityETL:
    def __init__(self, json_file, db_file):
        self.json_file = json_file
        self.db_file = db_file

    def extract_data(self):
        try:
            with open(self.json_file, "r", encoding="utf-8") as file:
                data = json.load(file)
            return data
        except Exception as e:
            logging.info(f"Failed to extract JSON: {e}")
            raise

    def transform_data(self, data):
        result = []
        for location in data["results"]:
            for item in location["measurements"]:
                if item["parameter"] == "pm25":
                    dt_obj = datetime.strptime(item["lastUpdated"], "%Y-%m-%dT%H:%M:%S%z")
                    dt = dt_obj.strftime("%Y-%m-%d %H:%M")

                    clean = {
                        "city": location["location"],
                        "pm25": item["value"],
                        "unit": item["unit"],
                        "datetime": dt
                    }

                    result.append(clean)

        return result

    def init_db(self):
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()

        create_table_query = """CREATE TABLE IF NOT EXISTS air_quality (city TEXT, pm25 REAL, unit TEXT, datetime TEXT)"""

        cursor.execute(create_table_query)
        conn.commit()
        conn.close()

    def insert_air_data(self, data):
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()

        for item in data:
            query = "INSERT INTO air_quality (city, pm25, unit, datetime) VALUES (?, ?, ?, ?)"
            values = (item["city"], item["pm25"], item["unit"], item["datetime"])
            cursor.execute(query, values)

        conn.commit()
        conn.close()

    def export_summary(self):
        conn = sqlite3.connect(self.db_file)

        query = """SELECT city, ROUND(AVG(pm25), 2) AS air_quality, COUNT(*) AS records FROM air_quality GROUP BY city"""
        df = pd.read_sql(query, conn)
        df.to_csv("air_quality.csv", index=False)
        conn.close()

        print("Exported summary to air_quality.csv")

    def run(self):
        logging.info("ETL process start..")

        raw = self.extract_data()
        logging.info(f"Extract {len(raw)} records.")

        clean = self.transform_data(raw)
        logging.info(f"Transform {len(clean)} pm25 records.")

        self.init_db()
        logging.info("Database initialized.")

        self.insert_air_data(clean)
        logging.info(f"Inserted {len(clean)} records into air_quality table.")

        self.export_summary()
        logging.info("Export summary into air_quality.csv")

        print(clean)

        logging.info("ETL process completed successfully.")
        logging.info("------------------------------------------------------")
        print("Insert data complete.")