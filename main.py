from airqualityETL import AirQualityETL

if __name__ == "__main__":
    etl = AirQualityETL("nested_air_quality.json", "air_quality.db")
    etl.run()
