import os
import sys
from datetime import datetime

import pandas as pd
from sqlalchemy.orm import Session

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from storage.db_writer import MachineSensorRaw, engine

# source file path for historical raw machine data
INPUT_FILE = (
    r"C:\Users\AbdulRauf(AIEngineer\OneDrive - Standardverzeichnis\Desktop\PM-Project - Copy"
    r"\timeSeriesDB\time-series-database\process_segmentation_outputs\results\Raw_Extruder_data.csv"
)

# batch size for insert operations
BATCH_SIZE = 1000

# sensor columns to map from input file into MachineSensorRaw
SENSOR_COLUMNS = [
    "Val_1",
    "Val_2",
    "Val_3",
    "Val_4",
    "Val_5",
    "Val_6",
    "Val_7",
    "Val_8",
    "Val_9",
    "Val_10",
    "Val_11",
    "Val_19",
    "Val_20",
    "Val_27",
    "Val_28",
    "Val_29",
    "Val_30",
    "Val_31",
    "Val_32",
    "Val_33",
]


def parse_trend_date(value):
    # parse TrendDate to UTC then store as naive datetime for SQLite
    if pd.isna(value):
        return None
    parsed = pd.to_datetime(value, utc=True, errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed.to_pydatetime().replace(tzinfo=None)


def read_input(path: str) -> pd.DataFrame:
    # read CSV or XLSX based on file extension
    ext = os.path.splitext(path)[1].lower()
    if ext == ".csv":
        return pd.read_csv(path)
    if ext in (".xlsx", ".xls"):
        return pd.read_excel(path)
    raise ValueError(f"Unsupported file extension: {ext}")


def print_summary() -> None:
    # print final counts by source and total rows
    with Session(engine) as session:
        historical_count = session.query(MachineSensorRaw).filter(
            MachineSensorRaw.source == "historical_import"
        ).count()
        live_count = session.query(MachineSensorRaw).filter(
            MachineSensorRaw.source == "live_api"
        ).count()
        total_count = session.query(MachineSensorRaw).count()

    print(f"historical_import count: {historical_count}")
    print(f"live_api count: {live_count}")
    print(f"total count: {total_count}")


def main() -> None:
    # skip import if historical data already exists
    with Session(engine) as session:
        existing = session.query(MachineSensorRaw).filter(
            MachineSensorRaw.source == "historical_import"
        ).count()
    if existing > 0:
        print("historical_import rows already exist in machine_sensor_raw. Skipping import.")
        print_summary()
        return

    # load source data file
    df = read_input(INPUT_FILE)
    if "TrendDate" not in df.columns:
        raise ValueError("Input file must contain 'TrendDate' column.")

    # fill missing sensor columns with None so mapping always works
    for col in SENSOR_COLUMNS:
        if col not in df.columns:
            df[col] = None

    total_rows = len(df)
    inserted_rows = 0

    # insert rows in batches for better performance and stability
    with Session(engine) as session:
        batch = []
        for idx, row in df.iterrows():
            trend_date = parse_trend_date(row.get("TrendDate"))
            now_utc_naive = datetime.utcnow()

            data = {
                "trend_date": trend_date,
                "recorded_at": now_utc_naive,
                "source": "historical_import",
                "created_at": now_utc_naive,
            }
            for col in SENSOR_COLUMNS:
                value = row.get(col)
                data[col] = None if pd.isna(value) else value

            batch.append(MachineSensorRaw(**data))

            # commit each full batch and print progress
            if len(batch) >= BATCH_SIZE:
                session.add_all(batch)
                session.commit()
                inserted_rows += len(batch)
                print(f"Inserted batch: {inserted_rows}/{total_rows}")
                batch = []

        # commit any remaining rows in the last partial batch
        if batch:
            session.add_all(batch)
            session.commit()
            inserted_rows += len(batch)
            print(f"Inserted batch: {inserted_rows}/{total_rows}")

    print("Historical raw data import completed.")
    print_summary()


if __name__ == "__main__":
    main()
