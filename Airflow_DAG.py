"""Airflow DAG for daily batch ETL of toll-road data."""

from __future__ import annotations

from datetime import timedelta
import csv
import logging
import os
import random
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

LOGGER = logging.getLogger("toll.etl")

DATA_DIR = os.getenv("TOLL_DATA_DIR", "/tmp/toll_data")
ROW_COUNT = int(os.getenv("TOLL_DATA_ROWS", "500"))
NOTIFICATION_EMAILS = os.getenv("TOLL_ALERT_EMAILS", "alerts@example.com").split(",")


def generate_sample_files(data_dir: str, row_count: int) -> None:
    """Generate realistic daily batch files for the ETL pipeline."""

    rng = random.Random(42)
    data_path = Path(data_dir)
    data_path.mkdir(parents=True, exist_ok=True)

    vehicle_types = [
        "car",
        "car",
        "car",
        "suv",
        "van",
        "pickup",
        "truck",
        "truck",
        "bus",
        "motorcycle",
    ]
    payment_methods = ["transponder", "card", "cash"]
    directions = ["N", "S", "E", "W"]

    vehicle_file = data_path / "vehicle-data.csv"
    plaza_file = data_path / "tollplaza-data.tsv"
    payment_file = data_path / "payment-data.txt"

    with vehicle_file.open("w", newline="", encoding="utf-8") as csv_handle, plaza_file.open(
        "w", newline="", encoding="utf-8"
    ) as tsv_handle, payment_file.open("w", encoding="utf-8") as payment_handle:
        vehicle_writer = csv.writer(csv_handle)
        plaza_writer = csv.writer(tsv_handle, delimiter="\t")

        for _ in range(row_count):
            vehicle_type = rng.choice(vehicle_types)
            axle_count = 2 if vehicle_type in {"car", "suv", "van", "pickup", "motorcycle"} else 4
            base_toll = 2.5 if axle_count == 2 else 6.0

            timestamp = days_ago(0).strftime("%Y-%m-%d")
            vehicle_id = rng.randint(100000, 9999999)
            plaza_id = rng.randint(4000, 4020)
            lane_id = rng.randint(1, 12)
            direction = rng.choice(directions)
            speed_kph = round(rng.uniform(30.0, 120.0), 1)
            payment_method = rng.choice(payment_methods)
            toll_amount = round(base_toll + rng.uniform(0.5, 3.0), 2)

            vehicle_writer.writerow([timestamp, vehicle_id, vehicle_type, plaza_id])
            plaza_writer.writerow([lane_id, direction, speed_kph])
            payment_handle.write(f"{payment_method:<10}{toll_amount:>8.2f}\n")

    LOGGER.info("Sample files generated in %s", data_dir)


# defining DAG arguments

default_args = {
    "owner": "data-platform",
    "start_date": days_ago(1),
    "email": NOTIFICATION_EMAILS,
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}


# define the DAG

dag = DAG(
    dag_id="ETL_toll_data",
    default_args=default_args,
    description="Daily ETL for toll-road batch files",
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=["toll", "etl", "batch"],
)


generate_data = PythonOperator(
    task_id="generate_sample_data",
    python_callable=generate_sample_files,
    op_kwargs={"data_dir": DATA_DIR, "row_count": ROW_COUNT},
    dag=dag,
)

# define the second task (extract)
extract_data_from_csv = BashOperator(
    task_id="extract_data_from_csv",
    bash_command=(
        f"cut -d',' -f1-4 {DATA_DIR}/vehicle-data.csv > {DATA_DIR}/csv_data.csv"
    ),
    dag=dag,
)

# define the third task (extract)
extract_data_from_tsv = BashOperator(
    task_id="extract_data_from_tsv",
    bash_command=(
        f"cut -f1-3 {DATA_DIR}/tollplaza-data.tsv > {DATA_DIR}/extract.tsv; "
        f"tr '\t' ',' < {DATA_DIR}/extract.tsv > {DATA_DIR}/tsv_data.csv"
    ),
    dag=dag,
)

# define the fourth task (extract)
extract_data_from_fixed_width = BashOperator(
    task_id="extract_data_from_fixed_width",
    bash_command=(
        f"awk '{{print substr($0,1,10) "," substr($0,11,8)}}' "
        f"{DATA_DIR}/payment-data.txt > {DATA_DIR}/fixed_width_data.csv"
    ),
    dag=dag,
)

# define the fifth task (transform)
consolidate_data = BashOperator(
    task_id="consolidate_data",
    bash_command=(
        f"paste -d',' {DATA_DIR}/csv_data.csv "
        f"{DATA_DIR}/tsv_data.csv {DATA_DIR}/fixed_width_data.csv "
        f"> {DATA_DIR}/extracted_data.csv"
    ),
    dag=dag,
)

# define the sixth task (transform)
transform_data = BashOperator(
    task_id="transform_data",
    bash_command=(
        f"tr '[a-z]' '[A-Z]' < {DATA_DIR}/extracted_data.csv "
        f"> {DATA_DIR}/transformed_data.csv"
    ),
    dag=dag,
)


# task pipeline

generate_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width
consolidate_data >> transform_data
extract_data_from_fixed_width >> consolidate_data
