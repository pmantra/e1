import csv
import datetime
import random
from typing import List, Tuple

TIMESTAMP = datetime.datetime.now().strftime("%m_%d_%Y_%H_%M_%S")

DEFAULT_FILENAME = f".generated/sample_{TIMESTAMP}.csv"

FIRST_NAME = "Test"
LAST_NAME = "McTestface"

EXTERNAL_IDS = [("1234", "3456")]


def _get_random_past_date() -> str:
    start_date = datetime.date(1950, 1, 1)
    end_date = datetime.date.today()

    time_between_dates = end_date - start_date
    days_between_dates = time_between_dates.days
    random_number_of_days = random.randrange(days_between_dates)
    random_date = start_date + datetime.timedelta(days=random_number_of_days)
    return random_date


def generate_file(file_name: str, external_ids: List[Tuple[str, str]]):
    data = [
        dict(
            first_name=f"{FIRST_NAME}{i:07}",
            last_name=LAST_NAME,
            date_of_birth=_get_random_past_date(),
            email=f"test.mctestface+{i:07}@undeliverable.net",
            unique_corp_id=200 + i,
            dependent_id=300 + i,
            client_id=external_ids[i % len(external_ids)][0],
            customer_id=external_ids[i % len(external_ids)][1],
        )
        for i in range(0, 100)
    ]

    with open(file_name, "w", newline="") as file:
        fieldnames = [*data[0].keys()]
        writer = csv.DictWriter(file, fieldnames)
        writer.writeheader()
        writer.writerows(data)


if __name__ == "__main__":
    generate_file(DEFAULT_FILENAME, EXTERNAL_IDS)
