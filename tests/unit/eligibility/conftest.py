import csv
import io
from unittest import mock

import pytest
from maven import feature_flags

from app.eligibility.client_specific import service
from db import model


def fake_records(faker, num: int = 10):
    return [
        {
            "date_of_birth": faker.date(),
            "unique_corp_id": f"{faker.pyint()}",
            "dependent_id": f"{faker.pyint()}",
            "gender": faker.word(),
            "beneficiaries_enabled": f"{faker.boolean()}".lower(),
            "wallet_enabled": f"{faker.boolean()}".lower(),
            "work_state": faker.state(),
            "first_name": faker.first_name(),
            "last_name": faker.first_name(),
            "email": faker.email(),
            "company_couple": f"{faker.boolean()}".lower(),
            "address_1": faker.street_address(),
            "address_2": "",
            "city": faker.city(),
            "state": faker.state(),
            "zip_code": faker.postcode(),
            "country": "US",
            "sub_population_identifier": f"{faker.pyint()}",
        }
        for i in range(num)
    ]


def make_csv(rows) -> str:
    stream = io.StringIO()
    writer = csv.DictWriter(stream, dialect="excel", fieldnames=rows[0].keys())
    writer.writeheader()
    writer.writerows(rows)
    stream.seek(0)
    return stream.getvalue()


@pytest.fixture
def file_data(faker):
    rows = fake_records(faker)
    return make_csv(rows)


@pytest.fixture
def file_data_bad_format(faker):
    rows = fake_records(faker)
    # Alter some data so it has a bad format
    rows[0]["date_of_birth"] = "2222.05.16"
    rows[1]["email"] = "thisisnotanemail"
    rows[1]["date_of_birth"] = "not_date_of_birth"

    return make_csv(rows)


@pytest.fixture
def mock_msft_check():
    with mock.patch(
        "app.eligibility.client_specific.microsoft.MicrosoftSpecificProtocol",
        autospec=True,
    ) as m:
        yield m.return_value


@pytest.fixture(autouse=False)
def client_specific_service(
    members, members_versioned, members_2
) -> service.ClientSpecificService:
    with mock.patch.object(
        service.ClientSpecificService, "_get_mode", autospec=True
    ) as m:
        m.return_value = model.ClientSpecificMode.ONLY_CENSUS
        svc = service.ClientSpecificService(
            members=members, members_versioned=members_versioned, members_2=members_2
        )
        yield svc


@pytest.fixture
def ff_test_data():
    with feature_flags.test_data() as td:
        yield td
