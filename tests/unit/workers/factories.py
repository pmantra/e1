from typing import Iterable

import factory
from tests.factories import data_models as factories

import app.eligibility.constants
from app.eligibility.domain import model
from app.worker import types


class PendingFileFactory(factory.Factory):
    class Meta:
        model = types.PendingFileNotification

    file_id = factory.Sequence(str)


class PersistFileFactory(factory.Factory):
    class Meta:
        model = types.PersistFileMessage

    id = factory.Sequence(str)
    action = factory.Faker(
        "random_element", elements=[*app.eligibility.constants.ProcessingTag]
    )


class PersistRecordFactory(factory.Factory):
    class Meta:
        model = types.PersistRecordMessage

    key = factory.Faker("word")


class PendingFileStreamEntryFactory(factories.RedisStreamEntryFactory):
    message = factory.SubFactory(PendingFileFactory)


def processed_data(keys: Iterable[str] = ()) -> dict:
    return {k: factories.RecordFactory.create(key=k) for k in keys}


class ProcessedRecordsFactory(factory.Factory):
    class Meta:
        model = model.ProcessedRecords

    valid = factory.LazyFunction(processed_data)
    errors = factory.LazyFunction(processed_data)
    missing = factory.LazyFunction(processed_data)
