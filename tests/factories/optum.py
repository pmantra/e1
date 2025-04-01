import factory
from ingestion import model

__all__ = ("OptumPolicyFactory",)


class OptumPolicyFactory(factory.Factory):
    class Meta:
        model = model.OptumPolicy

    customerAccountId = factory.Faker("swift11")
    effectiveDate = factory.Faker("date_this_decade")
    terminationDate = factory.Faker(
        "date_this_decade", before_today=False, after_today=True
    )
    planVariationCode = factory.Faker("swift11")
    reportingCode = factory.Faker("swift11")

    @factory.post_generation
    def finalize(obj, *args, **kwargs):
        if obj["effectiveDate"] is None:
            obj["effectiveDate"] = ""
        elif not isinstance(obj["effectiveDate"], str):
            obj["effectiveDate"] = obj["effectiveDate"].isoformat()

        if obj["terminationDate"] is None:
            obj["terminationDate"] = ""
        elif not isinstance(obj["terminationDate"], str):
            obj["terminationDate"] = obj["terminationDate"].isoformat()


class EffectiveRangeFactory(factory.Factory):
    class Meta:
        model = dict

    lower = None
    upper = None
    lower_inc = True
    upper_inc = True
