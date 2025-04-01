from db import model as db_model


# when remove feature flag
# remove this function and update related tests
def is_parent_org(config: db_model.Configuration) -> bool:
    # removed flagr feature flag
    # will apply launch darkly feature flag in https://mavenclinic.atlassian.net/browse/ELIG-1957
    feature_flag_for_org = False
    return feature_flag_for_org and config.data_provider
