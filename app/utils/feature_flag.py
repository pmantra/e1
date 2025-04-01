from typing import Set

from maven import feature_flags

from app.eligibility import constants as e9y_constants


def organization_enabled_for_e9y_2_read(organization_id: int) -> bool:
    enabled_orgs = set(
        feature_flags.json_variation(
            e9y_constants.E9yFeatureFlag.RELEASE_ELIGIBILITY_2_ENABLED_ORGS_READ,
            default=[],
        )
    )
    return organization_id in enabled_orgs


def organization_enabled_for_e9y_2_write(organization_id: int) -> bool:
    enabled_orgs = set(
        feature_flags.json_variation(
            e9y_constants.E9yFeatureFlag.RELEASE_ELIGIBILITY_2_ENABLED_ORGS_WRITE,
            default=[],
        )
    )
    return organization_id in enabled_orgs


def is_overeligibility_enabled() -> bool:
    return feature_flags.bool_variation(
        e9y_constants.E9yFeatureFlag.RELEASE_OVER_ELIGIBILITY,
        default=False,
    )


def are_all_organizations_enabled_for_overeligibility(
    organization_ids: Set[int],
) -> bool:
    flag_value = feature_flags.json_variation(
        e9y_constants.E9yFeatureFlag.RELEASE_OVER_ELIGIBILITY_ENABLED_ORGS,
        default={},
    )
    enabled_all_orgs = flag_value.get("enabled_all_orgs")
    enabled_organization_ids = set(flag_value.get("organizations", []))

    # check if all orgs are enabled
    if enabled_all_orgs:
        return True

    if not organization_ids or not enabled_organization_ids:
        return False

    # Check if all organization IDs are in the enabled orgs
    return organization_ids.issubset(enabled_organization_ids)


def is_write_disabled() -> bool:
    return feature_flags.bool_variation(
        e9y_constants.E9yFeatureFlag.E9Y_DISABLE_WRITE,
        default=False,
    )


def is_optum_file_logging_enabled() -> bool:
    return feature_flags.bool_variation(
        e9y_constants.E9yFeatureFlag.RELEASE_OPTUM_FILE_LOGGING_SWITCH,
        default=False,
    )
