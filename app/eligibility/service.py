from __future__ import annotations

import asyncio
import contextvars
import datetime
import uuid
from typing import Dict, List, Optional, Union

import asyncpg
from ddtrace import tracer
from mmlib.ops import log
from tests.factories.data_models import (
    DateRangeFactory,
    MemberFactory,
    MemberVersionedFactory,
)
from verification.repository.verification import VerificationRepository

from app.eligibility import client_specific, convert, errors
from app.eligibility.constants import ORGANIZATIONS_NOT_SENDING_DOB, EligibilityMethod
from app.utils import async_ttl_cache
from app.utils import eligibility_member as e9y_member_utils
from app.utils import feature_flag
from app.utils.eligibility_validation import (
    cached_organization_eligibility_type,
    check_member_org_active_and_overeligibility,
    check_member_org_active_and_single_org,
    is_cached_organization_active,
    is_organization_activated,
    is_verification_record_active,
)
from db import model
from db.clients import (
    configuration_client,
    member_2_client,
    member_client,
    member_versioned_client,
    population_client,
    sub_population_client,
)

logger = log.getLogger(__name__)


class EligibilityService:
    """This service internalizes the specific business logic for our service.

    Any idiosyncrasies for validating incoming or outgoing data can be handled here.

    See Also:
        https://www.notion.so/mavenclinic/Eligibility-97fc43a08b224434a418844145622ed4
    """

    __slots__ = (
        "members",
        "members_versioned",
        "configurations",
        "client_specific",
        "verifications",
        "populations",
        "sub_populations",
        "members_2",
    )

    def __init__(self):
        self.members = member_client.Members()
        self.members_versioned = member_versioned_client.MembersVersioned()
        self.configurations = configuration_client.Configurations()
        self.members_2 = member_2_client.Member2Client()
        self.client_specific = client_specific.ClientSpecificService(
            self.members, self.members_versioned, self.members_2
        )
        self.populations = population_client.Populations()
        self.sub_populations = sub_population_client.SubPopulations()
        self.verifications = VerificationRepository()

    # region check_standard_eligibility
    async def check_standard_eligibility(
        self, *, date_of_birth: datetime.date | str, email: str
    ) -> model.MemberResponse:
        """Search for a Member via 'Standard' eligibility information.

        'Standard' eligibility checks for a matching member given a date of birth and an
        email. 'Standard' eligibility is also referred to 'Primary' verification.

        Args:
            date_of_birth: The date of birth to match against.
            email: The email to match against. (case-insensitive)

        Returns:
            A Member record, if one is found.

        Raises:
            A StandardMatchError, if no match is found.
        """
        validated_dob = self._validate_date(date_of_birth)
        member_versioned = await self._get_by_dob_and_email_v1(validated_dob, email)
        member_record = member_versioned
        v1_id = member_versioned.id
        is_v2 = False
        v2_id = None
        if feature_flag.organization_enabled_for_e9y_2_write(
            member_versioned.organization_id
        ):
            set_tracer_tags(
                {"is_v2": True, "organization_id": member_versioned.organization_id}
            )
            member_2 = await self._get_by_dob_and_email_v2(
                date_of_birth=validated_dob, email=email
            )
            if member_2.organization_id != member_versioned.organization_id:
                raise ValueError("member_versioned and member_2 not fully synced")
            is_v2 = True
            member_record = member_2
            v2_id = member_2.id

        return self._convert_member_to_member_response(
            member_record, is_v2, v1_id, v2_id
        )

    async def _get_by_dob_and_email_v1(
        self,
        date_of_birth: datetime.date,
        email: str,
    ) -> model.MemberVersioned:
        member_list = await self.members_versioned.get_by_dob_and_email(
            date_of_birth=date_of_birth,
            email=email,
        )

        if len(member_list) == 0:
            raise errors.StandardMatchError()

        try:
            member = await check_member_org_active_and_single_org(
                configuration_client=self.configurations, member_list=member_list
            )
        except errors.MatchMultipleError as err:
            raise errors.StandardMatchMultipleError(err)
        except Exception as err:
            raise errors.StandardMatchError(err)

        return member

    async def _get_by_dob_and_email_v2(
        self,
        date_of_birth: datetime.date,
        email: str,
    ) -> model.Member2:
        member_2_list = await self.members_2.get_by_dob_and_email(
            date_of_birth=date_of_birth,
            email=email,
        )

        if len(member_2_list) == 0:
            raise errors.StandardMatchError()

        try:
            member_2 = await check_member_org_active_and_single_org(
                configuration_client=self.configurations, member_list=member_2_list
            )
        except errors.MatchMultipleError as err:
            raise errors.StandardMatchMultipleError(err)
        except Exception as err:
            raise errors.StandardMatchError(err)

        return member_2

    # endregion

    async def check_alternate_eligibility(
        self,
        *,
        date_of_birth: datetime.date | str,
        first_name: str,
        last_name: str,
        work_state: Optional[str],
        unique_corp_id: Optional[str],
    ) -> model.MemberResponse:
        """Search for a Member via 'Alternative' eligibility information.

        Args:
            date_of_birth: The date of birth to match against.
            first_name: The first name to match against. (case-insensitive)
            last_name: The last name to match against. (case-insensitive)
            work_state: The state/region of employment. (optional, case-insensitive)
            unique_corp_id: The uniquely-identifying ID provided by the client. (optional, case-insensitive)

        Returns:
            A Member record, if one is found.

        Raises:
            An AlternateMatchError, if no match is found.
        """
        dob = self._validate_date(date_of_birth)
        if unique_corp_id:
            member_list = await self.members_versioned.get_by_tertiary_verification(
                date_of_birth=dob, unique_corp_id=unique_corp_id
            )

        else:
            member_list = await self.members_versioned.get_by_secondary_verification(
                date_of_birth=dob,
                first_name=first_name,
                last_name=last_name,
                work_state=work_state,
            )

        entries = len(member_list)
        if entries == 0:
            raise errors.AlternateMatchError()

        #
        try:
            member_record = await check_member_org_active_and_single_org(
                configuration_client=self.configurations, member_list=member_list
            )
        except errors.MatchMultipleError as err:
            raise errors.AlternateMatchMultipleError(err)
        except Exception as err:
            raise errors.AlternateMatchError(err)

        member_result = member_record
        v1_id = member_result.id
        is_v2 = False
        v2_id = None
        if feature_flag.organization_enabled_for_e9y_2_write(
            member_record.organization_id
        ):
            set_tracer_tags(
                {"is_v2": True, "organization_id": member_record.organization_id}
            )
            if unique_corp_id:
                member_2_list = await self.members_2.get_by_tertiary_verification(
                    date_of_birth=dob, unique_corp_id=unique_corp_id
                )
            else:
                member_2_list = await self.members_2.get_by_secondary_verification(
                    date_of_birth=dob,
                    first_name=first_name,
                    last_name=last_name,
                    work_state=work_state,
                )
            entries = len(member_2_list)
            if entries == 0:
                logger.error(
                    "No member_2 records found for alternate eligibility",
                    date_of_birth=date_of_birth,
                    unique_corp_id=unique_corp_id,
                )
                raise errors.AlternateMatchError(
                    "No member_2 records found for alternate eligibility"
                )

            try:
                member_2_record = await check_member_org_active_and_single_org(
                    configuration_client=self.configurations, member_list=member_2_list
                )
            except errors.MatchMultipleError as err:
                logger.error(
                    "Multiple organization records found for user of v2.",
                    member_list=member_2_list,
                )
                raise errors.AlternateMatchMultipleError(err)
            except Exception as err:
                logger.error(
                    f"Exception {err} of v2.",
                    member_list=member_2_list,
                )
                raise errors.AlternateMatchError(err)

            if member_2_record.organization_id != member_record.organization_id:
                logger.error(
                    "Organization mismatch between member_versioned and member_2 records found for alternate eligibility",
                    member_record=member_record,
                    member_2_record=member_2_record,
                )
                raise ValueError("member_versioned and member_2 not fully synced")

            member_result = member_2_record
            is_v2 = True
            v2_id = member_2_record.id

        return self._convert_member_to_member_response(
            member_result, is_v2, v1_id, v2_id
        )

    async def check_overeligibility(
        self,
        *,
        date_of_birth: datetime.date | str,
        first_name: str,
        last_name: str,
        user_id: int,
        email: Optional[str],
        work_state: Optional[str],
        unique_corp_id: Optional[str],
    ) -> List[model.MemberResponse]:
        """
        Search for a Member via 'Overeligiblity' logic - this will return multiple results for
        matching demographic information, and filter it down based on rules regarding emails and unique corp ids

        Args:
            date_of_birth: The date of birth to match against.
            first_name: The first name to match against. (case-insensitive)
            last_name: The last name to match against. (case-insensitive)
            user_id: The mono identifier for a user
            email: Email a user has identified themselves with (optional, case-insensitive)
            work_state: The state/region of employment. (optional, case-insensitive)
            unique_corp_id: The uniquely-identifying ID provided by the client. (optional, case-insensitive)



        Returns:
            A list of Member records, if any are found.

        Raises:
            An OverEligibilityError, if no matches are found.
        """

        dob = self._validate_date(date_of_birth)
        member_list = await self.members_versioned.get_by_overeligibility(
            date_of_birth=dob, first_name=first_name, last_name=last_name
        )

        member_1_records = await self._aux_check_overeligibility(
            member_list, email, unique_corp_id
        )

        # Run v2 check if any orgs are enabled for e9y 2.0
        is_v2 = False
        for m in member_1_records:
            if feature_flag.organization_enabled_for_e9y_2_write(m.organization_id):
                is_v2 = True
                break

        member_2_records = []
        if is_v2:
            member_2_list = await self.members_2.get_by_overeligibility(
                date_of_birth=dob, first_name=first_name, last_name=last_name
            )
            member_2_records = await self._aux_check_overeligibility(
                member_2_list, email, unique_corp_id
            )

        member_1_dict = {m.organization_id: m for m in member_1_records}
        member_2_dict = {m.organization_id: m for m in member_2_records}
        combined_list = []
        v2_orgs = []
        for org_id in member_1_dict:
            member_1_record = member_1_dict[org_id]
            if not feature_flag.organization_enabled_for_e9y_2_write(org_id):
                combined_list.append(
                    self._convert_member_to_member_response(
                        member_1_record, False, member_1_record.id, None
                    )
                )
            else:
                v2_orgs.append(org_id)
                member_2_record = member_2_dict.get(org_id)
                if not member_2_record:
                    raise errors.OverEligibilityError(
                        errors.MatchError(
                            f"No active records of 2.0 found for user of org {org_id}."
                        )
                    )
                combined_list.append(
                    self._convert_member_to_member_response(
                        member_2_record, True, member_1_record.id, member_2_record.id
                    )
                )

        if len(v2_orgs) > 0:
            set_tracer_tags({"is_v2": True, "organization_ids": v2_orgs})

        return combined_list

    async def _aux_check_overeligibility(
        self,
        member_list: List[model.MemberVersioned] | List[model.Member2],
        email: Optional[str],
        unique_corp_id: Optional[str],
    ) -> List[model.MemberVersioned] | List[model.Member2]:
        if len(member_list) == 0:
            raise errors.OverEligibilityError()

        organization_ids = frozenset(m.organization_id for m in member_list)

        # Defensive check to ensure overeligibility feature flag is enabled.
        # If overeligibility is not enabled, return an empty list early.
        if not feature_flag.is_overeligibility_enabled():
            logger.warn(
                "Overeligibility is not enabled",
                organization_ids=organization_ids,
                member_ids={m.id for m in member_list},
            )
            raise errors.OverEligibilityError()

        # Below check is to ensure we rollout eligibility for a subset of organizations
        # We should remove this check once we roll this out for all organizations

        # Check if all organizations are enabled for overeligibility
        # If at least one org is not enabled, return an empty list
        if not feature_flag.are_all_organizations_enabled_for_overeligibility(
            organization_ids=organization_ids
        ):
            logger.warn(
                "One or more organizations not enabled for overeligibility.",
                organization_ids=organization_ids,
                member_ids={m.id for m in member_list},
            )
            raise errors.OverEligibilityError()

        # If we have received an email from the user, filter out any results that do not match that email
        sanitized_entries = [m for m in member_list]

        if email:
            for m in member_list:
                # Remove any non-matching emails - exclude non-populated/null emails
                if m.email and m.email != email:
                    sanitized_entries.remove(m)

        if sanitized_entries == []:
            # fail, return none
            raise errors.OverEligibilityError(
                errors.MatchError("No active records found for user.")
            )

        healthplan_matching_records = []
        healthplan_nonmatch_records = []

        if unique_corp_id:
            # check to see if the record is of type healthplan
            for m in sanitized_entries:
                if (
                    await cached_organization_eligibility_type(
                        organization_id=m.organization_id, configs=self.configurations
                    )
                    == "HEALTHPLAN"
                ):
                    # If so, let's try to enforce on unique_corp_id
                    if unique_corp_id == m.unique_corp_id:
                        healthplan_matching_records.append(m)
                    else:
                        healthplan_nonmatch_records.append(m)

            # If we received healthplan records and *at least one* matched our unique corp ID,
            # remove all non-matching *healthplan* records from our list of possible results
            if healthplan_matching_records != []:
                sanitized_entries = [
                    r for r in sanitized_entries if r not in healthplan_nonmatch_records
                ]

        # Last step- let's only grab the entries from active organizations
        try:
            return await check_member_org_active_and_overeligibility(
                configuration_client=self.configurations, member_list=sanitized_entries
            )

        except Exception as err:
            raise errors.OverEligibilityError(err)

    async def check_client_specific_eligibility(
        self,
        *,
        is_employee: bool,
        date_of_birth: datetime.date | str,
        dependent_date_of_birth: datetime.date | str,
        organization_id: int,
        unique_corp_id: str,
    ) -> Optional[model.MemberResponse]:
        """Search for a Member via 'Client-Specific' eligibility information.

        Args:
            is_employee: Whether the member is an employee of a client or benificiary thereof.
            date_of_birth: The employee date of birth to match against. Cannot be null
            dependent_date_of_birth: The benificiary date of birth to match against.
            organization_id: The organization to match against.
            unique_corp_id: The unique corp id to match against.
                            (case-insensitive, ignores leading zeroes (0))

        Returns:
            A Member record, if one is found.

        Raises:
            A ClientSpecificMatchError, if no match is found.
            An UpstreamClientSpecificException, if an exception is encountered when running the client check.
        """
        tracer.set_tags({"organization_id": organization_id})

        config = await self.configurations.get(organization_id)
        if config is None or config.implementation is None:
            logger.warn("This organization does not support client specific checks.")
            raise errors.ClientSpecificConfigurationError(
                f"This organization does not support a client specific eligibility check: {organization_id}"
            )

        # Ensure that the member belongs to an activated organization
        # This is done in code so we can more easily test our logic, prevent full table join that may be unnecessary (ex) if a record dne
        active_org = is_organization_activated(config)
        if not active_org:
            raise errors.ClientSpecificConfigurationError(
                f"This organization is not currently activated: {config.organization_id}"
            )

        dob, dep_dob = (
            self._validate_optional_date(date_of_birth),
            self._validate_optional_date(dependent_date_of_birth),
        )
        if (dob, dep_dob) == (None, None):
            raise ValidationError(
                "Neither 'date_of_birth' nor 'dependent_date_of_birth' provided.",
                date_of_birth=date_of_birth,
                dependent_date_of_birth=dependent_date_of_birth,
            )

        if dob is None:
            raise ValidationError(
                "Employee 'date_of_birth' must be provided.",
                is_employee=is_employee,
                date_of_birth=date_of_birth,
            )
        elif not is_employee and dep_dob is None:
            raise ValidationError(
                f"'dependent_date_of_birth' must be provided if {is_employee=}",
                is_employee=is_employee,
                dependent_date_of_birth=dependent_date_of_birth,
            )

        member = await self.client_specific.perform_client_specific_verification(
            is_employee=is_employee,
            organization_id=organization_id,
            unique_corp_id=unique_corp_id,
            implementation=config.implementation,
            date_of_birth=dob,
            dependent_date_of_birth=dep_dob,
        )
        is_v2 = feature_flag.organization_enabled_for_e9y_2_write(organization_id)
        if is_v2:
            set_tracer_tags({"is_v2": True, "organization_id": organization_id})
            v1_id = None
            v2_id = member.id
        else:
            v1_id = member.id
            v2_id = None

        return self._convert_member_to_member_response(member, is_v2, v1_id, v2_id)

    async def check_organization_specific_eligibility_without_dob(
        self,
        *,
        email: str,
        first_name: str,
        last_name: str,
    ) -> Optional[model.MemberResponse]:
        """
        This function queries the members database using the provided email, first name,
        and last name, and filters the results to include only members from organizations
        that do not require a date of birth (DOB). If eligible members are found, their
        records are returned after verifying their organization is active and unique.

        Args:
            email (str): The email address of the member.
            first_name (str): The first name of the member.
            last_name (str): The last name of the member.

        Returns:
            Optional[model.Member]: The eligible member record if found and verified;
            otherwise, raises an error.

        Raises:
            NoDobMatchError: If no eligible member records are found or if there is an
            issue with verifying the member's organization status.

        Note:
            This query is specifically applicable to organizations that do not send DOB.
            Member records for organizations that send DOB are filtered out.
        """
        member_list = await self.members_versioned.get_by_email_and_name(
            email=email,
            first_name=first_name,
            last_name=last_name,
        )

        # since this query is only applicable to organizations that dont send DOB
        # filter out member records for organizations that do send it
        member_list_filtered = []
        for member in member_list:
            if member.organization_id in ORGANIZATIONS_NOT_SENDING_DOB:
                member_list_filtered.append(member)

        if len(member_list_filtered) == 0:
            logger.info(
                "No member records found with email and name",
                method=EligibilityMethod.NO_DOB,
            )

            # log if we find records for other orgs
            # this could be due to over eligibility
            if len(member_list) > 0:
                member_ids = [member.id for member in member_list]
                logger.info(
                    "Found member records by email and name for organization sending member date of birth",
                    member_ids=member_ids,
                )
            raise errors.NoDobMatchError()

        try:
            member_record = await check_member_org_active_and_single_org(
                configuration_client=self.configurations,
                member_list=member_list_filtered,
            )
        except errors.MatchMultipleError as multi_err:
            raise multi_err
        except Exception as err:
            raise errors.NoDobMatchError(err)

        member_result = member_record
        is_v2 = False
        v1_id = member_record.id
        v2_id = None
        if feature_flag.organization_enabled_for_e9y_2_write(
            member_record.organization_id
        ):
            set_tracer_tags(
                {"is_v2": True, "organization_id": member_record.organization_id}
            )
            member_2_list = await self.members_2.get_by_email_and_name(
                email=email,
                first_name=first_name,
                last_name=last_name,
            )
            member_2_list_filtered = []
            for m in member_2_list:
                if m.organization_id == member_record.organization_id:
                    member_2_list_filtered.append(m)
            if len(member_2_list_filtered) == 0:
                raise errors.NoDobMatchError("No active records of 2.0 found for user.")
            member_2_record = member_2_list_filtered[0]
            member_result = member_2_record
            is_v2 = True
            v2_id = member_2_record.id

        return self._convert_member_to_member_response(
            member_result, is_v2, v1_id, v2_id
        )

    async def get_by_member_id(
        self,
        *,
        id: int,
    ) -> model.MemberResponse:
        """Get a member by their primary key.

        Args:
            id: An ID for an existing Member record.

        Returns:
            A MemberResponse record for v1/v2, if one is found;

        Raises:
            A GetMatchError, if no match is found.
        """
        member_versioned = await self.members_versioned.get(pk=id)
        if not member_versioned:
            raise errors.GetMatchError(f"member_versioned not found for id={id}")
        member_record = member_versioned
        is_v2 = feature_flag.organization_enabled_for_e9y_2_read(
            member_versioned.organization_id
        )
        if is_v2:
            set_tracer_tags(
                {"is_v2": True, "organization_id": member_versioned.organization_id}
            )
            member_2 = await self.members_2.get_by_member_versioned(
                member_versioned=member_versioned
            )
            # Fall back to 1.0 if 2.0 record is not found
            if not member_2:
                logger.error(
                    f"member_2 of 2.0 not found for member_versioned.id={member_versioned.id}"
                )
                is_v2 = False
            else:
                member_record = member_2

        member_1_id = member_versioned.id
        member_2_id = member_2.id if is_v2 else None

        return self._convert_member_to_member_response(
            member_record, is_v2, member_1_id, member_2_id
        )

    async def get_by_member_id_from_member(self, *, id: int) -> model.Member:
        """Get a member by their primary key from the existing member table

        Args:
            id: An ID for an existing Member record.

        Returns:
            A Member record, if one is found.

        Raises:
            A GetMatchError, if no match is found.
        """

        member = await self.members.get(pk=id)

        if not member:
            raise errors.GetMatchError()
        return member

    # region get_by_org_identity
    async def get_by_org_identity(
        self,
        *,
        organization_id: int,
        unique_corp_id: str,
        dependent_id: str,
    ) -> model.MemberResponse:
        """Search for a member via an 'Org-Identity'

        An 'Org-Identity' is a unique, composite key based on the Organization's ID and
        the Member record's `unique_corp_id` & `dependent_id`.

        Args:
            organization_id: The organization to match against.
            unique_corp_id: The unique corp id to match against.
                            (case-insensitive, ignores leading zeroes (0))
            dependent_id: The dependent id to match against.

        Returns:
            A MemberResponse record for v1/v2, if one is found;

        Raises:
            An IdentityMatchError, if no match is found.
        """
        identity = self._validate_identity(
            organization_id=organization_id,
            unique_corp_id=unique_corp_id,
            dependent_id=dependent_id,
        )

        member_versioned = await self._get_by_org_identity_v1(identity=identity)

        is_v2 = feature_flag.organization_enabled_for_e9y_2_read(
            identity.organization_id
        )
        if is_v2:
            set_tracer_tags(
                {"is_v2": True, "organization_id": identity.organization_id}
            )
            member_2 = await self._get_by_org_identity_v2(identity=identity)
            if not member_2:
                logger.error(f"member_2 of 2.0 not found for identity={identity}")
                is_v2 = False

        member_record = member_2 if is_v2 else member_versioned
        member_1_id = member_versioned.id
        member_2_id = member_2.id if is_v2 else None

        return self._convert_member_to_member_response(
            member_record, is_v2, member_1_id, member_2_id
        )

    async def _get_by_org_identity_v1(
        self,
        *,
        identity: model.OrgIdentity,
    ) -> model.MemberVersioned:
        member = await self.members_versioned.get_by_org_identity(identity=identity)

        if not member:
            raise errors.IdentityMatchError()

        # Ensure that the member belongs to an activated organization
        active_org = await is_cached_organization_active(
            member.organization_id, self.configurations
        )
        if not active_org:
            raise errors.IdentityMatchError()

        return member

    async def _get_by_org_identity_v2(
        self,
        *,
        identity: model.OrgIdentity,
    ) -> model.Member2:
        if not await self._check_organization_active_status(identity.organization_id):
            raise errors.IdentityMatchError("Organization not active of v2")
        member2 = await self.members_2.get_by_org_identity(identity=identity)
        if not member2:
            logger.error(f"Matching member of v2 not found for identity={identity}")
        return member2

    # endregion

    async def get_wallet_enablement_by_user_id(
        self, *, user_id: int
    ) -> model.WalletEnablementResponse:
        """
        Search for a wallet enablement configuration for the provided user ID.

        A "wallet enablement" is a set of configuration values provided to us at the
        member-level by an organization which allows or dis-allows a user to access our
        Maven Wallet product.

        Args:
            user ID: An ID for a Maven user
        Returns:
            A WalletEnablement, if one is found.
        Raises:
            A GetMatchError, if no match is found.

        """
        verification_record = await self.verifications.get_verification_key_for_user(
            user_id=user_id
        )
        if not verification_record:
            logger.info(
                "Unable to find a corresponding e9y member ID for user", user_id=user_id
            )
            raise errors.GetMatchError(
                f"Wallet enablement not found for user_id: {user_id}."
            )
        is_v2 = feature_flag.organization_enabled_for_e9y_2_read(
            verification_record.organization_id
        )
        member_1_id = verification_record.member_id
        member_2_id = verification_record.member_2_id if is_v2 else None

        enablement = None
        if is_v2:
            set_tracer_tags(
                {"is_v2": True, "organization_id": verification_record.organization_id}
            )
            enablement = await self.members_2.get_wallet_enablement(
                member_id=verification_record.member_2_id
            )
            if not enablement:
                logger.warning(
                    "Wallet enablement not found for member_id of v2.",
                    member_id=verification_record.member_id,
                )
        # Either v2 is not enabled, or we cannot find a v2 record, fallback to v1
        if not enablement:
            enablement = await self.members_versioned.get_wallet_enablement(
                member_id=verification_record.member_id
            )
            is_v2 = False
            member_2_id = None

        if not enablement:
            raise errors.GetMatchError(
                f"Wallet enablement not found for member_id: {verification_record.member_id}."
            )
        return self._convert_enablement_to_enablement_response(
            enablement, is_v2, member_1_id, member_2_id
        )

    # TODO: Remove when we cutover to member_versioned
    async def get_wallet_enablement(
        self, *, member_id: int
    ) -> model.WalletEnablementResponse:
        """Search for a wallet enablement configuration for the provided Member ID.

        A "wallet enablement" is a set of configuration values provided to us at the
        member-level by an organization which allows or dis-allows a user to access our
        Maven Wallet product.

        Args:
            member_id: An ID for an existing Member in our database.

        Returns:
            A WalletEnablement, if one is found.

        Raises:
            A GetMatchError, if no match is found.
        """
        member_record = await self.get_by_member_id(id=member_id)
        is_v2 = member_1_id = member_2_id = None
        member_1_id = member_id

        if feature_flag.organization_enabled_for_e9y_2_read(
            member_record.organization_id
        ):
            set_tracer_tags(
                {"is_v2": True, "organization_id": member_record.organization_id}
            )
            logger.debug(
                "Call get_wallet_enablement by member id v2",
                member_id=member_id,
            )
            enablement = await self.members_2.get_wallet_enablement(
                member_id=member_record.member_2_id
            )
            is_v2 = True
            member_2_id = member_record.member_2_id
        else:
            enablement = await self.members_versioned.get_wallet_enablement(
                member_id=member_id
            )
            is_v2 = False
            member_1_id = member_id
        if not enablement:
            raise errors.GetMatchError(
                f"Wallet enablement not found for member_id: {member_id=}."
            )

        return self._convert_enablement_to_enablement_response(
            enablement, is_v2, member_1_id, member_2_id
        )

    async def get_wallet_enablement_by_identity(
        self,
        *,
        organization_id: int,
        unique_corp_id: str,
        dependent_id: str,
    ) -> model.WalletEnablementResponse:
        """Search for a wallet enablement using an org identity.

        An 'Org-Identity' is a unique, composite key based on the Organization's ID and
        the Member record's `unique_corp_id` & `dependent_id`.

        A "wallet enablement" is a set of configuration values provided to us at the
        member-level by an organization which allows or dis-allows a user to access our
        Maven Wallet product.

        Args:
            organization_id: The ID of the organization this record should belong to.
            unique_corp_id: The unique_corp_id of the Member record.
            dependent_id: The dependent_id of the Member record.

        Returns:
            A WalletEnablement, if one is found.

        Raises:
            An IdentityMatchError, if no match is found.
        """
        identity = self._validate_identity(
            organization_id=organization_id,
            unique_corp_id=unique_corp_id,
            dependent_id=dependent_id,
        )
        is_v2 = member_1_id = member_2_id = None
        is_v2 = feature_flag.organization_enabled_for_e9y_2_read(organization_id)

        enablement = await self.members_versioned.get_wallet_enablement_by_identity(
            identity=identity
        )
        if not enablement:
            raise errors.IdentityMatchError(
                f"Wallet enablement 1.0 not found for identity={identity}."
            )
        member_1_id = enablement.member_id
        if is_v2:
            set_tracer_tags({"is_v2": True, "organization_id": organization_id})
            logger.debug(
                "Call get_wallet_enablement_by_identity v2",
                organization_id=organization_id,
                unique_corp_id=unique_corp_id,
                dependent_id=dependent_id,
            )
            enablement = await self.members_2.get_wallet_enablement_by_identity(
                identity=identity
            )
            if not enablement:
                raise errors.IdentityMatchError(
                    f"Wallet enablement 2.0 not found for identity={identity}."
                )
            member_2_id = enablement.member_id
        return self._convert_enablement_to_enablement_response(
            enablement, is_v2, member_1_id, member_2_id
        )

    async def get_verification_for_user(
        self,
        *,
        user_id: int,
        active_verifications_only: bool = False,
        organization_id: int | None = None,
    ) -> model.EligibilityVerificationForUser:
        """Fetch eligibility verification record for a user by user_id.

        "EligibilityVerificationForUser" record contains member and eligibility verification
        attributes associated with the user

        Args:
            user_id: The ID of the user requesting.
            active_verifications_only: If True, only eligibility verification records
            are returned.
            organization_id: The ID of the organization this record should belong to.

        Returns:
            A EligibilityVerificationForUser, if one is found.

        Raises:
            A GetMatchError, if no match is found.
        """

        if organization_id and feature_flag.organization_enabled_for_e9y_2_read(
            organization_id
        ):
            set_tracer_tags({"is_v2": True, "organization_id": organization_id})

        eligibility_verification_record = (
            await self.verifications.get_eligibility_verification_record_for_user(
                user_id=user_id
            )
        )

        # Check for orgs and active e9y records:
        if eligibility_verification_record:
            # Ensure our verification record belongs to the right org
            if organization_id:
                if eligibility_verification_record.organization_id != organization_id:
                    logger.info(
                        "Member Eligibility Verification record not found for organization/user combination",
                        user_id=user_id,
                        organization_id=organization_id,
                        active_verifications_only=active_verifications_only,
                    )
                    raise errors.GetMatchError(
                        "Member Eligibility Verification record not found for organization/user combination"
                    )

            # Ensure our verification has an active e9y record, if the e9y record has an effective range
            if (
                active_verifications_only
                and eligibility_verification_record.effective_range
                and not is_verification_record_active(
                    record=eligibility_verification_record
                )
            ):
                logger.info(
                    "No Member Eligibility Verification record with valid eligibility record found for user",
                    user_id=user_id,
                    organization_id=organization_id,
                    active_verifications_only=active_verifications_only,
                )
                raise errors.GetMatchError(
                    "No Member Eligibility Verification record with valid eligibility found for user"
                )

        if not eligibility_verification_record:
            logger.info(
                "Member Eligibility Verification record not found for user",
                user_id=user_id,
                organization_id=organization_id,
                active_verifications_only=active_verifications_only,
            )
            raise errors.GetMatchError(
                "Member Eligibility Verification record not found for user"
            )

        return eligibility_verification_record

    async def get_all_verifications_for_user(
        self,
        *,
        user_id: int,
        active_verifications_only: bool = False,
        organization_ids: Optional[List[int]] = None,
    ) -> List[model.EligibilityVerificationForUser]:
        """Fetch all eligibility verification records for a user by user_id.
        Check if over-e9y flag is set and if so return all verification records for all orgs
        else fallback to return only one record

        "EligibilityVerificationForUser" record contains member and eligibility verification
        attributes associated with the user

        Args:
            user_id: The ID of the user requesting.
            active_verifications_only: Whether to only return eligibility verification records
            organization_ids: The IDs of the organizations this record should belong to.

        Returns:
            A list of EligibilityVerificationForUser records, if they exist.

        Raises:
            A GetMatchError, if no match is found.
        """
        v2_orgs = []
        if organization_ids:
            v2_orgs = [
                org_id
                for org_id in organization_ids or []
                if feature_flag.organization_enabled_for_e9y_2_read(org_id)
            ]

        if v2_orgs:
            set_tracer_tags({"is_v2": True, "organization_ids": v2_orgs})

        # Fetch eligibility verification records
        eligibility_verification_records = (
            await self.verifications.get_all_eligibility_verification_records_for_user(
                user_id=user_id
            )
        )

        if not eligibility_verification_records:
            logger.info(
                "No Member Eligibility Verification record with valid eligibility record found for user",
                user_id=user_id,
                organization_ids=set(organization_ids) if organization_ids else [],
                active_verifications_only=active_verifications_only,
            )
            raise errors.GetMatchError(
                "No Member Eligibility Verification records found for user"
            )

        # Apply organization filter if provided
        unique_organization_ids = {}
        if organization_ids:
            unique_organization_ids = set(organization_ids)
            eligibility_verification_records = [
                record
                for record in eligibility_verification_records
                if record.organization_id in unique_organization_ids
            ]

        # Apply active status filter if requested
        if active_verifications_only:
            eligibility_verification_records = [
                record
                for record in eligibility_verification_records
                if record.eligibility_member_id is None
                or is_verification_record_active(record)
            ]

        # Raise error if no records match after filtering
        if not eligibility_verification_records:
            logger.info(
                "No Member Eligibility Verification records found after filtering",
                user_id=user_id,
                organization_ids=organization_ids,
                active_verifications_only=active_verifications_only,
            )
            raise errors.GetMatchError(
                "No matching Member Eligibility Verification records found for user"
            )

        if len(eligibility_verification_records) > 1:
            logger.info(
                "Multiple eligibility verification records found for user",
                user_id=user_id,
                organization_ids=unique_organization_ids,
                active_verifications_only=active_verifications_only,
                num_active_verifications=len(eligibility_verification_records),
            )

        return eligibility_verification_records

    async def create_failed_verification(
        self,
        *,
        verification_type: str,
        unique_corp_id: str = "",
        dependent_id: str = "",
        first_name: str = "",
        last_name: str = "",
        email: str = "",
        work_state: str = "",
        eligibility_member_id: int | None = None,
        organization_id: int | None = None,
        policy_used: str | None = None,
        user_id: int | None = None,
        date_of_birth: datetime.date | str | None = None,
        verified_at: datetime.date | str | None = None,
        additional_fields: dict | None = {},
    ) -> model.VerificationAttemptResponse:
        """
        Create verification attempt for a user- representing a failed verification attempt

        Returns:
            VerificationAttempt

        Raises:
            A EligibilityVerificationConfigurationError, if a verification attempt is not created
        """
        if feature_flag.is_write_disabled():
            raise errors.CreateVerificationError(
                "Creation is disabled due to feature flag"
            )

        user_id = self._validate_user_id(user_id)
        date_of_birth = self._validate_optional_date(date_of_birth)
        verified_at = self._validate_optional_date(verified_at)
        verification_type = self._validate_verification_type(verification_type)
        is_v2 = False
        verification_attempt_2 = None

        # TODO(ELIG-2259): persist to audit store for v2
        # organization_id may not be present
        if organization_id and feature_flag.organization_enabled_for_e9y_2_write(
            organization_id
        ):
            set_tracer_tags({"is_v2": True, "organization_id": organization_id})
            logger.info(
                "Created failed verification attempt for v2",
                organization_id=organization_id,
                verification_type=verification_type,
                user_id=user_id,
            )
            is_v2 = True
            current_dt = datetime.datetime.now(datetime.timezone.utc)
            verification_attempt_2 = model.VerificationAttempt(
                user_id=user_id,
                verification_type=verification_type,
                date_of_birth=date_of_birth,
                organization_id=organization_id,
                unique_corp_id=unique_corp_id,
                dependent_id=dependent_id,
                first_name=first_name,
                last_name=last_name,
                email=email,
                work_state=work_state,
                policy_used="",
                verified_at=verified_at,
                additional_fields=additional_fields,
                created_at=current_dt,
                updated_at=current_dt,
            )

        # Create a record of the verification attempt
        try:
            verification_attempt = await self.verifications.create_verification_attempt(
                user_id=user_id,
                verification_type=verification_type,
                date_of_birth=date_of_birth,
                organization_id=organization_id,
                unique_corp_id=unique_corp_id,
                dependent_id=dependent_id,
                first_name=first_name,
                last_name=last_name,
                email=email,
                work_state=work_state,
                # TODO - add this later when we begin logging which policy is used to create a verification
                policy_used="",
                verified_at=verified_at,
                additional_fields=additional_fields,
            )
        except Exception as e:
            logger.exception(
                "Error persisting failed verification attempt",
                error=e,
                organization_id=organization_id,
                unique_corp_id=unique_corp_id,
                eligibility_member_id=eligibility_member_id,
            )
            raise errors.CreateVerificationError(
                "Error persisting failed verification attempt"
            )

        # Tie our e9y member record to the verification attempt we just created
        if eligibility_member_id:
            try:
                await self.verifications.create_member_verification(
                    member_id=eligibility_member_id,
                    verification_attempt_id=verification_attempt.id,
                )
            except Exception as e:
                logger.exception(
                    "Error persisting member_verification record",
                    error=e,
                    eligibility_member_id=eligibility_member_id,
                    verification_attempt_id=verification_attempt.id,
                )
                raise errors.CreateVerificationError(
                    "Error persisting member_verification record"
                )

        logger.info(
            "Created failed verification attempt",
            organization_id=organization_id,
            verification_type=verification_type,
            user_id=user_id,
        )
        return model.VerificationAttemptResponse(
            id=verification_attempt.id,
            user_id=user_id,
            verification_type=verification_type,
            date_of_birth=date_of_birth,
            organization_id=organization_id,
            unique_corp_id=unique_corp_id,
            dependent_id=dependent_id,
            first_name=first_name,
            last_name=last_name,
            email=email,
            work_state=work_state,
            policy_used="",
            successful_verification=verification_attempt.successful_verification,
            verified_at=verification_attempt.verified_at,
            additional_fields=additional_fields,
            created_at=verification_attempt.created_at,
            updated_at=verification_attempt.updated_at,
            is_v2=is_v2,
            verification_attempt_1_id=verification_attempt.id,
            verification_attempt_2_id=verification_attempt_2.id if is_v2 else None,
            eligibility_member_id=eligibility_member_id,
            eligibility_member_2_id=None,
        )

    @tracer.wrap()
    async def create_verification_for_user(
        self,
        *,
        organization_id: int,
        verification_type: str,
        unique_corp_id: str,
        dependent_id: str = "",
        first_name: str = "",
        last_name: str = "",
        email: str = "",
        work_state: str = "",
        user_id: int = None,
        eligibility_member_id: int = None,
        verified_at: datetime.datetime | str = None,
        date_of_birth: datetime.date | str = None,
        additional_fields: dict | None = {},
        verification_session: str | None = None,
    ):
        """
        Create eligibility verification record for the user

        "EligibilityVerificationForUser" record contains member and eligibility verification
        attributes associated with the user

        Returns:
            EligibilityVerificationForUser record that was created for the user

        Raises:
            A EligibilityVerificationConfigurationError, if a verification record is not created
        """
        if feature_flag.is_write_disabled():
            raise errors.CreateVerificationError(
                "Creation is disabled due to feature flag"
            )

        user_id = self._validate_user_id(user_id)
        date_of_birth = self._validate_optional_date(date_of_birth)
        verified_at = self._validate_optional_timestamp(verified_at)
        verification_type = self._validate_verification_type(verification_type)

        if not verification_session or verification_session == "":
            verification_session = None
        else:
            verification_session = uuid.UUID(verification_session)

        if feature_flag.organization_enabled_for_e9y_2_write(organization_id):
            set_tracer_tags({"is_v2": True, "organization_id": organization_id})
            await self._create_verification_for_user_v2(
                organization_id=organization_id,
                verification_type=verification_type,
                unique_corp_id=unique_corp_id,
                dependent_id=dependent_id,
                first_name=first_name,
                last_name=last_name,
                email=email,
                work_state=work_state,
                user_id=user_id,
                eligibility_member_id=eligibility_member_id,
                verified_at=verified_at,
                date_of_birth=date_of_birth,
                additional_fields=additional_fields,
                verification_session=verification_session,
            )
        else:
            await self._create_verification_for_user_v1(
                organization_id=organization_id,
                verification_type=verification_type,
                unique_corp_id=unique_corp_id,
                dependent_id=dependent_id,
                first_name=first_name,
                last_name=last_name,
                email=email,
                work_state=work_state,
                user_id=user_id,
                eligibility_member_id=eligibility_member_id,
                verified_at=verified_at,
                date_of_birth=date_of_birth,
                additional_fields=additional_fields,
                verification_session=verification_session,
            )

        # hydrate record from db and return response
        return await self.get_verification_for_user(user_id=user_id)

    async def _create_verification_for_user_v1(
        self,
        *,
        organization_id: int,
        verification_type: str,
        unique_corp_id: str,
        dependent_id: str,
        first_name: str,
        last_name: str,
        email: str,
        work_state: str,
        user_id: int,
        eligibility_member_id: int = None,
        verified_at: datetime.datetime | None,
        date_of_birth: datetime.date | None,
        additional_fields: dict | None,
        verification_session: uuid.UUID | None,
        verification_2_id: int = None,
    ):
        # Check to see if the e9y ID is already in use- if so, can it be reused for dependents?
        if eligibility_member_id:
            usable = await self.verify_eligibility_record_usable(
                eligibility_member_id=eligibility_member_id,
                organization_id=organization_id,
            )
            if not usable:
                logger.error(
                    error="Error persisting verification record- e9y record already claimed",
                    user_id=user_id,
                    organization_id=organization_id,
                    unique_corp_id=unique_corp_id,
                    eligibility_member_id=eligibility_member_id,
                )
                raise errors.RecordAlreadyClaimedError(
                    "Error persisting verification record- e9y record already claimed"
                )
        # First log our successful verification record
        try:
            verification = await self.verifications.create_verification(
                user_id=user_id,
                verification_type=verification_type,
                organization_id=organization_id,
                unique_corp_id=unique_corp_id,
                dependent_id=dependent_id,
                first_name=first_name,
                last_name=last_name,
                email=email,
                work_state=work_state,
                date_of_birth=date_of_birth,
                verified_at=verified_at,
                additional_fields=additional_fields,
                verification_session=verification_session,
                verification_2_id=verification_2_id,
            )
        except Exception as e:
            logger.exception(
                "Error persisting verification record",
                error=e,
                user_id=user_id,
                organization_id=organization_id,
                unique_corp_id=unique_corp_id,
                eligibility_member_id=eligibility_member_id,
                verification_session=str(verification_session),
            )
            raise errors.CreateVerificationError("Error persisting verification record")

        # Then create a record of the verification attempt
        try:
            verification_attempt = await self.verifications.create_verification_attempt(
                verification_type=verification_type,
                date_of_birth=date_of_birth,
                organization_id=organization_id,
                unique_corp_id=unique_corp_id,
                dependent_id=dependent_id,
                first_name=first_name,
                last_name=last_name,
                email=email,
                work_state=work_state,
                # TODO - add this later when we begin logging which policy is used to create a verification
                policy_used="",
                verification_id=verification.id,
                verified_at=verified_at,
                additional_fields=additional_fields,
                user_id=user_id,
            )
        except Exception as e:
            logger.exception(
                "Error persisting verification attempt",
                error=e,
                user_id=user_id,
                organization_id=organization_id,
                unique_corp_id=unique_corp_id,
                eligibility_member_id=eligibility_member_id,
                verification_id=verification.id,
            )
            raise errors.CreateVerificationError(
                "Error persisting verification attempt"
            )

        # Finally, tie our e9y member record to the verification we just created
        if eligibility_member_id:
            try:
                await self.verifications.create_member_verification(
                    member_id=eligibility_member_id,
                    verification_id=verification.id,
                    verification_attempt_id=verification_attempt.id,
                )
            except Exception as e:
                logger.exception(
                    "Error persisting member_verification record",
                    error=e,
                    eligibility_member_id=eligibility_member_id,
                    verification_id=verification.id,
                    verification_attempt_id=verification_attempt.id,
                )
        logger.info(
            "Created verification",
            organization_id=organization_id,
            verification_type=verification_type,
            user_id=user_id,
        )

    @staticmethod
    def can_create_verification_2(
        existing_verification_2: model.Verification2 | Exception,
        configuration: model.Configuration,
        member_2: model.Member2,
    ):
        """
        check if we can create verification_2 or not:
        verification not exists, return True
        otherwise:
        1. if organization configured as `employee_only`, return False
        2. if organization configured as `medical_plan_only` and
            eligibility member has beneficiaries_enabled disabled, return False
        all other case:
        return True
        """
        if not isinstance(existing_verification_2, model.Verification2):
            return True

        if configuration.employee_only:
            return False

        beneficiaries_enabled = member_2.record.get("beneficiaries_enabled", False)
        if configuration.medical_plan_only and not beneficiaries_enabled:
            return False
        return True

    async def _create_verification_for_user_v2(
        self,
        *,
        organization_id: int,
        verification_type: str,
        unique_corp_id: str,
        dependent_id: str,
        first_name: str,
        last_name: str,
        email: str,
        work_state: str,
        user_id: int,
        date_of_birth: datetime.date | None,
        additional_fields: dict | None,
        verification_session: uuid.UUID | None,
        verified_at: datetime.datetime | None = None,
        eligibility_member_id: int = None,
    ):
        # in case of `eligibility_member_id` passed in,
        # check if existing
        if eligibility_member_id:
            member_record = await self.get_by_member_id(id=eligibility_member_id)
            if not member_record:
                raise errors.CreateVerificationError(
                    f"Member not found for eligibility_member_id={eligibility_member_id} of v2"
                )
            verification_2, configuration = await asyncio.gather(
                self.verifications.get_verification_2_for_member(
                    member_id=member_record.member_2_id
                ),
                self.configurations.get(organization_id),
                return_exceptions=True,
            )
            if isinstance(verification_2, Exception):
                logger.error(
                    "Failed to get existing verification_2 of v2",
                    member_id=eligibility_member_id,
                    details=verification_2,
                )

            if isinstance(configuration, Exception):
                logger.error(
                    "Failed to get configuration of v2",
                    organization_id=organization_id,
                    details=configuration,
                )

            configuration_found = isinstance(configuration, model.Configuration)
            if not configuration_found:
                raise errors.CreateVerificationError(
                    f"Configuration not found for organization_id={organization_id} of v2"
                )

            if not EligibilityService.can_create_verification_2(
                verification_2, configuration, member_record
            ):
                raise errors.CreateVerificationError(
                    f"Failed can_create_verification_2 check for eligibility_member_id={eligibility_member_id} of v2"
                )

            member_1_id = member_record.member_1_id

            usable = await self.verify_eligibility_record_usable(
                eligibility_member_id=member_1_id,
                organization_id=organization_id,
            )
            if not usable:
                logger.exception(
                    error="Error persisting verification record- e9y record already claimed of v2",
                    user_id=user_id,
                    organization_id=organization_id,
                    unique_corp_id=unique_corp_id,
                    eligibility_member_id=member_1_id,
                )
                raise errors.CreateVerificationError(
                    "Error persisting verification record- e9y record already claimed of v2"
                )

            await self.verifications.create_verification_dual_write(
                user_id=user_id,
                verification_type=verification_type,
                organization_id=organization_id,
                unique_corp_id=unique_corp_id,
                dependent_id=dependent_id,
                first_name=first_name,
                last_name=last_name,
                email=email,
                work_state=work_state,
                date_of_birth=date_of_birth,
                additional_fields=additional_fields,
                verification_session=verification_session,
                verified_at=verified_at,
                deactivated_at=None,
                eligibility_member_1_id=member_record.member_1_id,
                eligibility_member_2_id=member_record.member_2_id,
                eligibility_member_2_version=member_record.version,
            )
        else:
            await self.verifications.create_verification_dual_write(
                user_id=user_id,
                verification_type=verification_type,
                organization_id=organization_id,
                unique_corp_id=unique_corp_id,
                dependent_id=dependent_id,
                first_name=first_name,
                last_name=last_name,
                email=email,
                work_state=work_state,
                date_of_birth=date_of_birth,
                additional_fields=additional_fields,
                verification_session=verification_session,
                verified_at=verified_at,
                deactivated_at=None,
                eligibility_member_1_id=None,
                eligibility_member_2_id=None,
                eligibility_member_2_version=None,
            )

    @tracer.wrap()
    async def create_multiple_verifications_for_user(
        self,
        *,
        verification_data_list: List[model.VerificationData],
        verification_type: str,
        first_name: str = "",
        last_name: str = "",
        user_id: int = None,
        verified_at: Union[datetime.datetime, str] = None,
        date_of_birth: Union[datetime.date, str, None] = None,
        verification_session: Optional[str] = None,
    ) -> List[model.EligibilityVerificationForUser]:
        if feature_flag.is_write_disabled():
            raise errors.CreateVerificationError(
                "Creation is disabled due to feature flag"
            )

        # Validate inputs
        user_id = self._validate_user_id(user_id)
        date_of_birth = self._validate_optional_date(date_of_birth)
        verified_at = self._validate_optional_timestamp(verified_at)
        verification_type = self._validate_verification_type(verification_type)

        organization_ids = {data.organization_id for data in verification_data_list}
        eligibility_member_ids = {
            data.eligibility_member_id
            for data in verification_data_list
            if data.eligibility_member_id not in (None, 0)
        }
        unique_corp_ids = {data.unique_corp_id for data in verification_data_list}

        verification_session = self._parse_verification_session(verification_session)

        # Filter verifications that are not already claimed
        verification_data_not_already_claimed = (
            await self._filter_unclaimed_verifications(verification_data_list, user_id)
        )

        logger.info(
            "Attempting to create multiple verifications for user",
            user_id=user_id,
            organization_ids=organization_ids,
            eligibility_member_ids=eligibility_member_ids,
            unique_corp_ids=unique_corp_ids,
            verification_session=verification_session,
            verification_data_not_already_claimed=verification_data_not_already_claimed,
        )

        # If all records are already claimed, raise an error
        if not verification_data_not_already_claimed:
            logger.error(
                "Error persisting verification records - e9y records already claimed",
                user_id=user_id,
                organization_ids=organization_ids,
                unique_corp_ids=unique_corp_ids,
                eligibility_member_ids=eligibility_member_ids,
            )
            raise errors.RecordAlreadyClaimedError(
                "Error persisting verification records - e9y records already claimed"
            )

        verification_data_1_not_already_claimed = []
        verification_data_2_not_already_claimed = []

        for verification_data in verification_data_not_already_claimed:
            if feature_flag.organization_enabled_for_e9y_2_write(
                verification_data.organization_id
            ):
                verification_data_2_not_already_claimed.append(verification_data)
            else:
                verification_data_1_not_already_claimed.append(verification_data)

        # Create verifications of 1.0
        await self.create_multiple_verifications_v1(
            verification_data_1_not_already_claimed,
            verification_type,
            first_name,
            last_name,
            user_id,
            verified_at,
            date_of_birth,
            verification_session,
        )

        # Create verifications of 2.0
        await self.prepare_and_create_multiple_verifications_v2(
            verification_data_2_not_already_claimed,
            verification_type,
            first_name,
            last_name,
            user_id,
            verified_at,
            date_of_birth,
            verification_session,
        )

        # Return all verifications for the user
        return await self.get_all_verifications_for_user(user_id=user_id)

    @tracer.wrap()
    async def create_multiple_verifications_v1(
        self,
        verification_data_1_not_already_claimed: List[model.VerificationData],
        verification_type: str,
        first_name: str = "",
        last_name: str = "",
        user_id: int = None,
        verified_at: Union[datetime.datetime, str] = None,
        date_of_birth: Union[datetime.date, str, None] = None,
        verification_session: Optional[str] = None,
    ):
        if feature_flag.is_write_disabled():
            raise errors.CreateVerificationError(
                "Creation is disabled due to feature flag"
            )

        # Create verifications of 1.0
        if not verification_data_1_not_already_claimed:
            return
        async with self.verifications._verification_client.client.connector.transaction() as c:
            verification_organization_map: Dict[
                int, model.Verification
            ] = await self._create_verifications(
                user_id,
                verification_type,
                verification_data_1_not_already_claimed,
                first_name,
                last_name,
                date_of_birth,
                verified_at,
                verification_session,
                connection=c,
            )

            # Map verifications back to verification_data_list
            for data in verification_data_1_not_already_claimed:
                verification = verification_organization_map.get(data.organization_id)
                if verification is not None:
                    data.verification_id = verification.id

            # Create verification attempts
            verification_attempt_map: Dict[
                int, model.VerificationAttempt
            ] = await self._create_verification_attempts(
                verification_data_1_not_already_claimed,
                user_id,
                verification_type,
                first_name,
                last_name,
                date_of_birth,
                verified_at,
                connection=c,
            )

            # Map verification attempts back to verification_data_list
            for data in verification_data_1_not_already_claimed:
                attempt = verification_attempt_map.get(data.organization_id)
                if attempt is not None:
                    data.verification_attempt_id = attempt.id

            # Tie eligibility member record to verifications
            await self._create_member_verifications(
                verification_data_1_not_already_claimed, connection=c
            )

            logger.info(
                "Created multiple verifications of 1.0",
                verification_type=verification_type,
                user_id=user_id,
            )

    @tracer.wrap()
    async def prepare_and_create_multiple_verifications_v2(
        self,
        verification_data_2_not_already_claimed: List[model.VerificationData],
        verification_type: str,
        first_name: str = "",
        last_name: str = "",
        user_id: int = None,
        verified_at: Union[datetime.datetime, str] = None,
        date_of_birth: Union[datetime.date, str, None] = None,
        verification_session: Optional[str] = None,
    ):
        # Create verifications of 2.0
        if not verification_data_2_not_already_claimed:
            return

        v2_orgs = []

        for verification_data in verification_data_2_not_already_claimed:
            v2_orgs.append(verification_data.organization_id)
            if verification_data.eligibility_member_id in (None, 0):
                verification_data.member_2_id = None
                verification_data.member_2_version = None
                verification_data.member_1_id = None
            else:
                member_record = await self.get_by_member_id(
                    id=verification_data.eligibility_member_id
                )
                if not member_record:
                    raise errors.CreateVerificationError(
                        f"Member not found for eligibility_member_id={verification_data.eligibility_member_id} of v2"
                    )
                verification_2, configuration = await asyncio.gather(
                    self.verifications.get_verification_2_for_member(
                        member_id=member_record.member_2_id,
                    ),
                    self.configurations.get(verification_data.organization_id),
                    return_exceptions=True,
                )
                configuration_found = isinstance(configuration, model.Configuration)
                if not configuration_found:
                    raise errors.CreateVerificationError(
                        f"Configuration not found for organization_id={verification_data.organization_id} of v2"
                    )

                if not EligibilityService.can_create_verification_2(
                    verification_2, configuration, member_record
                ):
                    raise errors.CreateVerificationError(
                        f"Failed can_create_verification_2 check for eligibility_member_id={verification_data.eligibility_member_id} of v2"
                    )

                verification_data.member_2_id = member_record.member_2_id
                verification_data.member_2_version = member_record.version
                verification_data.member_1_id = member_record.member_1_id

        set_tracer_tags({"is_v2": True, "organization_ids": v2_orgs})

        await self.verifications.create_multiple_verification_dual_write(
            user_id=user_id,
            verification_type=verification_type,
            verification_data_list=verification_data_2_not_already_claimed,
            first_name=first_name,
            last_name=last_name,
            date_of_birth=date_of_birth,
            verified_at=verified_at,
            verification_session=verification_session,
        )
        logger.info(
            "Created multipleverifications of 2.0",
            verification_type=verification_type,
            user_id=user_id,
        )

    async def _filter_unclaimed_verifications(
        self,
        verification_data_list: List[model.VerificationData],
        user_id: int,
    ) -> List[model.VerificationData]:
        """
        Filter out verifications that have already been claimed.

        :param verification_data_list: List of verification data objects
        :param user_id: User ID
        :return: List of unclaimed verification data objects
        """
        unclaimed_verifications = []
        for verification_data in verification_data_list:
            eligibility_member_id = verification_data.eligibility_member_id
            organization_id = verification_data.organization_id
            if eligibility_member_id:
                if await self.verify_eligibility_record_usable(
                    eligibility_member_id, organization_id
                ):
                    unclaimed_verifications.append(verification_data)
                else:
                    logger.warning(
                        "Skipping creation of verification record - e9y record already claimed",
                        user_id=user_id,
                        organization_id=organization_id,
                        unique_corp_id=verification_data.unique_corp_id,
                        eligibility_member_id=eligibility_member_id,
                    )
            else:
                unclaimed_verifications.append(verification_data)
        return unclaimed_verifications

    @staticmethod
    def _parse_verification_session(verification_session):
        """Parse the verification session ID."""
        if not verification_session or verification_session == "":
            return None
        return uuid.UUID(verification_session)

    async def _create_verifications(
        self,
        user_id: int,
        verification_type: str,
        verification_data_list: List[model.VerificationData],
        first_name: str,
        last_name: str,
        date_of_birth: Optional[datetime.date],
        verified_at: Optional[datetime.datetime],
        verification_session: Optional[uuid.UUID],
        connection: asyncpg.Connection = None,
    ) -> Dict[int, model.Verification]:
        """Create multiple verification records and return a dictionary with organization_id as keys."""
        try:
            verifications = await self.verifications.create_multiple_verifications(
                user_id=user_id,
                verification_type=verification_type,
                verification_data_list=verification_data_list,
                first_name=first_name,
                last_name=last_name,
                date_of_birth=date_of_birth,
                verified_at=verified_at,
                verification_session=verification_session,
                connection=connection,
            )
            return {
                verification.organization_id: verification
                for verification in verifications
            }
        except Exception as e:
            organization_ids = [data.organization_id for data in verification_data_list]
            unique_corp_ids = [data.unique_corp_id for data in verification_data_list]
            eligibility_member_ids = [
                data.eligibility_member_id for data in verification_data_list
            ]

            logger.exception(
                "Error persisting verification records",
                error=e,
                user_id=user_id,
                organization_ids=organization_ids,
                unique_corp_ids=unique_corp_ids,
                eligibility_member_ids=eligibility_member_ids,
                verification_session=str(verification_session),
            )
            raise errors.CreateVerificationError(
                "Error persisting verification records"
            )

    async def _create_verification_attempts(
        self,
        verification_data_list: List[model.VerificationData],
        user_id: int,
        verification_type: str,
        first_name: str,
        last_name: str,
        date_of_birth: Optional[datetime.date],
        verified_at: Optional[datetime.datetime],
        connection: asyncpg.Connection = None,
    ) -> Dict[int, model.VerificationAttempt]:
        """Create verification attempt records and update verification data.
        Returns a dictionary mapping organization_id to VerificationAttempt objects."""
        try:
            verification_attempts = (
                await self.verifications.create_multiple_verification_attempts(
                    verification_type=verification_type,
                    date_of_birth=date_of_birth,
                    verification_data_list=verification_data_list,
                    first_name=first_name,
                    last_name=last_name,
                    policy_used={},  # TODO: Add policy when logging is implemented
                    verified_at=verified_at,
                    user_id=user_id,
                    connection=connection,
                )
            )
            return {
                attempt.organization_id: attempt for attempt in verification_attempts
            }
        except Exception as e:
            organization_ids = [data.organization_id for data in verification_data_list]
            unique_corp_ids = [data.unique_corp_id for data in verification_data_list]
            eligibility_member_ids = [
                data.eligibility_member_id for data in verification_data_list
            ]
            verification_ids = [data.verification_id for data in verification_data_list]

            logger.exception(
                "Error persisting verification attempt records",
                error=e,
                user_id=user_id,
                organization_ids=organization_ids,
                unique_corp_ids=unique_corp_ids,
                eligibility_member_ids=eligibility_member_ids,
                verification_ids=verification_ids,
            )
            raise errors.CreateVerificationError(
                "Error persisting verification attempt records"
            )

    async def _create_member_verifications(
        self,
        verification_data_list: List[model.VerificationData],
        connection: asyncpg.Connection = None,
    ) -> None:
        """Tie eligibility member records to verifications."""
        try:
            records_to_save = []
            for data in verification_data_list:
                # Validate that both member_id and verification_id are not None
                if data.eligibility_member_id in (None, 0) or data.verification_id in (
                    None,
                    0,
                ):
                    logger.info(
                        "Skip creating member_verification record due to missing verification_id/member_id",
                        member_id=data.eligibility_member_id,
                        verification_id=data.verification_id,
                        verification_attempt_id=data.verification_attempt_id,
                    )
                    continue
                record = model.MemberVerification(
                    member_id=data.eligibility_member_id,
                    verification_id=data.verification_id,
                    verification_attempt_id=data.verification_attempt_id,
                )
                records_to_save.append(record)

            if records_to_save:
                await self.verifications.create_multiple_member_verifications(
                    records_to_save,
                    connection=connection,
                )
        except Exception as e:
            eligibility_member_ids = [
                data.eligibility_member_id for data in verification_data_list
            ]
            verification_ids = [data.verification_id for data in verification_data_list]
            verification_attempt_ids = [
                data.verification_attempt_id for data in verification_data_list
            ]

            logger.exception(
                "Error persisting member_verification records",
                error=e,
                eligibility_member_id=eligibility_member_ids,
                verification_id=verification_ids,
                verification_attempt_id=verification_attempt_ids,
            )
            raise errors.CreateVerificationError(
                "Error persisting member_verification records"
            )

    async def get_eligible_features_for_user(
        self,
        *,
        user_id: int,
        feature_type: int,
    ) -> List[int] | None:
        """
        Fetch eligible features for a given user and the specified feature type

        Args:
            user_id: The ID of the user requesting.
            feature_type: The numeric identifier of the feature type

        Returns:
            A list of feature ids if an active population exists. If no population is configured, it will
            return a None to indicate that the user isn't blocked from any particular features.
        """
        sub_pop_id, is_active_pop = await self.get_sub_population_id_for_user(
            user_id=user_id
        )

        # If the member's subpopulation is not found, return an empty list to prevent providing incorrect access
        if sub_pop_id is None:
            logger.info(
                "sub_pop_id not found",
                user_id=user_id,
                feature_type=feature_type,
                is_active_pop=is_active_pop,
            )
            return [] if is_active_pop else None

        # If a sub_population is found, get the IDs for the features of the type requested
        return await self.sub_populations.get_feature_list_of_type_for_id(
            id=sub_pop_id, feature_type=feature_type
        )

    async def get_eligible_features_for_user_and_org(
        self,
        *,
        user_id: int,
        organization_id: int,
        feature_type: int,
    ) -> List[int] | None:
        """
        Fetch eligible features for given (user, org) and the specified feature type

        Args:
            user_id: The ID of the user requesting.
            organization_id: The ID of the organization requesting.
            feature_type: The numeric identifier of the feature type

        Returns:
            A list of feature ids if an active population exists. If no population is configured, it will
            return a None to indicate that the user isn't blocked from any particular features.
        """
        sub_pop_id, is_active_pop = await self.get_sub_population_id_for_user_and_org(
            user_id=user_id, organization_id=organization_id
        )

        # If the member's subpopulation is not found, return an empty list to prevent providing incorrect access
        if sub_pop_id is None:
            return [] if is_active_pop else None

        # If a sub_population is found, get the IDs for the features of the type requested
        return await self.sub_populations.get_feature_list_of_type_for_id(
            id=sub_pop_id, feature_type=feature_type
        )

    @tracer.wrap()
    async def deactivate_verification_for_user(
        self,
        *,
        verification_id: int,
        user_id: int,
    ) -> model.Verification:
        """
        Deactivate a verification record associated with a user

        Args:
            verification_id: The ID of the verification record associated with the user
            user_id: The ID of the user
        Returns:
            Verification record that was deactivated
        """
        try:
            return await self.verifications.deactivate_verification_for_user(
                verification_id=verification_id, user_id=user_id
            )

        except Exception as e:
            logger.exception(
                "Error deactivating verification record for the user",
                error=e,
                verification_id=verification_id,
                user_id=user_id,
            )
            raise errors.DeactivateVerificationError(
                "Error deactivating verification record for the user"
            )

    async def get_eligible_features_by_sub_population_id(
        self,
        *,
        sub_population_id: int,
        feature_type: int,
    ) -> List[int] | None:
        """
        Fetch eligible features for a given sub-population and the specified feature type

        Args:
            sub_population_id: The ID of the sub-population
            feature_type: The numeric identifier of the feature type

        Returns:
            A list of feature ids for the sub-population. If no sub-population is configured, it will
            return a None to indicate that the user isn't blocked from any particular features.
        """
        return await self.sub_populations.get_feature_list_of_type_for_id(
            id=sub_population_id, feature_type=feature_type
        )

    async def get_sub_population_id_for_user(
        self,
        *,
        user_id: int,
    ) -> (int, bool) | (None, bool):
        """
        Gets the sub-population ID for the user

        Args:
            user_id: The ID of the user

        Returns:
            A sub-population ID of the user and a boolean indicating whether or not a population
            was configured. The boolean can be used to determine whether to allow all features,
            due to the lack of an active population, or to disallow all features, due to not being
            assigned to a sub-population
        """
        # Get the population information for the user
        pop_info = await self.populations.get_the_population_information_for_user_id(
            user_id=user_id
        )
        # If no population is configured, return None to allow access to all features
        if pop_info is None:
            logger.info(
                "No pop info records found with user id",
                user_id=user_id,
                method="get_sub_population_id_for_user",
            )
            return None, False

        verification_record = await self.verifications.get_verification_key_for_user(
            user_id=user_id
        )
        if not verification_record:
            logger.info(
                "No verification records found with user id",
                user_id=user_id,
                method="get_sub_population_id_for_user",
            )
            return None, True

        member = None
        is_v2 = feature_flag.organization_enabled_for_e9y_2_read(
            verification_record.organization_id
        )
        if is_v2:
            set_tracer_tags(
                {"is_v2": True, "organization_id": verification_record.organization_id}
            )
            member = await self.members_2.get(verification_record.member_2_id)

        # If v2 is not enabled, or v2 record is not found, fallback to v1
        if not member:
            member = await self.members_versioned.get(verification_record.member_id)
            # error logging if we can find the member in v1 but not v2
            if member and is_v2:
                logger.error(
                    "No member records found with member 2 id of v2",
                    verification_id=verification_record.verification_1_id,
                    member_id=verification_record.member_id,
                    member_2_id=verification_record.member_2_id,
                    user_id=user_id,
                    method="get_sub_population_id_for_user",
                )

        # If the member is not found, return None
        if member is None:
            logger.info(
                "No member records found with member id",
                member_id=verification_record.member_id,
                member_2_id=verification_record.member_2_id,
                method="get_sub_population_id_for_user",
            )
            return None, True

        if not pop_info.advanced:
            # Find the sub-population ID using the lookup keys and the Eligibility Member information
            return (
                await self.populations.get_the_sub_pop_id_using_lookup_keys_for_member(
                    lookup_keys_csv=pop_info.sub_pop_lookup_keys_csv,
                    member=member,
                    population_id=pop_info.population_id,
                ),
                True,
            )
        else:
            # Find the sub-population ID for the advanced population configuration
            return (
                await e9y_member_utils.get_advanced_sub_pop_id_for_member(
                    member=member,
                    population_id=pop_info.population_id,
                ),
                True,
            )

    async def get_sub_population_id_for_user_and_org(
        self,
        *,
        user_id: int,
        organization_id: int,
    ) -> (int, bool) | (None, bool):
        """
        Gets the sub-population ID for the user and specified org

        Args:
            user_id: The ID of the user
            organization_id: The ID of the organization

        Returns:
            A sub-population ID for (user, org) and a boolean indicating whether or not a population
            was configured. The boolean can be used to determine whether to allow all features,
            due to the lack of an active population, or to disallow all features, due to not being
            assigned to a sub-population
        """
        # Get the population information for the user
        pop_info = (
            await self.populations.get_the_population_information_for_user_and_org(
                user_id=user_id, organization_id=organization_id
            )
        )
        # If no population is configured, return None to allow access to all features
        if pop_info is None:
            return None, False

        member = None
        is_v2 = feature_flag.organization_enabled_for_e9y_2_read(organization_id)
        verification_1_id = None
        if is_v2:
            set_tracer_tags({"is_v2": True, "organization_id": organization_id})
            verification_record = (
                await self.verifications.get_verification_key_2_for_user_and_org(
                    user_id=user_id,
                    organization_id=organization_id,
                )
            )
            if not verification_record:
                logger.info(
                    "No verification records found with user id and org id of v2",
                    user_id=user_id,
                    organization_id=organization_id,
                    method="get_sub_population_id_for_user_and_org",
                )
            else:
                member_id = verification_record.member_2_id
                member = await self.members_2.get(member_id)
                verification_1_id = verification_record.verification_1_id

        # If v2 is not enabled, or v2 record is not found, fallback to v1
        if not member:
            member_id = (
                await self.verifications.get_eligibility_member_id_for_user_and_org(
                    user_id=user_id, organization_id=organization_id
                )
            )
            # If the member ID is not found, return None
            if member_id is None:
                return None, True

            member = await self.members_versioned.get(member_id)
            # error logging if we can find the member in v1 but not v2
            if member and is_v2:
                logger.error(
                    "No member records found with member 2 id of v2",
                    member_id=member_id,
                    user_id=user_id,
                    organization_id=organization_id,
                    verification_1_id=verification_1_id,
                    method="get_sub_population_id_for_user_and_org",
                )

        # If the member is not found, return None
        if member is None:
            logger.info(
                "No member records found with member id",
                member_id=member_id,
                method="get_sub_population_id_for_user_and_org",
            )
            return None, True

        if not pop_info.advanced:
            # Find the sub-population ID using the lookup keys and the Eligibility Member information
            return (
                await self.populations.get_the_sub_pop_id_using_lookup_keys_for_member(
                    lookup_keys_csv=pop_info.sub_pop_lookup_keys_csv,
                    member=member,
                    population_id=pop_info.population_id,
                ),
                True,
            )
        else:
            # Find the sub-population ID for the advanced population configuration
            return (
                await e9y_member_utils.get_advanced_sub_pop_id_for_member(
                    member=member,
                    population_id=pop_info.population_id,
                ),
                True,
            )

    async def get_other_user_ids_in_family(self, user_id: int) -> List[int]:
        """
        Gets the other active user_id's for a "family" as defined by a shared "unique_corp_id"

        Args:
            user_id: The ID of the user

        Returns:
            A list of user_id's, does not include the input user's own user_id
        """
        verification_record = await self.verifications.get_verification_key_for_user(
            user_id=user_id
        )
        if not verification_record:
            logger.info(
                "No verification records found with user id",
                user_id=user_id,
                method="get_other_user_ids_in_family",
            )
            return []

        family_user_ids = []
        is_v2 = feature_flag.organization_enabled_for_e9y_2_read(
            verification_record.organization_id
        )
        if is_v2:
            set_tracer_tags(
                {"is_v2": True, "organization_id": verification_record.organization_id}
            )
            family_user_ids = await self.members_2.get_other_user_ids_in_family(
                user_id=user_id
            )

        # If v2 is not enabled, or v2 record is not found, fallback to v1
        if not family_user_ids:
            family_user_ids = await self.members_versioned.get_other_user_ids_in_family(
                user_id=user_id
            )

            if family_user_ids and is_v2:
                logger.error(
                    "No family user ids found with user id using v2",
                    user_id=user_id,
                )

        return family_user_ids

    async def verify_eligibility_record_usable(
        self, eligibility_member_id: int, organization_id: int
    ) -> bool:
        """
        Determine if an eligibility record is already used in a verification
        If so, can it be re-used (i.e. associated with a dependent?)
        True - record can be used
        False - record should not be used
        """

        # First, see if our E9y record is used in an existing verification
        if feature_flag.organization_enabled_for_e9y_2_write(organization_id):
            member_verification = (
                await self.verifications.get_verification_2_for_member(
                    member_id=eligibility_member_id
                )
            )
        else:
            member_verification = await self.verifications.get_verification_for_member(
                member_id=eligibility_member_id
            )

        if not member_verification:
            return True

        # If it's already in use, can we use it again?
        # We CANNOT IF the org provides:
        #   a) coverage for only employees
        #   b) coverage through a selected medical plan but not beneficiaries
        organization_record = await self.configurations.get(organization_id)

        # Does the organization explicitly say employee only?
        if organization_record.employee_only:
            return False

        # Are we looking at a medical plan? Are beneficiaries enabled?
        if feature_flag.organization_enabled_for_e9y_2_write(organization_id):
            e9y_record = await self.members_2.get(eligibility_member_id)
        else:
            e9y_record = await self.members_versioned.get(eligibility_member_id)
        beneficiaries_enabled = e9y_record.record.get("beneficiaries_enabled", False)
        if organization_record.medical_plan_only and not beneficiaries_enabled:
            return False

        return True

    @classmethod
    def _convert_member_to_member_response(
        cls,
        member: model.Member | model.Member2 | model.MemberVersioned,
        is_v2,
        member_1_id,
        member_2_id,
    ) -> model.MemberResponse:
        version = 0 if not isinstance(member, model.Member2) else member.version
        file_id = member.file_id if not isinstance(member, model.Member2) else 0

        return model.MemberResponse(
            id=member.id,
            version=version,
            organization_id=member.organization_id,
            first_name=member.first_name,
            last_name=member.last_name,
            date_of_birth=member.date_of_birth,
            file_id=file_id,
            work_state=member.work_state,
            work_country=member.work_country,
            email=member.email,
            unique_corp_id=member.unique_corp_id,
            employer_assigned_id=member.employer_assigned_id,
            dependent_id=member.dependent_id,
            effective_range=member.effective_range,
            record=member.record,
            custom_attributes=member.custom_attributes,
            do_not_contact=member.do_not_contact,
            gender_code=member.gender_code,
            created_at=member.created_at,
            updated_at=member.updated_at,
            is_v2=is_v2,
            member_1_id=member_1_id,
            member_2_id=member_2_id,
        )

    @classmethod
    def _convert_enablement_to_enablement_response(
        cls,
        enablement: model.WalletEnablement,
        is_v2,
        member_1_id,
        member_2_id,
    ) -> model.WalletEnablementResponse:
        return model.WalletEnablementResponse(
            member_id=enablement.member_id,
            organization_id=enablement.organization_id,
            unique_corp_id=enablement.unique_corp_id,
            dependent_id=enablement.dependent_id,
            enabled=enablement.enabled,
            insurance_plan=enablement.insurance_plan,
            start_date=enablement.start_date,
            eligibility_date=enablement.eligibility_date,
            created_at=enablement.created_at,
            updated_at=enablement.updated_at,
            effective_range=enablement.effective_range,
            is_v2=is_v2,
            member_1_id=member_1_id,
            member_2_id=member_2_id,
        )

    @classmethod
    def _validate_user_id(cls, user_id: int | None) -> int:
        if user_id is None:
            raise ValidationError("Null user_id provided", user_id=user_id)
        return user_id

    @classmethod
    def _validate_optional_date(cls, date: str | None) -> datetime.date | None:
        if not date:
            return None
        return cls._validate_date(date)

    @classmethod
    def _validate_optional_timestamp(
        cls, timestamp: str | None
    ) -> datetime.datetime | None:
        if not timestamp:
            return None
        if isinstance(timestamp, datetime.datetime):
            return timestamp
        return datetime.datetime.fromisoformat(timestamp)

    @staticmethod
    def _validate_date(date: datetime.date | str) -> datetime.date:
        parsed = convert.to_date(date)
        if parsed is convert.DATE_UNKNOWN:
            logger.error("Unsupported date format provided", date=date)
            raise ValidationError(
                "Unsupported date format provided.",
                date=date,
            )
        return parsed

    @staticmethod
    def _validate_identity(
        *,
        organization_id: int,
        unique_corp_id: str,
        dependent_id: str,
    ) -> model.OrgIdentity:
        if not unique_corp_id:
            logger.error(
                "Got an empty value for unique_corp_id.",
                unique_corp_id=unique_corp_id,
                organization_id=organization_id,
                dependent_id=dependent_id,
            )

            raise ValidationError(
                "Got an empty value for unique_corp_id.",
                unique_corp_id=unique_corp_id,
                organization_id=organization_id,
                dependent_id=dependent_id,
            )
        return model.OrgIdentity(
            organization_id=organization_id,
            unique_corp_id=unique_corp_id,
            dependent_id=dependent_id,
        )

    @staticmethod
    def _validate_verification_type(
        verification_type: str,
        organization_id: int = None,
        unique_corp_id: int = None,
        eligibility_member_id: int = None,
    ) -> str:
        if verification_type.upper() not in [v.value for v in model.VerificationTypes]:
            logger.error(
                "Received an unknown verification type",
                organization_id=organization_id,
                unique_corp_id=unique_corp_id,
                eligibility_member_id=eligibility_member_id,
                verification_type=verification_type,
            )
            raise ValidationError(
                f"Received an unknown verification type : {verification_type}",
                organization_id=organization_id,
                unique_corp_id=unique_corp_id,
                eligibility_member_id=eligibility_member_id,
                verification_type=verification_type,
            )
        return verification_type.upper()

    @async_ttl_cache.AsyncTTLCache(time_to_live=30 * 60, max_size=1024)
    async def _check_organization_active_status(self, organization_id: int) -> bool:
        """
        get organization information and return if organization is active or not
        :param organization_id: organization id
        :return: True or False
        """
        organization = await self.configurations.get(organization_id)
        return is_organization_activated(organization)


class ValidationError(ValueError):
    def __init__(self, message: str, **fields):
        super().__init__(message)
        self.fields = fields


_SERVICE: contextvars.ContextVar[Optional[EligibilityService]] = contextvars.ContextVar(
    "e9y_service", default=None
)

_PRE_ELIGIBILITY_SERVICE: contextvars.ContextVar[
    Optional[PreEligibilityService]
] = contextvars.ContextVar("pre9y_service", default=None)

_TEST_SERVICE: contextvars.ContextVar[
    Optional[EligibilityTestUtilityService]
] = contextvars.ContextVar("e9y_test_utility_service", default=None)


def service() -> EligibilityService:
    if (svc := _SERVICE.get()) is None:
        svc = EligibilityService()
        _SERVICE.set(svc)
    return svc


class PreEligibilityService:
    __slots__ = ("members", "members_versioned")

    def __init__(self):
        self.members = member_client.Members()
        self.members_versioned = member_versioned_client.MembersVersioned()

    async def get_members_by_name_and_date_of_birth(
        self,
        *,
        first_name: str,
        last_name: str,
        date_of_birth: datetime.date | str,
    ) -> List[model.Member]:
        """Searches for member information using first_name, last_name, and date_of_birth

        Args:
            first_name: The first name to match against. (case-insensitive)
            last_name: The last name to match against. (case-insensitive)
            date_of_birth: The date of birth to match against.

        Returns:
            Member record(s), if found with active eligibility only
            empty list if no match is found.
        """
        members = await self.members_versioned.get_by_name_and_date_of_birth(
            first_name=first_name,
            last_name=last_name,
            date_of_birth=service()._validate_date(date_of_birth),
        )

        if not members:
            return []
        return members


def pre_eligibility_service() -> PreEligibilityService:
    if (svc := _PRE_ELIGIBILITY_SERVICE.get()) is None:
        svc = PreEligibilityService()
        _PRE_ELIGIBILITY_SERVICE.set(svc)
    return svc


class EligibilityTestUtilityService:
    __slots__ = ["_members_versioned", "_members"]

    DEFAULT_FIRST_NAME = "E9Y Test User"
    DEFAULT_LAST_NAME = "API"
    DEFAULT_EMAIL = "apiTestUser@testemail.com"
    DEFAULT_CORP_ID = "apiTestUser"
    DEFAULT_DEPENDENT_ID = "apiTestDependent"
    DEFAULT_WORK_COUNTRY = "US"
    DEFAULT_DATE_OF_BIRTH = "1970-01-01"

    def __init__(self):
        self._members_versioned = member_versioned_client.MembersVersioned()
        self._members = member_client.Members()

    async def create_members_for_organization(
        self, organization_id: str, test_records: List[model.MemberTestRecord]
    ) -> List[model.MemberVersioned]:
        """Create members for the specified organization based on test records.

        This method creates members for the specified organization using the provided test records.
        It iterates through each test record, resolves the attributes, creates a member, and persists
        it to the database.

        Args:
            self: The class instance.
            organization_id (str): The ID of the organization for which members are being created.
            test_records (List[model.MemberTestRecord]): A list of test records containing information
                about the members to be created.

        Returns:
            List[model.MemberVersioned]: A list of persisted member objects.
        """
        if feature_flag.is_write_disabled():
            raise errors.CreateVerificationError(
                "Creation is disabled due to feature flag"
            )

        members = []
        legacy_members = []
        for record in test_records:
            test_record = self.resolve_test_member(test_record=record)
            date_of_birth = convert.to_date(test_record["date_of_birth"])
            member: model.MemberVersioned = MemberVersionedFactory.create(
                organization_id=organization_id,
                first_name=test_record["first_name"],
                last_name=test_record["last_name"],
                email=test_record["email"],
                effective_range=test_record["effective_range"],
                date_of_birth=date_of_birth,
                unique_corp_id=test_record["unique_corp_id"],
                dependent_id=test_record["dependent_id"],
                work_country=test_record["work_country"],
            )
            members.append(member)
            legacy_member: model.Member = MemberFactory.create(
                file_id=None,
                organization_id=organization_id,
                first_name=test_record["first_name"],
                last_name=test_record["last_name"],
                email=test_record["email"],
                effective_range=test_record["effective_range"],
                date_of_birth=date_of_birth,
                unique_corp_id=test_record["unique_corp_id"],
                dependent_id=test_record["dependent_id"],
                work_country=test_record["work_country"],
            )
            legacy_members.append(legacy_member)
        persisted: List[
            model.MemberVersioned
        ] = await self._members_versioned.bulk_persist(models=members)

        _ = await self._members.bulk_persist(models=legacy_members)
        return persisted

    def resolve_test_member(self, test_record: model.MemberTestRecord) -> dict:
        """Resolve attributes for a test member record.

        This function takes a `test_record` object and extracts various attributes
        to create a dictionary representing the test member. If an attribute is missing
        in the `test_record`, default values are used. The `effective_range` attribute
        is calculated based on the current date, where the lower bound is set to
        yesterday and the upper bound is set to a year from now.

        Args:
            test_record: An object representing the test member record.

        Returns:
            dict: A dictionary containing the resolved attributes for the test member.
                The dictionary includes the following keys:
                    - 'first_name': The first name of the test member.
                    - 'last_name': The last name of the test member.
                    - 'email': The email address of the test member.
                    - 'unique_corp_id': The unique corporate ID of the test member.
                    - 'dependent_id': The dependent ID of the test member.
                    - 'effective_range': A DateRange object representing the effective
                        date range for the test member.
                    - 'work_country': The country where the test member works.

        """
        first_name = test_record.get("first_name", self.DEFAULT_FIRST_NAME)
        last_name = test_record.get("last_name", self.DEFAULT_LAST_NAME)
        email = test_record.get("email", self.DEFAULT_EMAIL)
        unique_corp_id = test_record.get("unique_corp_id", self.DEFAULT_CORP_ID)
        dependent_id = test_record.get("dependent_id", self.DEFAULT_DEPENDENT_ID)
        date_of_birth = test_record.get("date_of_birth", self.DEFAULT_DATE_OF_BIRTH)
        work_country = test_record.get("work_country", self.DEFAULT_WORK_COUNTRY)
        # set default effective range
        effective_range = DateRangeFactory.create(
            lower=datetime.date.today() - datetime.timedelta(days=1),
            upper=datetime.date.today() + datetime.timedelta(days=365),
        )

        return {
            "first_name": first_name,
            "last_name": last_name,
            "email": email,
            "date_of_birth": date_of_birth,
            "unique_corp_id": unique_corp_id,
            "dependent_id": dependent_id,
            "effective_range": effective_range,
            "work_country": work_country,
        }


def eligibility_test_utility_service() -> EligibilityTestUtilityService:
    if (svc := _TEST_SERVICE.get()) is None:
        svc = EligibilityTestUtilityService()
        _TEST_SERVICE.set(svc)
    return svc


def set_tracer_tags(tags_dict):
    """
    Temporary function for v2 rollout monitoring.
    """
    span = tracer.current_root_span()
    if span:
        span.set_tags(tags_dict)
    else:
        logger.warn("Cannot get current root span")
