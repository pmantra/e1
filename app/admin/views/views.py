import re
from typing import Iterable, Optional

from flask import request
from flask_admin import BaseView, expose
from flask_admin.contrib.sqla import filters as sqla_filters
from mmlib.ops.log import getLogger

from app.admin.base import base_view, filter
from app.admin.base.base_view import BaseViewWithSessionRollback
from app.admin.base.protocol_view import ServiceProtocolModelView
from app.admin.file_parsing.file_parse_result_errors_view import (
    FileParseErrorView,
    FileParseResultsView,
)
from app.admin.file_parsing.incomplete_files_view import IncompleteFilesView
from app.admin.member import filter as member_filter
from app.admin.member.generation import MemberGenerationView
from app.admin.populations.population_view import PopulationView
from app.admin.transform_entries.transform_entries_view import TransformEntriesView
from app.admin.verification.user_verification_view import UserVerificationView
from config import settings
from db.clients import (
    configuration_client,
    file_client,
    header_aliases_client,
    member_2_client,
    member_verification_client,
    member_versioned_client,
    verification_2_client,
    verification_attempt_client,
    verification_client,
)
from db.sqlalchemy.models.file_parse_errors import FileParseErrors
from db.sqlalchemy.models.file_parse_results import FileParseResults
from db.sqlalchemy.models.member_2 import Member2
from db.sqlalchemy.models.member_verification import MemberVerifications
from db.sqlalchemy.models.member_versioned import MemberVersioned
from db.sqlalchemy.models.verification import Verifications
from db.sqlalchemy.models.verification_2 import Verifications2
from db.sqlalchemy.models.verification_attempt import VerificationAttempts
from db.sqlalchemy.sqlalchemy_config import get_session_maker

logger = getLogger(__name__)
_original_base_url = None


class FilesModelView(ServiceProtocolModelView):
    def __init__(
        self,
        name=None,
        category=None,
        endpoint=None,
        url=None,
        static_folder=None,
        menu_class_name=None,
        menu_icon_type=None,
        menu_icon_value=None,
    ):
        super().__init__(
            file_client.Files,
            name=name,
            category=category,
            endpoint=endpoint,
            url=url,
            static_folder=static_folder,
            menu_class_name=menu_class_name,
            menu_icon_type=menu_icon_type,
            menu_icon_value=menu_icon_value,
        )

    column_list = list(file_client.Files.model.__annotations__.keys())
    column_list.remove("id")
    column_list.insert(0, "id")

    can_delete = False
    can_create = False
    can_edit = False
    can_export = True
    can_view_details = True
    simple_list_pager = True
    default_sort_field = "created_at"
    default_sort_desc = True

    column_sortable_list = list(file_client.Files.model.__annotations__.keys())

    column_details_list = column_list

    column_filters = (
        filter.IntEqualFilter("organization_id"),
        filter.FilterEqual("name"),
        filter.FilterEqual(
            "status",
            options=[(s.name, s.value) for s in file_client.FileStatus],
        ),
    )


class ConfigurationsModelView(ServiceProtocolModelView):
    def __init__(
        self,
        name=None,
        category=None,
        endpoint=None,
        url=None,
        static_folder=None,
        menu_class_name=None,
        menu_icon_type=None,
        menu_icon_value=None,
    ):
        super().__init__(
            configuration_client.Configurations,
            name=name,
            category=category,
            endpoint=endpoint,
            url=url,
            static_folder=static_folder,
            menu_class_name=menu_class_name,
            menu_icon_type=menu_icon_type,
            menu_icon_value=menu_icon_value,
        )

    column_filters = (
        filter.IntEqualFilter("organization_id"),
        filter.FilterEqual("directory_name"),
    )


class HeaderAliasesModelsView(ServiceProtocolModelView):
    def __init__(
        self,
        name=None,
        category=None,
        endpoint=None,
        url=None,
        static_folder=None,
        menu_class_name=None,
        menu_icon_type=None,
        menu_icon_value=None,
    ):
        super().__init__(
            header_aliases_client.HeaderAliases,
            name=name,
            category=category,
            endpoint=endpoint,
            url=url,
            static_folder=static_folder,
            menu_class_name=menu_class_name,
            menu_icon_type=menu_icon_type,
            menu_icon_value=menu_icon_value,
        )

    column_filters = (
        filter.IntEqualFilter("organization_id"),
        filter.IntEqualFilter("id"),
        filter.FilterEqual("header"),
        filter.FilterEqual("alias"),
    )


class MemberVersionedModelView(base_view.ModelViewWithSessionRollback):

    column_filters = (
        "id",
        "file_id",
        "unique_corp_id",
        sqla_filters.FilterEqual(
            column=MemberVersioned.employer_assigned_id, name="Employer Assigned ID"
        ),
        sqla_filters.FilterInList(
            column=MemberVersioned.employer_assigned_id, name="Employer Assigned ID"
        ),
        "dependent_id",
        "email",
        "first_name",
        "last_name",
        "organization_id",
        "date_of_birth",
        member_filter.ActiveE9yRecordFilter(
            column=None,
            name="Active/Inactive Eligibility Only",
            options=((True, "Active Only"), (False, "Inactive Only")),
        ),
    )
    column_display_pk = True
    column_hide_backrefs = False

    # def get_query(self):
    #     return self.session.query(self.model).filter(self.model.effective_range.contains(datetime.date.today()))

    column_list = list(
        member_versioned_client.MembersVersioned.model.__annotations__.keys()
    )
    column_list.remove("id")
    column_list.insert(0, "id")
    column_list.insert(9, "active")

    can_delete = False
    can_create = False
    can_edit = False
    can_export = True
    can_view_details = True
    simple_list_pager = True

    column_sortable_list = list(
        member_versioned_client.MembersVersioned.model.__annotations__.keys()
    )

    column_details_list = column_list
    column_details_list.insert(10, "reason_member_inactive")


class Member2ModelView(base_view.ModelViewWithSessionRollback):

    column_filters = (
        "id",
        "unique_corp_id",
        sqla_filters.FilterEqual(
            column=Member2.employer_assigned_id, name="Employer Assigned ID"
        ),
        sqla_filters.FilterInList(
            column=Member2.employer_assigned_id, name="Employer Assigned ID"
        ),
        "dependent_id",
        "email",
        "first_name",
        "last_name",
        "organization_id",
        "date_of_birth",
        member_filter.ActiveE9yRecordFilter(
            column=None,
            name="Active/Inactive Eligibility Only",
            options=((True, "Active Only"), (False, "Inactive Only")),
        ),
    )
    column_display_pk = True
    column_hide_backrefs = False
    column_list = list(member_2_client.Member2Client.model.__annotations__.keys())
    column_list.remove("id")
    column_list.insert(0, "id")
    column_list.insert(9, "active")

    can_delete = False
    can_create = False
    can_edit = False
    can_export = True
    can_view_details = True
    simple_list_pager = True

    column_sortable_list = list(
        member_2_client.Member2Client.model.__annotations__.keys()
    )

    column_details_list = column_list
    column_details_list.insert(10, "reason_member_inactive")


class MemberVerificationsModelView(base_view.ModelViewWithSessionRollback):

    column_filters = (
        "id",
        "member_id",
        "verification_id",
        "verification_attempt_id",
    )
    column_display_pk = True
    column_hide_backrefs = False

    column_list = list(
        member_verification_client.MemberVerifications.model.__annotations__.keys()
    )
    column_list.remove("id")
    column_list.insert(0, "id")

    can_delete = False
    can_create = False
    can_edit = False
    can_export = True
    can_view_details = True

    column_sortable_list = list(
        member_verification_client.MemberVerifications.model.__annotations__.keys()
    )

    column_details_list = column_list


class VerificationsModelView(base_view.ModelViewWithSessionRollback):

    column_filters = (
        "id",
        "user_id",
        "organization_id",
        "verification_type",
        "unique_corp_id",
        "first_name",
        "last_name",
        "dependent_id",
    )
    column_exclude_list = ["configurations"]
    column_display_pk = True
    column_hide_backrefs = False

    column_list = list(verification_client.Verifications.model.__annotations__.keys())
    column_list.remove("id")
    column_list.insert(0, "id")

    can_delete = False
    can_create = False
    can_edit = False
    can_export = True
    can_view_details = True

    column_sortable_list = list(
        verification_client.Verifications.model.__annotations__.keys()
    )

    column_details_list = column_list


class Verifications2ModelView(base_view.ModelViewWithSessionRollback):

    column_filters = (
        "id",
        "user_id",
        "organization_id",
        "verification_type",
        "unique_corp_id",
        "first_name",
        "last_name",
        "dependent_id",
        "member_id",
    )
    column_exclude_list = ["configurations"]
    column_display_pk = True
    column_hide_backrefs = False

    column_list = list(
        verification_2_client.Verification2Client.model.__annotations__.keys()
    )
    column_list.remove("id")
    column_list.insert(0, "id")

    can_delete = False
    can_create = False
    can_edit = False
    can_export = True
    can_view_details = True

    column_sortable_list = list(
        verification_2_client.Verification2Client.model.__annotations__.keys()
    )

    column_details_list = column_list


class VerificationAttemptsModelView(base_view.ModelViewWithSessionRollback):

    column_filters = (
        "id",
        "user_id",
        "organization_id",
        "verification_type",
        "unique_corp_id",
        "first_name",
        "last_name",
        "dependent_id",
    )
    column_exclude_list = ["configurations"]
    column_display_pk = True
    column_hide_backrefs = False

    column_list = list(
        verification_attempt_client.VerificationAttempts.model.__annotations__.keys()
    )
    column_list.remove("id")
    column_list.insert(0, "id")

    can_delete = False
    can_create = False
    can_edit = False
    can_export = True
    can_view_details = True

    column_sortable_list = list(
        verification_attempt_client.VerificationAttempts.model.__annotations__.keys()
    )

    column_details_list = column_list


def get_referrer_base_url() -> Optional[str]:
    """
    This helper function gets the base url from the referrer.
    """
    referrer = request.referrer
    if referrer is None:
        return None

    # Uses regex to get the pattern for the base url with http or https
    re_matches = re.match(pattern="(https?://[^/]*)", string=referrer)
    return re_matches.group(0) if re_matches else None


def add_original_base_url(url: str) -> str:
    """
    This helper function prepends the referrer's base url to the provided url. It
    also does some sanity checking to make sure that the provided url does not
    already start with http and that the referrer does include a valid base url.

    This is needed because of the way the Eligibility Admin is loaded by Mono. Mono
    uses a WSGI proxy to get content from http://eligibility-admin/, but that causes
    problems for the redirect() calls from within Eligibility Admin since
    http://eligibility-admin/ is not publicly available. By using the base url from
    the referrer, the connection will continue to work through the WSGI proxy.
    """
    ret_url = url
    if not ret_url.startswith("http"):
        global _original_base_url
        # Check if the original base URL has already been saved
        if _original_base_url is None:
            # Set the original base URL to that of the referrer
            _original_base_url = get_referrer_base_url()

        # Check if the original base URL was extracted successfully before using it
        if _original_base_url is not None:
            ret_url = _original_base_url + ret_url
    return ret_url


class HealthCheckView(BaseViewWithSessionRollback):
    @expose("/")
    def health(self):
        return {"status": "healthy"}

    def is_visible(self):
        return False


def get_views() -> Iterable[BaseView]:
    """Must return an iterable of Flask Admin Views"""
    Session = get_session_maker()
    SessionWithStatementTime = get_session_maker(max_execution_seconds=300)

    pending_files = IncompleteFilesView(
        name="Incomplete Files",
        endpoint="incomplete-files",
    )
    records_view = FileParseResultsView(
        FileParseResults,
        Session(),
        name="Parsed Records",
        endpoint="parsed-records",
    )
    errors_view = FileParseErrorView(
        FileParseErrors,
        Session(),
        name="Parsed Errors",
        endpoint="parsed-errors",
    )
    files_view = FilesModelView()
    configs_view = ConfigurationsModelView(category="Organization")
    population_view = PopulationView(
        name="Population",
        endpoint="population",
        category="Organization",
    )
    headers_view = HeaderAliasesModelsView(name="Header Aliases", url="header-aliases")
    user_verification_view = UserVerificationView(
        name="User Verifications",
        url="user-verification",
        category="User Enrollment",
    )
    member_versioned_view = MemberVersionedModelView(
        MemberVersioned,
        SessionWithStatementTime(),
        name="Member",
        endpoint="member_versioned",
    )
    member_2_view = Member2ModelView(
        Member2,
        SessionWithStatementTime(),
        name="Member_2",
        endpoint="member_2",
        category="Internal Testing",
    )
    verification_view = VerificationsModelView(
        Verifications,
        Session(),
        name="Verification",
        endpoint="verification",
        category="User Enrollment",
    )
    verification_2_view = Verifications2ModelView(
        Verifications2,
        Session(),
        name="Verification_2",
        endpoint="verification_2",
        category="Internal Testing",
    )
    member_verification_view = MemberVerificationsModelView(
        MemberVerifications,
        Session(),
        name="Member Verification",
        endpoint="member_verification",
        category="User Enrollment",
    )
    verification_attempt_view = VerificationAttemptsModelView(
        VerificationAttempts,
        Session(),
        name="Verification Attempt",
        endpoint="verification_attempt",
        category="User Enrollment",
    )
    transform_entries_view = TransformEntriesView(
        name="Transform Entries",
        url="transform_entries",
        category="Internal Testing",
    )
    health_check_view = HealthCheckView(name="Health Check", url="health")

    views = [
        pending_files,
        records_view,
        errors_view,
        files_view,
        configs_view,
        population_view,
        headers_view,
        member_versioned_view,
        user_verification_view,
        verification_view,
        member_verification_view,
        verification_attempt_view,
        health_check_view,
    ]

    if settings.Admin().can_create_member:
        member_generation_view = MemberGenerationView(
            name="Generate Members",
            url="member-generation",
            category="Other",
        )
        views.append(member_generation_view)

    # Add v2 views to the end
    views.append(member_2_view)
    views.append(verification_2_view)
    views.append(transform_entries_view)

    return views
