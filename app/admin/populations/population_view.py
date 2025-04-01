from __future__ import annotations

import asyncio
import datetime
import re
from typing import Optional, Set

import flask
import flask_admin
import orjson
import structlog.stdlib
from ddtrace import tracer
from flask import request
from flask_admin.babel import gettext
from werkzeug.wrappers import Response

from app.admin.base import base_view
from app.admin.utils import filter as filter_utils
from app.admin.utils import sort as sort_utils
from app.admin.views import views
from app.mono import repository as mono_repo
from db.flask import synchronize
from db.sqlalchemy.models.population import Populations
from db.sqlalchemy.models.sub_population import SubPopulations
from db.sqlalchemy.sqlalchemy_config import Session
from sqlalchemy import and_, or_
from sqlalchemy.exc import DataError, IntegrityError, OperationalError
from sqlalchemy.orm.session import Session as ORM_Session

logger = structlog.getLogger(__name__)


# This is the source of truth for the list of columns displayed in the list page, with the attributes
# defined here to indicate whether or not the column is sortable and if the field should be stylized
# based on the activation/deactivation dates
list_columns = [
    {
        "alias": "Id",
        "attribute": "id",
        "sortable": True,
        "stylized": False,
    },
    {
        "alias": "Organization Id",
        "attribute": "organization_id",
        "sortable": True,
        "stylized": False,
    },
    {
        "alias": "Criteria",
        "attribute": "sub_pop_lookup_keys_csv",
        "sortable": False,
        "stylized": False,
    },
    {
        "alias": "Activated At",
        "attribute": "activated_at",
        "sortable": True,
        "stylized": True,
    },
    {
        "alias": "Deactivated At",
        "attribute": "deactivated_at",
        "sortable": True,
        "stylized": True,
    },
    {
        "alias": "Sub-Populations",
        "attribute": "custom__sub_populations",  # Custom handling within population-list template
        "sortable": False,
        "stylized": False,
    },
]


# This is the source of truth for what filtering is supported based on the field name and operation
# types. The values are modeled after what's used in flask-admin so that we can use the existing
# flask-admin javascript to handle the filtering.
filter_groups_data = {
    "Id": [
        {
            "arg": "0",
            "index": 0,
            "operation": "equals",
            "options": None,
            "type": None,
        },
        {
            "arg": "1",
            "index": 1,
            "operation": "in list",
            "options": None,
            "type": "select2-tags",
        },
    ],
    "Organization Id": [
        {
            "arg": "2",
            "index": 2,
            "operation": "equals",
            "options": None,
            "type": None,
        },
        {
            "arg": "3",
            "index": 3,
            "operation": "in list",
            "options": None,
            "type": "select2-tags",
        },
    ],
    "Active": [
        {
            "arg": "4",
            "index": 4,
            "operation": "equals",
            "options": [["1", "Yes"], ["0", "No"]],
            "type": None,
        },
    ],
}


class PopulationView(base_view.BaseViewWithSessionRollback):
    @tracer.wrap()
    @flask_admin.expose("/")
    def population_list(self) -> str:
        """
        Displays a list of populations with a default sorting of an ascending order of
        organization IDs.
        """
        # Gets the referrer base URL since the WSGI proxy will not work with the actual
        # base URL
        referrer_base_url = views.get_referrer_base_url()
        request_args = request.args
        # By default, no filters and a sort order of ascending organization IDs
        active_filters = []
        sort_index = int(request_args.get("sort", 1))
        sort_direction = int(request_args.get("direction", 0))
        # Re-creates the query string without the sorting to preserve the information
        # so that sorting can be modified without resetting filtering
        unsorted_query_string = "&".join(
            [
                f"{key}={value}"
                for (key, value) in request_args.items()
                if key not in {"sort", "direction"}
            ]
        )
        # Get the alert message if there is one
        alert_msg = request_args.get("alert_msg", None)
        try:
            with Session.begin() as session:
                populations_query = session.query(Populations)
                # Filter
                active_filters, populations_query = filter_utils.apply_filters(
                    filter_groups_data=filter_groups_data,
                    request_args=request_args,
                    the_query=populations_query,
                )

                # Sort
                populations_query = sort_utils.apply_sort(
                    list_columns=list_columns,
                    sort_index=sort_index,
                    sort_direction=sort_direction,
                    the_query=populations_query,
                )

                # Get total count separately for future pagination
                total_count = populations_query.count()
                # Get list of populations
                populations = populations_query.all()

                # If there are no populations, use an empty list
                if populations is None:
                    populations = []

                # TODO: Pagination
                # Render the list page, passing in the parameter values that are needed to
                # display the page as expected
                return self.render(
                    "population-list.html",
                    unsorted_query_string=unsorted_query_string,
                    sort_index=sort_index,
                    sort_direction=sort_direction,
                    referrer_base_url=referrer_base_url
                    if referrer_base_url is not None
                    else "",
                    total_count=total_count,
                    populations=populations,
                    list_columns=list_columns,
                    filter_groups_data=filter_groups_data,
                    active_filters=active_filters,
                    current_time=datetime.datetime.now(tz=datetime.timezone.utc),
                    alert_msg=alert_msg,
                )
        except (DataError, IntegrityError, OperationalError) as the_error:
            logger.error(
                "There was an error listing populations",
                query=populations_query,
                request_args=request_args,
                details=the_error,
            )
            alert_msg = f"There was an error listing populations: {the_error}"
        except Exception as e:
            logger.error(
                "There was an unexpected error listing populations",
                query=populations_query,
                request_args=request_args,
                details=e,
            )
            alert_msg = f"There was an unexpected error listing populations: {e}"

        # If there was an error trying to get population information, render the page as if
        # no population information was returned
        return self.render(
            "population-list.html",
            unsorted_query_string=unsorted_query_string,
            sort_index=sort_index,
            sort_direction=sort_direction,
            referrer_base_url=referrer_base_url
            if referrer_base_url is not None
            else "",
            total_count=None,
            populations=[],
            list_columns=list_columns,
            filter_groups_data=filter_groups_data,
            active_filters=active_filters,
            current_time=datetime.datetime.now(tz=datetime.timezone.utc),
            alert_msg=alert_msg,
        )

    @synchronize
    @tracer.wrap()
    async def population_edit(
        self,
        population: Optional[Populations],
        edit_type: str = "edit_existing",
        alert_msg: Optional[str] = None,
    ) -> str | Response:
        """
        Renders the page that allows editing of existing populations
        """
        new_pop = None
        if population is not None:
            if edit_type == "edit_new_clone":
                new_pop = self._clone_population(population)

            referrer_base_url = views.get_referrer_base_url()
            organization_id = population.organization_id

            m_repo = mono_repo.MavenMonoRepository()
            org_ros, org_tracks = await asyncio.gather(
                m_repo.get_non_ended_reimbursement_organization_settings_information_for_organization_id(
                    organization_id=organization_id
                ),
                m_repo.get_non_ended_track_information_for_organization_id(
                    organization_id=organization_id
                ),
            )

            return self.render(
                "population-edit.html",
                referrer_base_url=referrer_base_url
                if referrer_base_url is not None
                else "",
                population=population if not new_pop else new_pop,
                reimbursement_organization_settings=org_ros,
                client_tracks=org_tracks,
                current_time=datetime.datetime.now(tz=datetime.timezone.utc),
                edit_type=edit_type,
                alert_msg=alert_msg,
            )
        # If no population was provided, redirect the user to the list page
        return flask.redirect(
            views.add_original_base_url(
                flask.url_for("population.population_list", alert_msg=alert_msg)
            )
        )

    @tracer.wrap()
    def population_view(
        self, population: Optional[Populations], alert_msg: Optional[str] = None
    ) -> str | Response:
        """
        Renders the page to view the attributes of existing populations
        """
        if population is not None:
            referrer_base_url = views.get_referrer_base_url()
            return self.render(
                "population-view.html",
                referrer_base_url=referrer_base_url
                if referrer_base_url is not None
                else "",
                population=population,
                current_time=datetime.datetime.now(tz=datetime.timezone.utc),
                alert_msg=alert_msg,
            )
        # If no population was provided, redirect the user to the list page
        return flask.redirect(
            views.add_original_base_url(
                flask.url_for("population.population_list", alert_msg=alert_msg)
            )
        )

    @tracer.wrap()
    def population_create(
        self, organization_id: int, alert_msg: Optional[str] = None
    ) -> str:
        """
        Calls population_edit, passing in a new population for the specified organization
        """
        return self.population_edit(
            population=Populations(
                organization_id=organization_id,
                sub_pop_lookup_keys_csv="",
                sub_pop_lookup_map_json={},
            ),
            edit_type="edit_new",
            alert_msg=alert_msg,
        )

    @tracer.wrap()
    @flask_admin.expose("/save/", methods=("POST",))
    def population_save(self) -> Response:
        """
        Handles the population save endpoint. This is where data validation will be done,
        along with user redirection based on the user's previous input (button)
        """
        alert_msg = request.args.get("alert_msg", None)
        request_form = request.form
        edit_type: str | None = request_form.get("edit_type", None)
        population_id: int | None = request_form.get("population_id", None)
        organization_id: int | None = request_form.get("organization_id", None)
        sub_pop_lookup_keys_csv: str | None = request_form.get(
            "sub_pop_lookup_keys_csv", None
        )
        sub_pop_lookup_map_json: str | None = request_form.get(
            "sub_pop_lookup_map_json", None
        )
        advanced: str | None = request_form.get("advanced", None)
        activated_at: datetime.datetime | None = request_form.get("activated_at", None)
        deactivated_at: datetime.datetime | None = request_form.get(
            "deactivated_at", None
        )

        logger.info(
            "Saving population information",
            edit_type=edit_type,
            id=population_id,
            organization_id=organization_id,
            sub_pop_lookup_keys_csv=sub_pop_lookup_keys_csv,
            sub_pop_lookup_map_json=sub_pop_lookup_map_json,
            advanced=advanced,
            activated_at=activated_at,
            deactivated_at=deactivated_at,
        )

        try:
            with Session.begin() as session:
                if edit_type == "edit_existing":
                    # If we're editing an existing population, get the model to be updated
                    the_pop: Optional[Populations] = (
                        session.query(Populations)
                        .filter(Populations.id == population_id)
                        .one_or_none()
                    )
                else:  # edit_new or edit_new_clone
                    # If this is a new population, create a new model to hold the changes
                    the_pop = Populations()
                    session.add(the_pop)
                    the_pop.organization_id = organization_id
                    the_pop.sub_pop_lookup_keys_csv = ""
                    the_pop.sub_pop_lookup_map_json = {}

                # Disabled inputs are not sent, so we need to check if they actually exist to
                # determine if the values need to be set
                if sub_pop_lookup_keys_csv is not None:
                    the_pop.sub_pop_lookup_keys_csv = sub_pop_lookup_keys_csv
                if advanced is not None:
                    the_pop.advanced = advanced == "on"
                if activated_at is not None:
                    if activated_at != "":
                        the_pop.activated_at = activated_at
                    else:
                        the_pop.activated_at = None
                if deactivated_at is not None:
                    if deactivated_at != "":
                        the_pop.deactivated_at = deactivated_at
                    else:
                        the_pop.deactivated_at = None

                if edit_type.startswith("edit_new"):
                    # If this is a new population, flush the changes to be able to get the new
                    # population ID
                    session.flush()
                    population_id = the_pop.id

                # Sub-populations
                sub_pop_indices = self._get_sub_population_indices(request_form)
                # Handle deletions first in case new sub-populations cause conflicts due to
                # uniqueness constraints
                sub_pop_indices = self._delete_sub_populations(
                    session=session,
                    request_form=request_form,
                    population_id=population_id,
                    sub_pop_indices=sub_pop_indices,
                )
                # Make the updates to the sub-populations
                self._update_sub_populations(
                    session=session,
                    request_form=request_form,
                    population_id=population_id,
                    sub_pop_indices=sub_pop_indices,
                )

                if sub_pop_lookup_map_json is not None:
                    # Flush to write the latest sub-population to the DB
                    session.flush()
                    lookup_map = orjson.loads(sub_pop_lookup_map_json)
                    name_regex = re.compile(pattern=r"^<(.+) \(New\)>$")
                    self._update_lookup_map_for_new_sub_pops(
                        session=session,
                        lookup_map=lookup_map,
                        population_id=the_pop.id,
                        name_regex=name_regex,
                    )
                    the_pop.sub_pop_lookup_map_json = lookup_map
        except (DataError, IntegrityError, OperationalError) as the_error:
            logger.error(
                "There was an error saving population information",
                organization_id=organization_id,
                population_id=population_id,
                details=the_error,
            )
            alert_msg = f"There was an error saving populations: {the_error}"
        except Exception as e:
            logger.error(
                "There was an unexpected error saving population information",
                organization_id=organization_id,
                population_id=population_id,
                details=e,
            )
            alert_msg = f"There was an unexpected error saving populations: {e}"

        # Determine where to go next based on the button the user used to submit the form
        # By default, we will go back to the population list page
        next_page = flask.url_for("population.population_list", alert_msg=alert_msg)
        if request_form.get("_continue_editing", None):
            # If the user used "Continue Editing," redirect the user back to the population
            # edit page, specifying the population ID
            next_page = flask.url_for(
                "population.population_edit_by_population_id",
                population_id=population_id,
                alert_msg=alert_msg,
            )
        return flask.redirect(views.add_original_base_url(next_page))

    @tracer.wrap()
    @flask_admin.expose("/<int:population_id>/")
    def population_edit_by_population_id(self, population_id: int) -> str | Response:
        """
        This function gets the population for the given ID and presents the Edit
        page for the population. If there is no such population, present the user
        with the list of populations (index).
        """
        logger.info("Editing population information by ID", population_id=population_id)
        alert_msg = request.args.get("alert_msg", None)

        try:
            with Session.begin() as session:
                # Get the population for the given ID
                the_pop = (
                    session.query(Populations)
                    .filter(Populations.id == population_id)
                    .one_or_none()
                )
                if the_pop is None:
                    # The population for this ID was not found, redirect the user to the main list
                    return flask.redirect(
                        views.add_original_base_url(
                            flask.url_for(
                                "population.population_list", alert_msg=alert_msg
                            )
                        )
                    )
                return self.population_edit(population=the_pop, alert_msg=alert_msg)
        except (DataError, IntegrityError, OperationalError) as the_error:
            logger.error(
                "There was an error editing the population by ID",
                population_id=population_id,
                details=the_error,
            )
            alert_msg = f"There was an error editing the population by ID: {the_error}"
        except Exception as e:
            logger.error(
                "There was an unexpected error editing the population by ID",
                population_id=population_id,
                details=e,
            )
            alert_msg = (
                f"There was an unexpected error editing the population by ID: {e}"
            )
        # If there was an error getting the population, redirect the user to the list page
        return flask.redirect(
            views.add_original_base_url(
                flask.url_for("population.population_list", alert_msg=alert_msg)
            )
        )

    @tracer.wrap()
    @flask_admin.expose("/<int:population_id>/view")
    def population_view_by_population_id(self, population_id: int) -> str | Response:
        """
        This function gets the population for the given ID and presents the edit
        page for the population. If there is no such population, present the user
        with the list of populations (index).
        """
        logger.info(
            "Fetching population information by ID", population_id=population_id
        )
        alert_msg = request.args.get("alert_msg", None)

        try:
            with Session.begin() as session:
                # Get the population for the given ID
                the_pop = (
                    session.query(Populations)
                    .filter(Populations.id == population_id)
                    .one_or_none()
                )
                if the_pop is None:
                    # The population for this ID was not found, redirect the user to the main list
                    return flask.redirect(
                        views.add_original_base_url(
                            flask.url_for(
                                "population.population_list", alert_msg=alert_msg
                            )
                        )
                    )
                return self.population_view(population=the_pop)
        except (DataError, IntegrityError, OperationalError) as the_error:
            logger.error(
                "There was an error viewing the population by ID",
                population_id=population_id,
                details=the_error,
            )
            alert_msg = f"There was an error viewing the population by ID: {the_error}"
        except Exception as e:
            logger.error(
                "There was an unexpected error viewing the population by ID",
                population_id=population_id,
                details=e,
            )
            alert_msg = (
                f"There was an unexpected error viewing the population by ID: {e}"
            )
        # If there was an error getting the population, redirect the user to the list page
        return flask.redirect(
            views.add_original_base_url(
                flask.url_for("population.population_list", alert_msg=alert_msg)
            )
        )

    @tracer.wrap()
    @flask_admin.expose("/<int:population_id>/clone")
    def population_clone_by_population_id(self, population_id: int) -> str | Response:
        """
        This function gets the population for the given ID and presents the edit
        page for a clone of the population. If there is no such population, present
        the user with the list of populations (index).
        """
        logger.info("Cloning population information by ID", population_id=population_id)
        alert_msg = request.args.get("alert_msg", None)

        try:
            with Session.begin() as session:
                # Get the population for the given ID
                the_pop = (
                    session.query(Populations)
                    .filter(Populations.id == population_id)
                    .one_or_none()
                )
                if the_pop is None:
                    # The population for this ID was not found, redirect the user to the main list
                    return flask.redirect(
                        views.add_original_base_url(
                            flask.url_for(
                                "population.population_list", alert_msg=alert_msg
                            )
                        )
                    )
                return self.population_edit(
                    population=the_pop, edit_type="edit_new_clone"
                )
        except (DataError, IntegrityError, OperationalError) as the_error:
            logger.error(
                "There was an error cloning the population by ID",
                population_id=population_id,
                details=the_error,
            )
            alert_msg = f"There was an error cloning the population by ID: {the_error}"
        except Exception as e:
            logger.error(
                "There was an unexpected error cloning the population by ID",
                population_id=population_id,
                details=e,
            )
            alert_msg = (
                f"There was an unexpected error cloning the population by ID: {e}"
            )
        # If there was an error getting the population, redirect the user to the list page
        return flask.redirect(
            views.add_original_base_url(
                flask.url_for("population.population_list", alert_msg=alert_msg)
            )
        )

    @tracer.wrap()
    @flask_admin.expose("/organization/<int:organization_id>/")
    def population_edit_by_organization_id(
        self, organization_id: int
    ) -> str | Response:
        """
        This function gets the latest active population for the specified organization
        and presents the Edit page for the population. If there are no active populations,
        the function will use the most recently created population. If the organization has
        no populations associated with it at all, the function brings the user to a page
        to create a new population for the organization. If there is an error during this
        process, the function will present the user with the list of populations (index).
        """
        logger.info(
            "Editing population information by organization ID",
            organization_id=organization_id,
        )
        alert_msg = request.args.get("alert_msg", None)

        try:
            with Session.begin() as session:
                current_datetime_with_tz = datetime.datetime.now(
                    tz=datetime.timezone.utc
                )
                # Get the latest active population for the organization
                latest_pop_result = (
                    session.query(Populations)
                    .filter(
                        Populations.organization_id == organization_id,
                        and_(
                            Populations.activated_at != None,  # noqa: E711
                            Populations.activated_at <= current_datetime_with_tz,
                        ),
                        or_(
                            Populations.deactivated_at == None,  # noqa: E711
                            Populations.deactivated_at > current_datetime_with_tz,
                        ),
                    )
                    .order_by(Populations.activated_at.desc())
                    .limit(1)
                    .one_or_none()
                )

                if latest_pop_result is None:
                    # There is no active population, so just get the most recently created population.
                    # The idea is that this will either bring the user to a configured population that
                    # has not yet been activated, or the last deactivated population.
                    latest_pop_result = (
                        session.query(Populations)
                        .filter(
                            Populations.organization_id == organization_id,
                        )
                        .order_by(Populations.created_at.desc())
                        .limit(1)
                        .one_or_none()
                    )

                if latest_pop_result is not None:
                    # Bring the user to the edit page for this latest population result
                    return self.population_edit(
                        population=latest_pop_result, alert_msg=alert_msg
                    )

                # There are no populations, active or inactive, for this organization, so redirect
                # the user to the population creation page
                return self.population_create(
                    organization_id=organization_id, alert_msg=alert_msg
                )
        except (DataError, IntegrityError, OperationalError) as the_error:
            logger.error(
                "There was an error editing the population by organization ID",
                organization_id=organization_id,
                details=the_error,
            )
            alert_msg = f"There was an error viewing the population by organization ID: {the_error}"
        except Exception as e:
            logger.error(
                "There was an unexpected error editing the population by organization ID",
                organization_id=organization_id,
                details=e,
            )
            alert_msg = f"There was an unexpected error viewing the population by organization ID: {e}"

        # If there was an error getting the population, redirect the user to the list page
        return flask.redirect(
            views.add_original_base_url(
                flask.url_for("population.population_list", alert_msg=alert_msg)
            )
        )

    @tracer.wrap()
    @flask_admin.expose("/organization/<int:organization_id>/new/")
    def population_create_by_organization_id(
        self, organization_id: int
    ) -> str | Response:
        """
        This function handles the population creation endpoint for a specified organization.
        """
        logger.info(
            "Creating a new population",
            organization_id=organization_id,
        )
        alert_msg = request.args.get("alert_msg", None)

        try:
            return self.population_create(
                organization_id=organization_id, alert_msg=alert_msg
            )
        except Exception as e:
            logger.error(
                "There was an unexpected error creating a new population by organization ID",
                organization_id=organization_id,
                details=e,
            )
            alert_msg = f"There was an unexpected error creating a new population by organization ID: {e}"
            return flask.redirect(
                views.add_original_base_url(
                    flask.url_for("population.population_list", alert_msg=alert_msg)
                )
            )

    def get_empty_list_message(self) -> str:
        """
        This is the message displayed for an empty table. It is based on what is used
        in flask-admin.
        """
        return gettext("There are no items in the table.")

    def _get_sub_population_indices(self, request_form: dict) -> Set[str]:
        """
        This function gets the list of sub-population indices by checking the request form
        for patterns matching the sub-pop ID input.
        """
        sub_pop_indices = set()
        for key in request_form.keys():
            re_matches = re.match(pattern="^sub_populations-(\\d+)-id$", string=key)
            if re_matches:
                sub_pop_indices.add(re_matches.group(1))
        return sub_pop_indices

    def _delete_sub_populations(
        self,
        session: ORM_Session,
        request_form: dict,
        population_id: int,
        sub_pop_indices: Set[str],
    ) -> Set[str]:
        """
        This function handles the deletion of a sub-population. This logic is based
        on flask-admin behavior so that we can use the existing JavaScript.
        It returns the set of indices remaining after deletions are complete.
        """
        removed_sub_pop_indices = set()
        for sub_pop_index in sub_pop_indices:
            sub_pop_id: int | None = request_form.get(
                f"sub_populations-{sub_pop_index}-id", None
            )

            # If there is no ID value, it means that this is a new sub-population and
            # we can ignore these for the purposes of deletion since we will not receive
            # a request to delete a new sub-population that doesn't exist yet
            if sub_pop_id == "":
                continue

            # Check for a deletion request for the sub-population. The flask-admin JS
            # does this by setting an "on" value for an input with the name formatted
            # as indicated.
            del_sub_pop: str = request_form.get(
                f"del-sub_populations-{sub_pop_index}", "off"
            )
            if del_sub_pop == "on":
                logger.info(f"Deleting sub-population-{sub_pop_index} ({sub_pop_id})")
                the_sub_pop = (
                    session.query(SubPopulations)
                    .where(SubPopulations.id == sub_pop_id)
                    .one_or_none()
                )
                if the_sub_pop is None:
                    # If the indicated sub-population doesn't exist, nothing to
                    # delete so log and continue
                    logger.warning(
                        "Sub-population not found",
                        population_id=population_id,
                        sub_population_id=sub_pop_id,
                    )
                    continue
                # Delete the sub-population
                session.delete(the_sub_pop)
                removed_sub_pop_indices.add(sub_pop_index)

        return sub_pop_indices - removed_sub_pop_indices

    def _update_sub_populations(
        self,
        session: ORM_Session,
        request_form: dict,
        population_id: int,
        sub_pop_indices: Set[str],
    ) -> None:
        """
        This function handles sub-population updates via the request form. This logic is
        based on flask-admin behavior so that we can use the existing JavaScript.
        """
        for sub_pop_index in sub_pop_indices:
            sub_pop_id: int | None = request_form.get(
                f"sub_populations-{sub_pop_index}-id", None
            )
            logger.info(f"Updating sub-population-{sub_pop_index} ({sub_pop_id})")

            if sub_pop_id != "":
                # If an ID was passed in, get the sub-population
                the_sub_pop = (
                    session.query(SubPopulations)
                    .where(SubPopulations.id == sub_pop_id)
                    .one_or_none()
                )
            else:
                # If no ID was passed in, create a new sub-population and link it
                # to the current population
                the_sub_pop = SubPopulations()
                session.add(the_sub_pop)
                the_sub_pop.population_id = population_id

            if the_sub_pop is None:
                # If the sub-population was found or failed to be created, log
                # and continue
                logger.warning(
                    "Sub-population not found",
                    population_id=population_id,
                    sub_population_id=sub_pop_id,
                )
                continue

            # Disabled inputs are not sent, so we need to check if they actually exist to
            # determine if the values need to be set
            sub_pop_name: str | None = request_form.get(
                f"sub_populations-{sub_pop_index}-feature_set_name", None
            )
            if sub_pop_name is not None:
                the_sub_pop.feature_set_name = sub_pop_name

            # Process the sub-population details which includes the track and reimbursement
            # organization settings information
            sub_pop_details_str: str | None = request_form.get(
                f"sub_populations-{sub_pop_index}-feature_set_details_json", None
            )
            try:
                sub_pop_details = orjson.loads(sub_pop_details_str)
            except orjson.JSONDecodeError:
                sub_pop_details = None
            if sub_pop_details is None:
                sub_pop_details = {}

            logger.info(
                "Saving sub-population information",
                id=sub_pop_id,
                name=sub_pop_name,
                sub_pop_details_str=sub_pop_details_str,
                sub_pop_details=sub_pop_details,
            )

            # Reimbursement organization settings information
            sub_pop_ros: str | None = request_form.get(
                f"sub_populations-{sub_pop_index}-ros", None
            )
            if sub_pop_ros is not None:
                sub_pop_details["2"] = sub_pop_ros

            # Track information
            # Only need to process track information if the population hasn't been activated yet
            if request_form.get("was_activated", "off") == "off":
                num_tracks = int(request_form.get("track_count", "0"))
                sub_pop_tracks = []
                for i in range(num_tracks):
                    enabled_track = request_form.get(
                        f"sub_populations-{sub_pop_index}-track-{i}", None
                    )
                    if enabled_track is not None:
                        sub_pop_tracks.append(enabled_track)
                sub_pop_details["1"] = ",".join(
                    str(track_id) for track_id in sub_pop_tracks
                )

            the_sub_pop.feature_set_details_json = sub_pop_details

    def _update_lookup_map_for_new_sub_pops(
        self,
        session: Session,
        lookup_map: dict,
        population_id: int,
        name_regex: re.Pattern,
    ):
        """
        This function updates the lookup map by replacing the names of new
        sub-populations with their IDs. This is because new sub-populations
        will not have had an ID yet so the front end uses their names to
        identify them.
        """
        for key, value in lookup_map.items():
            if isinstance(value, dict):
                self._update_lookup_map_for_new_sub_pops(
                    session=session,
                    lookup_map=value,
                    population_id=population_id,
                    name_regex=name_regex,
                )
            elif isinstance(value, str):
                re_matches = name_regex.match(string=value)
                if re_matches is not None:
                    the_sub_pop = (
                        session.query(SubPopulations)
                        .filter(
                            and_(
                                SubPopulations.population_id == population_id,
                                SubPopulations.feature_set_name == re_matches.group(1),
                            )
                        )
                        .one_or_none()
                    )
                    if the_sub_pop is not None:
                        the_sub_pop_id = the_sub_pop.id
                        logger.info(
                            "Replacing sub-population name with ID",
                            name=value,
                            id=the_sub_pop_id,
                        )
                        lookup_map[key] = the_sub_pop_id
                    else:
                        logger.warning(
                            "Sub-population name not found",
                            name=value,
                        )

    def _update_lookup_map_for_cloned_sub_pops(
        self, sub_pop_lookup_map_json: dict, sub_pop_names: dict
    ):
        """
        Updates the sub_pop_lookup_map_json, replacing the sub-population IDs with the sub-population
        names using the sub_pop_names, which maps the IDs to the names. This allows the lookup map to
        be used for the cloned sub-populations.
        """
        for key, value in sub_pop_lookup_map_json.items():
            if isinstance(value, dict):
                sub_pop_lookup_map_json[
                    key
                ] = self._update_lookup_map_for_cloned_sub_pops(value, sub_pop_names)
            else:
                sub_pop_lookup_map_json[key] = sub_pop_names.get(value, None)
        return sub_pop_lookup_map_json

    def _clone_population(self, population: Populations) -> Populations:
        """
        This helper function creates a clone of the population and its sub-populations. It
        does not copy over the activation and deactivation dates as the cloned population
        should be treated as a completely new population.
        """
        new_sub_pops = []
        sub_pop_names = {}
        # Create sub-pop clones
        for sub_pop in population.sub_populations:
            sub_pop_names[sub_pop.id] = f"<{sub_pop.feature_set_name} (New)>"
            new_sub_pops.append(
                SubPopulations(
                    id=0,
                    feature_set_name=sub_pop.feature_set_name,
                    feature_set_details_json=sub_pop.feature_set_details_json,
                )
            )
        # Get a new sub-pop lookup map for the cloned sub-pops
        new_sub_pop_lookup_map_json = self._update_lookup_map_for_cloned_sub_pops(
            sub_pop_lookup_map_json=population.sub_pop_lookup_map_json,
            sub_pop_names=sub_pop_names,
        )
        # Create the population clone
        new_pop = Populations(
            id=0,
            organization_id=population.organization_id,
            sub_pop_lookup_keys_csv=population.sub_pop_lookup_keys_csv,
            sub_pop_lookup_map_json=new_sub_pop_lookup_map_json,
            advanced=True,
        )
        new_pop.sub_populations = new_sub_pops
        return new_pop
