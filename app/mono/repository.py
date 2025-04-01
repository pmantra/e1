from typing import Any, Dict, List

from db.mono import client as mono_client


class MavenMonoRepository:
    def __init__(
        self,
        context: Dict[str, Any] = {},
    ):
        """
        The repository class to abstract out the data layer from the business logic. This will allow
        us to replace the direct DB access currently used to get data from Mono's MySQL DB with a
        service to be developed in the future.
        """
        # The mysql_connector context is only used for pytests so that the DB client can have its
        # connector be replaced with the DB client fixture. In normal use cases, we can leave
        # the MavenMonoClient as None.
        connector = context.get("mysql_connector", None)
        self._mono_db_client = mono_client.MavenMonoClient(c=connector)

    async def get_non_ended_track_information_for_organization_id(
        self,
        organization_id: int,
    ) -> List[mono_client.FeatureInformation]:
        """
        Gets FeatureInformation for the client tracks of an organization. This would
        include the IDs and descriptors of each track available to the organization.
        """
        return await self._mono_db_client.get_non_ended_track_information_for_organization_id(
            organization_id=organization_id
        )

    async def get_non_ended_reimbursement_organization_settings_information_for_organization_id(
        self,
        organization_id: int,
    ) -> List[mono_client.FeatureInformation]:
        """
        Gets FeatureInformation for the reimbursement organization settings of an
        organization. This would include the IDs and descriptors of each one
        available to the organization.
        """
        return await self._mono_db_client.get_non_ended_reimbursement_organization_settings_information_for_organization_id(
            organization_id=organization_id
        )
