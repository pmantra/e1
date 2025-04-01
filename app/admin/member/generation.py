import datetime
from typing import List

from flask import request
from flask_admin import expose
from mmlib.ops.log import getLogger
from tests.factories.data_models import (
    DateRangeFactory,
    Member2Factory,
    MemberVersionedFactory,
)

from app.admin.base.base_view import BaseViewWithSessionRollback
from app.utils import feature_flag
from db import model
from db.clients import member_2_client, member_versioned_client
from db.flask import synchronize
from db.sqlalchemy.models.configuration import Configurations
from db.sqlalchemy.sqlalchemy_config import Session

logger = getLogger(__name__)


class MemberGenerationView(BaseViewWithSessionRollback):
    @expose("/")
    def index(self):
        batch_options = [10, 20, 50, 100]
        with Session.begin() as session:
            orgs = (
                session.query(Configurations)
                .order_by(Configurations.organization_id.asc())
                .all()
            )
        return self.render(
            "member-generation-index.html", orgs=orgs, batch_options=batch_options
        )

    @synchronize
    @expose("/generate", methods=["POST"])
    async def generate(self):
        org_id: int = request.form.get("org_id")
        batch_size: int = request.form.get("batch_size", 10)
        mv_client = member_versioned_client.MembersVersioned()

        default_config = dict(
            organization_id=int(org_id),
            gender_code=None,
            do_not_contact=None,
            file_id=None,
            pre_verified=False,
            work_country=None,
            effective_range=DateRangeFactory.create(
                lower=datetime.date.today() - datetime.timedelta(days=1),
                upper=datetime.date.today() + datetime.timedelta(days=365),
            ),
        )

        members: List[model.MemberVersioned] = MemberVersionedFactory.create_batch(
            size=int(batch_size), **default_config
        )

        persisted: List[model.MemberVersioned] = await mv_client.bulk_persist(
            models=members
        )

        # Create member_2 records if ff write enabled
        persisted_member_2 = []
        if feature_flag.organization_enabled_for_e9y_2_write(int(org_id)):
            m2_client = member_2_client.Member2Client()
            member_2_list = []
            for mv in persisted:
                m_2 = Member2Factory.create(
                    id=mv.id + 1_000_000,
                    organization_id=int(org_id),
                    first_name=mv.first_name,
                    last_name=mv.last_name,
                    date_of_birth=mv.date_of_birth,
                    work_state=mv.work_state,
                    work_country=mv.work_country,
                    email=mv.email,
                    unique_corp_id=mv.unique_corp_id,
                    effective_range=mv.effective_range,
                    do_not_contact=mv.do_not_contact,
                    gender_code=mv.gender_code,
                    employer_assigned_id=mv.employer_assigned_id,
                    dependent_id=mv.dependent_id,
                )
                member_2_list.append(m_2)
            persisted_member_2: List[model.Member2] = await m2_client.bulk_persist(
                models=member_2_list
            )

        return self.render(
            "member-generation-generate.html",
            members=persisted,
            members_2=persisted_member_2,
        )
