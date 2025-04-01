import datetime

from ddtrace import tracer
from flask import flash, request
from flask_admin import expose
from mmlib.ops.log import getLogger

from app.admin.base.base_view import BaseViewWithSessionRollback
from app.admin.utils import grpc_service
from db.sqlalchemy.models.verification import Verifications
from db.sqlalchemy.models.verification_attempt import VerificationAttempts
from db.sqlalchemy.sqlalchemy_config import Session

logger = getLogger(__name__)


class UserVerificationView(BaseViewWithSessionRollback):
    @expose(url="/", methods=("GET",))
    def index(self):
        return self.render("user-verification-index.html")

    @expose(url="/<int:user_id>", methods=("GET",))
    def user_verification_dashboard(self, user_id):
        logger.info("Fetching verification information", user_id=user_id)

        with Session.begin() as session:
            # Get verifications
            all_verifications_for_user = (
                session.query(Verifications)
                .filter(Verifications.user_id == user_id)
                .all()
            )

            verification_attempts = (
                session.query(VerificationAttempts)
                .filter(VerificationAttempts.user_id == user_id)
                .order_by(VerificationAttempts.id.desc())
                .all()
            )

            # Filter down our member_versioned records to get the most recent one for a given verification id
            # We may have multiple member_versioned records for each verification record
            user_has_active_verification = False
            user_has_active_e9y_record = False
            verification_needs_attention = False

            for v in all_verifications_for_user:
                # Set default values
                v.member_id = None
                v.member_created_at = None
                v.member_updated_at = None
                v.active_e9y_record = None

                if not user_has_active_verification and not v.deactivated_at:
                    user_has_active_verification = True

                # Loop through the member_verifications attached to this verification and grab the most recent one
                max_mv_id = -1
                for mv in v.memberverifications_collection:
                    if mv.id >= max_mv_id:
                        max_mv_id = mv.id
                        v.member_id = mv.member_id
                        v.member_created_at = mv.created_at
                        v.member_updated_at = mv.updated_at
                        v.member_effective_range = mv.memberversioned.effective_range

                        # Check to see if we have an active e9y record for this verification
                        current_date = datetime.date.today()
                        effective_range_upper = mv.memberversioned.effective_range.upper
                        effective_range_lower = mv.memberversioned.effective_range.lower

                        # Case - effective range lower is before than our current date i.e. record not active yet
                        if (
                            effective_range_lower
                            and current_date < effective_range_lower
                        ):
                            v.active_e9y_record = False
                            continue
                        # Case - effective range upper is after our current date i.e. record has expired
                        if (
                            effective_range_upper
                            and current_date > effective_range_upper
                        ):
                            v.active_e9y_record = False
                            continue

                    v.active_e9y_record = True

                # See if we need to raise a status issue for this verification
                v.error_status = None
                # Raise an error if we have an active verification, but an inactive e9y record- that should be corrected
                if v.deactivated_at is None and not v.active_e9y_record and v.member_id:
                    v.error_status = "Needs to be deactivated- active verification but inactive e9y record"
                    verification_needs_attention = True

            # endregion verifications

            return self.render(
                "user-verifications.html",
                verification_attempts=verification_attempts,
                user_id=user_id,
                verifications=all_verifications_for_user,
                user_has_active_verification=user_has_active_verification,
                user_has_active_e9y_record=user_has_active_e9y_record,
                verification_needs_attention=verification_needs_attention,
            )

    @tracer.wrap()
    @expose(url="/<int:user_id>", methods=("POST",))
    def actions(self, **kwargs):
        user_id = kwargs["user_id"]
        verification_id = int(list(request.form.keys())[0])

        logger.info(
            "Deactivating verification",
            user_id=user_id,
            verification_id=verification_id,
        )
        try:
            e9y_svc = grpc_service.EligibilityServiceStub()
            e9y_svc.deactivate_verification(verification_id, user_id)
        except Exception as err:
            logger.error(
                "Error deactivating verification via admin",
                user_id=user_id,
                verification_id=verification_id,
            )
            flash(
                f"⚠️ Error deactivating verification for user_id {user_id}: {err}",
                category="error",
            )

        return self.user_verification_dashboard(user_id)
