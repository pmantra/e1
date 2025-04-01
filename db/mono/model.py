from __future__ import annotations

import dataclasses
import datetime

# mono models for backfill


@dataclasses.dataclass
class CreditBackfillRequest:
    id: int  # for credit_id
    user_id: int
    organization_id: int
    organization_employee_id: int
    oe_e9y_member_id: int | None


@dataclasses.dataclass
class CreditBackfillResult:
    request: CreditBackfillRequest
    e9y_verification_id: int
    e9y_member_id: int

    @staticmethod
    def get_csv_cols():
        return [
            "credit_id",
            "user_id",
            "organization_id",
            "organization_employee_id",
            "e9y_verification_id",
            "e9y_member_id",
        ]

    def as_csv_dict(self):
        return {
            "credit_id": self.request.id,
            "user_id": self.request.user_id,
            "organization_id": self.request.organization_id,
            "organization_employee_id": self.request.organization_employee_id,
            "e9y_verification_id": self.e9y_verification_id,
            "e9y_member_id": self.e9y_member_id,
        }


@dataclasses.dataclass
class CreditBackfillError:
    request: CreditBackfillRequest
    reason: str = ""

    @staticmethod
    def get_csv_cols():
        return ["credit_id", "reason"]

    def as_csv_dict(self):
        return {"credit_id": self.request.id, "reason": self.reason}


@dataclasses.dataclass
class MemberTrackBackfillRequest:
    member_track_id: int
    user_id: int
    organization_employee_id: int
    created_at: datetime.datetime
    organization_id: int
    existing_e9y_verification_id: int | None = None
    existing_e9y_member_id: int | None = None


@dataclasses.dataclass
class MemberTrackBackfillResult:
    request: MemberTrackBackfillRequest
    e9y_verification_id: int
    e9y_member_id: int

    @staticmethod
    def get_csv_cols():
        return [
            "member_track_id",
            "user_id",
            "organization_employee_id",
            "organization_id",
            "existing_e9y_verification_id",
            "existing_e9y_member_id",
            "e9y_verification_id",
            "e9y_member_id",
        ]

    def as_csv_dict(self):
        return {
            "member_track_id": self.request.member_track_id,
            "user_id": self.request.user_id,
            "organization_employee_id": self.request.organization_employee_id,
            "organization_id": self.request.organization_id,
            "existing_e9y_verification_id": self.request.existing_e9y_verification_id,
            "existing_e9y_member_id": self.request.existing_e9y_member_id,
            "e9y_verification_id": self.e9y_verification_id,
            "e9y_member_id": self.e9y_member_id,
        }


@dataclasses.dataclass
class MemberTrackBackfillError:
    request: MemberTrackBackfillRequest
    reason: str = ""

    @staticmethod
    def get_csv_cols():
        return [
            "member_track_id",
            "user_id",
            "organization_employee_id",
            "organization_id",
            "existing_e9y_verification_id",
            "existing_e9y_member_id",
            "reason",
        ]

    def as_csv_dict(self):
        return {
            "member_track_id": self.request.member_track_id,
            "user_id": self.request.user_id,
            "organization_employee_id": self.request.organization_employee_id,
            "organization_id": self.request.organization_id,
            "existing_e9y_verification_id": self.request.existing_e9y_verification_id,
            "existing_e9y_member_id": self.request.existing_e9y_member_id,
            "reason": self.reason,
        }
