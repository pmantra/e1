import unittest
from datetime import datetime, timedelta

from db.model import BackfillMemberTrackEligibilityData
from db.mono.model import (
    MemberTrackBackfillError,
    MemberTrackBackfillRequest,
    MemberTrackBackfillResult,
)
from utils.backfill_member_track import (
    _found_best_match_with_verification_and_member,
    _found_best_match_with_verification_only,
)
from utils.backfill_mt_billing import _parse_csv_string


class TestFoundBestMatchWithVerificationAndMember(unittest.TestCase):
    def setUp(self):
        self.request = MemberTrackBackfillRequest(
            member_track_id=1,
            user_id=123,
            organization_id=456,
            organization_employee_id=789,
            created_at=datetime(2023, 1, 1),
        )

    def test_no_records(self):
        records = []
        result = _found_best_match_with_verification_and_member(records, self.request)
        self.assertIsInstance(result, MemberTrackBackfillError)

    def test_no_matching_organization_id(self):
        records = [
            BackfillMemberTrackEligibilityData(
                user_id=self.request.user_id,
                verification_id=1,
                verification_organization_id=self.request.organization_id + 1,
                verification_created_at=datetime(2022, 12, 31),
                member_id=1001,
                member_organization_id=self.request.organization_id + 1,
                member_created_at=datetime(2022, 12, 31),
            ),
        ]
        result = _found_best_match_with_verification_and_member(records, self.request)
        self.assertIsInstance(result, MemberTrackBackfillError)
        self.assertEqual(
            result.reason,
            "Match with verification and member: 1 records found, but no matching organization_id",
        )

    def test_single_matching_record(self):
        records = [
            BackfillMemberTrackEligibilityData(
                user_id=self.request.user_id,
                verification_id=1,
                verification_organization_id=self.request.organization_id,
                verification_created_at=datetime(2022, 12, 31),
                member_id=1001,
                member_organization_id=self.request.organization_id,
                member_created_at=datetime(2022, 12, 31),
            ),
        ]
        result = _found_best_match_with_verification_and_member(records, self.request)
        self.assertIsInstance(result, MemberTrackBackfillResult)
        self.assertEqual(result.e9y_verification_id, 1)
        self.assertEqual(result.e9y_member_id, 1001)

    def test_multiple_matching_records_has_before_track_created(self):
        records = [
            BackfillMemberTrackEligibilityData(
                user_id=self.request.user_id,
                verification_id=1,
                verification_organization_id=self.request.organization_id,
                verification_created_at=self.request.created_at - timedelta(days=100),
                member_id=1001,
                member_organization_id=self.request.organization_id,
                member_created_at=self.request.created_at - timedelta(days=100),
            ),
            BackfillMemberTrackEligibilityData(
                user_id=self.request.user_id,
                verification_id=2,
                verification_organization_id=self.request.organization_id,
                verification_created_at=self.request.created_at - timedelta(days=10),
                member_id=1002,
                member_organization_id=self.request.organization_id,
                member_created_at=self.request.created_at - timedelta(days=10),
            ),
            BackfillMemberTrackEligibilityData(
                user_id=self.request.user_id,
                verification_id=3,
                verification_organization_id=self.request.organization_id,
                verification_created_at=self.request.created_at + timedelta(days=1),
                member_id=1003,
                member_organization_id=self.request.organization_id,
                member_created_at=self.request.created_at + timedelta(days=1),
            ),
        ]
        result = _found_best_match_with_verification_and_member(records, self.request)
        self.assertIsInstance(result, MemberTrackBackfillResult)
        self.assertEqual(result.e9y_verification_id, 2)
        self.assertEqual(result.e9y_member_id, 1002)

    def test_multiple_matching_records_has_no_before_track_created(self):
        records = [
            BackfillMemberTrackEligibilityData(
                user_id=self.request.user_id,
                verification_id=1,
                verification_organization_id=self.request.organization_id,
                verification_created_at=self.request.created_at + timedelta(days=100),
                member_id=1001,
                member_organization_id=self.request.organization_id,
                member_created_at=self.request.created_at + timedelta(days=100),
            ),
            BackfillMemberTrackEligibilityData(
                user_id=self.request.user_id,
                verification_id=2,
                verification_organization_id=self.request.organization_id,
                verification_created_at=self.request.created_at + timedelta(days=10),
                member_id=1002,
                member_organization_id=self.request.organization_id,
                member_created_at=self.request.created_at + timedelta(days=10),
            ),
            BackfillMemberTrackEligibilityData(
                user_id=self.request.user_id,
                verification_id=3,
                verification_organization_id=self.request.organization_id,
                verification_created_at=self.request.created_at + timedelta(days=1),
                member_id=1003,
                member_organization_id=self.request.organization_id,
                member_created_at=self.request.created_at + timedelta(days=1),
            ),
        ]
        result = _found_best_match_with_verification_and_member(records, self.request)
        self.assertIsInstance(result, MemberTrackBackfillResult)
        self.assertEqual(result.e9y_verification_id, 3)
        self.assertEqual(result.e9y_member_id, 1003)


class TestFoundBestMatchWithVerificationOnly(unittest.TestCase):
    def setUp(self):
        self.request = MemberTrackBackfillRequest(
            member_track_id=1,
            user_id=123,
            organization_id=456,
            organization_employee_id=789,
            created_at=datetime(2023, 1, 1),
        )

    def test_no_records(self):
        records = []
        result = _found_best_match_with_verification_only(records, self.request)
        self.assertIsInstance(result, MemberTrackBackfillError)

    def test_no_matching_organization_id(self):
        records = [
            BackfillMemberTrackEligibilityData(
                user_id=self.request.user_id,
                verification_id=1,
                verification_organization_id=self.request.organization_id + 1,
                verification_created_at=datetime(2022, 12, 31),
            ),
        ]
        result = _found_best_match_with_verification_only(records, self.request)
        self.assertIsInstance(result, MemberTrackBackfillError)
        self.assertEqual(
            result.reason,
            "Match with verification only: 1 records found, but no matching organization_id",
        )

    def test_single_matching_record(self):
        records = [
            BackfillMemberTrackEligibilityData(
                user_id=self.request.user_id,
                verification_id=1,
                verification_organization_id=self.request.organization_id,
                verification_created_at=datetime(2022, 12, 31),
            ),
        ]
        result = _found_best_match_with_verification_only(records, self.request)
        self.assertIsInstance(result, MemberTrackBackfillResult)
        self.assertEqual(result.e9y_verification_id, 1)
        self.assertEqual(result.e9y_member_id, None)

    def test_multiple_matching_records_has_before_track_created(self):
        records = [
            BackfillMemberTrackEligibilityData(
                user_id=self.request.user_id,
                verification_id=1,
                verification_organization_id=self.request.organization_id,
                verification_created_at=self.request.created_at - timedelta(days=100),
            ),
            BackfillMemberTrackEligibilityData(
                user_id=self.request.user_id,
                verification_id=2,
                verification_organization_id=self.request.organization_id,
                verification_created_at=self.request.created_at - timedelta(days=10),
            ),
            BackfillMemberTrackEligibilityData(
                user_id=self.request.user_id,
                verification_id=3,
                verification_organization_id=self.request.organization_id,
                verification_created_at=self.request.created_at + timedelta(days=1),
            ),
        ]
        result = _found_best_match_with_verification_only(records, self.request)
        self.assertIsInstance(result, MemberTrackBackfillResult)
        self.assertEqual(result.e9y_verification_id, 2)
        self.assertEqual(result.e9y_member_id, None)

    def test_multiple_matching_records_has_no_before_track_created(self):
        records = [
            BackfillMemberTrackEligibilityData(
                user_id=self.request.user_id,
                verification_id=1,
                verification_organization_id=self.request.organization_id,
                verification_created_at=self.request.created_at + timedelta(days=100),
            ),
            BackfillMemberTrackEligibilityData(
                user_id=self.request.user_id,
                verification_id=2,
                verification_organization_id=self.request.organization_id,
                verification_created_at=self.request.created_at + timedelta(days=10),
            ),
            BackfillMemberTrackEligibilityData(
                user_id=self.request.user_id,
                verification_id=3,
                verification_organization_id=self.request.organization_id,
                verification_created_at=self.request.created_at + timedelta(days=1),
            ),
        ]
        result = _found_best_match_with_verification_only(records, self.request)
        self.assertIsInstance(result, MemberTrackBackfillResult)
        self.assertEqual(result.e9y_verification_id, 3)
        self.assertEqual(result.e9y_member_id, None)


class TestParseCsvString(unittest.TestCase):
    def test_valid_csv_string(self):
        csv_string = """member_track_id,eligibility_member_id,member_track_name,user_id,organization_id,organization_name,activated_at,ended_at,track_runtime_days
1,11,partner_newparent,111,1111,Procter and Gamble,2024-07-27 5:36:31,,
2,22,partner_newparent,222,2222,Procter and Gamble,2024-07-27 5:36:31,,
3,33,partner_newparent,333,3333,Procter and Gamble,2024-07-27 5:36:31,,
4,44,partner_newparent,444,4444,Procter and Gamble,2024-07-27 5:36:31,,
5,55,partner_newparent,555,5555,Procter and Gamble,2024-07-27 5:36:31,,
"""
        expected_result = [1, 2, 3, 4, 5]
        result = _parse_csv_string(csv_string)
        self.assertEqual(result, expected_result)

    def test_empty_csv_string(self):
        csv_string = ""
        expected_result = []
        result = _parse_csv_string(csv_string)
        self.assertEqual(result, expected_result)
