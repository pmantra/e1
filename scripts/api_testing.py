# This script runs e2e API testing locally for E9Y 2.0 project
# Instructions:
# - Enable LaunchDarkly feature flags (release-eligibility-2-enabled-orgs-for-read and release-eligibility-2-enabled-orgs-for-write) for the testing org
# - Run scripts/api_testing_cleanup.sql to reset DB for the testing member to the original state
# - Run port-forward: kubectl port-forward service/eligibility-api 50067:50051
# - Run this script: python scripts/api_testing.py -o <org_id>

import argparse
import pathlib
import sys

PROJECT_DIR = pathlib.Path(__file__).parent.parent
PROTOS = PROJECT_DIR / "api" / "protobufs" / "generated" / "python"

sys.path.append(str(PROTOS))

import grpc
from google.protobuf.wrappers_pb2 import Int64Value
from maven_schemas import eligibility_pb2 as e9ypb
from maven_schemas import eligibility_pb2_grpc as e9ygrpc


member_merge_671 = {
    "date_of_birth": "01/01/1991",
    "first_name": "merge_1_fn",
    "last_name": "merge_1_ln",
    "email": "merge_1@advpop.com",
    "unique_corp_id": "1001",
    "work_state": "GA",
    "member_2_id": 2881923055618,
    "member_2_version": 2,
    "member_1_id": 5378259,
    "organization_id": 671,
    "dependent_id": "",
    "user_id": 8086688,
    "oe_id": 53208888,
}

member_merge_748 = {
    "date_of_birth": "03/03/1993",
    "first_name": "merge_3_fn",
    "last_name": "merge_3_ln",
    "email": "",  # No email provided from Cigna
    "unique_corp_id": "U42231447",
    "work_state": "CA",
    "member_2_id": 3212635537415,
    "member_2_version": 1,
    "member_1_id": 5406183,
    "organization_id": 748,
    "dependent_id": "01",
    "user_id": 80866748,
    "oe_id": 53208748,
}

member_merge_834 = {
    "date_of_birth": "02/02/1992",
    "first_name": "bcbsma_merge_2_fn",
    "last_name": "bcbsma_merge_2_ln",
    "email": "ipaul@example.org",
    "unique_corp_id": "100002",
    "work_state": "",  # No work state from bcbsma
    "member_2_id": 3582002724865,
    "member_2_version": 2,
    "member_1_id": 5411751,
    "organization_id": 834,
    "dependent_id": "3",
    "user_id": 808661834,
    "oe_id": 53208834,
}

member_merge_669 = {
    "date_of_birth": "04/04/1994",
    "first_name": "optum_merge_4_fn",
    "last_name": "Wilcox",
    "email": "merge_4@optum.com",
    "unique_corp_id": "optum100004",
    "work_state": "FL",
    "member_2_id": 2873333121305,
    "member_2_version": 1,
    "member_1_id": 2975669,
    "organization_id": 669,
    "dependent_id": "dependent_id_3",
    "user_id": 808661669,
    "oe_id": 53208669,
}

member_merge_95 = {
    "date_of_birth": "02/27/1984",
    "first_name": "",
    "last_name": "",
    "email": "",
    "unique_corp_id": "299068",
    "work_state": "",
    "member_2_id": 0,
    "member_2_version": 0,
    "member_1_id": 0,
    "organization_id": 95,
    "dependent_id": "",
    "user_id": 80866195,
    "is_employee": True,
    "dependent_date_of_birth": "",
}

member_merge_767 = {
    "date_of_birth": "02/24/1989",
    "first_name": "Brooke",
    "last_name": "Mckay",
    "email": "jer+7670@e9yfileless.com",
    "unique_corp_id": "AUTOGEN229782d2b207cdf81efcb33a71423542ad27edfd",
    "work_state": "",
    "member_2_id": 0,
    "member_2_version": 0,
    "member_1_id": 0,
    "organization_id": 767,
    "dependent_id": "",
    "user_id": 808678605,
    "is_employee": True,
    "dependent_date_of_birth": "",
}

member_merge_715 = {
    "date_of_birth": "01/01/1991",
    "first_name": "amazon_merge_1_fn",
    "last_name": "amazon_merge_1_ln",
    "email": "merge_1@amazon.com",
    "unique_corp_id": "1001",
    "work_state": "NV",
    "member_2_id": 3070901676920,
    "member_2_version": 1,
    "member_1_id": 7434884,
    "organization_id": 715,
    "dependent_id": "",
    "user_id": 808789692,
    "oe_id": 53208888,
}

member_not_exists = {
    "date_of_birth": "06/23/1992",
    "first_name": "Alan",
    "last_name": "Turing",
    "email": "alan@gmail.com",
    "unique_corp_id": "100123454",
    "work_state": "HI",
    "member_2_id": 12345678,
    "member_2_version": 2,
    "member_1_id": 1234554321,
    "organization_id": 671,
    "dependent_id": "non",
    "user_id": 1122334455,
    "is_employee": True,
    "dependent_date_of_birth": "",
}


org_settings = {
    671: {
        "member_exists": member_merge_671,
        "member_not_exists": member_not_exists,
        "is_client_specific": False,
        "is_no_dob": False,
        "is_filess": False,
    },
    748: {
        "member_exists": member_merge_748,
        "member_not_exists": member_not_exists,
        "is_client_specific": False,
        "is_no_dob": False,
        "is_filess": False,
    },
    834: {
        "member_exists": member_merge_834,
        "member_not_exists": member_not_exists,
        "is_client_specific": False,
        "is_no_dob": False,
        "is_filess": False,
    },
    669: {
        "member_exists": member_merge_669,
        "member_not_exists": member_not_exists,
        "is_client_specific": False,
        "is_no_dob": False,
        "is_filess": False,
    },
    95: {
        "member_exists": member_merge_95,
        "member_not_exists": member_not_exists,
        "is_client_specific": True,
        "is_no_dob": False,
        "is_filess": False,
    },
    767: {
        "member_exists": member_merge_767,
        "member_not_exists": member_not_exists,
        "is_client_specific": False,
        "is_no_dob": False,
        "is_filess": True,
    },
    715: {
        "member_exists": member_merge_715,
        "member_not_exists": member_not_exists,
        "is_client_specific": False,
        "is_no_dob": False,
        "is_filess": False,
    },
}


class TestResults:
    def __init__(self) -> None:
        self.results = {}

    def add_result(self, func_name, is_successful, details, identifier=None):
        self.results[func_name + "_" + str(identifier)] = (is_successful, details)

    def print_results(self, only_failed=True):
        failed_cnt = total_cnt = 0
        for name, v in self.results.items():
            total_cnt += 1
            is_successful, details = v
            if not only_failed and is_successful:
                print(f"  --{name} successful: {details}")
            if not is_successful:
                failed_cnt += 1
                print(f"  --{name} failed: {details}")
        print("*" * 60 + " Test Summary " + "*" * 60)
        if failed_cnt == 0:
            print(f"All {total_cnt} tests passed.")
        else:
            print(f"{failed_cnt}/{total_cnt} tests failed.")


class APITesting:
    def __init__(self, organization_id) -> None:
        self.channel = grpc.insecure_channel("localhost:50067")
        self.stub = e9ygrpc.EligibilityServiceStub(self.channel)
        self.test_results = TestResults()
        self.organization_id = organization_id
        org_setting = org_settings[self.organization_id]
        self.member_exists = org_setting["member_exists"]
        self.member_not_exists = org_setting["member_not_exists"]
        self.member_not_exists["organization_id"] = organization_id
        self.is_client_specific = org_setting["is_client_specific"]
        self.is_no_dob = org_setting["is_no_dob"]
        self.is_fileless = org_setting["is_filess"]

    def run(self):
        print(f"Now running API testing for organization {self.organization_id}")
        # Checke Eligibility
        self.check_basic_elig_member_exists()
        self.check_basic_elig_member_non_exists()
        self.check_employee_elig_member_exists()
        self.check_employee_elig_member_non_exists()
        self.check_health_plan_elig_member_exists()
        self.check_health_plan_elig_member_non_exists()
        # Deprecated APIs
        # self.check_standard_elig_member_exists()
        # self.check_standard_elig_member_non_exists()
        # self.check_alter_elig_member_exists()
        # self.check_alter_elig_member_non_exists()
        # self.check_no_dob()
        # self.check_over_elig_exists()
        # self.check_over_elig_non_exists()
        self.check_client_specific_exists()
        self.check_client_specific_non_exists()

        # Get Member
        self.get_member_by_id_exists()
        self.get_member_by_id_non_exists()
        self.get_member_by_org_identity_exists()
        self.get_member_by_org_identity_non_exists()

        # Verification
        self.get_verification_for_user_non_exists(
            self.member_exists, "not_found"
        )  # initially no verification
        self.get_all_verifications_for_user_non_exists(
            self.member_exists, "not_found"
        )  # initially no verification
        self.create_verification_for_user_exists(self.member_exists)
        self.create_verification_for_user_non_exists(self.member_not_exists)
        self.get_verification_for_user_exists(
            self.member_exists, "found"
        )  # 2nd request to get should succeed
        self.get_all_verifications_for_user_exists(
            self.member_exists, "found"
        )  # 2nd request to get should succeed
        self.create_multiple_verifications_for_user_exists(self.member_exists)
        self.create_multiple_verifications_for_user_non_exists(self.member_not_exists)
        self.create_failed_verification()

        # Wallet Enablement
        self.get_wallet_enablement_by_user_id_exists()
        self.get_wallet_enablement_by_user_id_non_exists()

        # Eligible Features
        self.get_eligbile_features_for_user_exists()
        self.get_eligbile_features_for_user_non_exists()
        self.get_eligbile_features_for_user_and_org_exists()
        self.get_eligbile_features_for_user_and_org_non_exists()

        # Sub Population
        self.get_subpopulation_id_for_user_exists()
        self.get_subpopulation_id_for_user_non_exists()
        self.get_subpopulation_id_for_user_and_org_exists()
        self.get_subpopulation_id_for_user_and_org_non_exists()
        self.get_other_user_ids_in_family_exists()

        self.test_results.print_results(only_failed=True)

    def check_basic_elig_member_exists(self):
        if self.is_client_specific:
            return
        if self.is_fileless:
            return

        request = e9ypb.BasicEligibilityRequest(
            user_id=self.member_exists["user_id"],
            date_of_birth=self.member_exists["date_of_birth"],
            first_name=self.member_exists["first_name"],
            last_name=self.member_exists["last_name"],
        )
        try:
            member_list_response = self.stub.CheckBasicEligibility(request)
            if len(member_list_response.member_list) != 1:
                self.test_results.add_result(
                    "check_basic_elig_member_exists",
                    False,
                    f"Wrong response {member_list_response}",
                )
                return
            member_response = member_list_response.member_list[0]
            if (
                member_response.member_1_id != self.member_exists["member_1_id"]
                or member_response.member_2_id != self.member_exists["member_2_id"]
            ):
                self.test_results.add_result(
                    "check_basic_elig_member_exists",
                    False,
                    f"Member response has different fields: {member_response}",
                )
            else:
                self.test_results.add_result("check_basic_elig_member_exists", True, "")
        except grpc.RpcError as e:
            self.test_results.add_result(
                "check_basic_elig_member_exists", False, f"RpcError {e}"
            )
        except Exception as e:
            self.test_results.add_result(
                "check_basic_elig_member_exists",
                False,
                f"Unexpected exception: {e}",
            )

    def check_basic_elig_member_non_exists(self):
        if self.is_client_specific:
            return
        if self.is_fileless:
            return
        request = e9ypb.BasicEligibilityRequest(
            user_id=self.member_not_exists["user_id"],
            date_of_birth=self.member_not_exists["date_of_birth"],
            first_name=self.member_not_exists["first_name"],
            last_name=self.member_not_exists["last_name"],
        )
        try:
            member_list_response = self.stub.CheckBasicEligibility(request)
            self.test_results.add_result(
                "check_basic_elig_member_non_exists",
                False,
                f"Response returns unexpected member: {member_list_response}",
            )
        except grpc.RpcError as e:
            self.test_results.add_result(
                "check_basic_elig_member_non_exists",
                True,
                f"{e}",
            )
        except Exception as e:
            self.test_results.add_result(
                "check_basic_elig_member_non_exists",
                False,
                f"Unexpected exception: {e}",
            )

    def check_employee_elig_member_exists(self):
        if self.is_client_specific:
            return
        if self.is_fileless:
            return
        request = e9ypb.EmployerEligibilityRequest(
            first_name=self.member_exists["first_name"],
            last_name=self.member_exists["last_name"],
            date_of_birth=self.member_exists["date_of_birth"],
            dependent_date_of_birth="",
            company_email=self.member_exists["email"],
            work_state=self.member_exists["work_state"],
            employee_first_name="",
            employee_last_name="",
            user_id=self.member_exists["user_id"],
        )

        try:
            member_response = self.stub.CheckEmployerEligibility(request)
            if (
                member_response.member_1_id != self.member_exists["member_1_id"]
                or member_response.member_2_id != self.member_exists["member_2_id"]
            ):
                self.test_results.add_result(
                    "check_standard_elig_member_exists",
                    False,
                    f"Member response has different fields: {member_response}",
                )
            else:
                self.test_results.add_result(
                    "check_standard_elig_member_exists", True, ""
                )
        except grpc.RpcError as e:
            self.test_results.add_result(
                "check_standard_elig_member_exists", False, f"RpcError {e}"
            )
        except Exception as e:
            self.test_results.add_result(
                "check_standard_elig_member_exists",
                False,
                f"Unexpected exception: {e}",
            )

    def check_employee_elig_member_non_exists(self):
        if self.is_client_specific:
            return
        if self.is_fileless:
            return
        request = e9ypb.EmployerEligibilityRequest(
            first_name=self.member_not_exists["first_name"],
            last_name=self.member_not_exists["last_name"],
            date_of_birth=self.member_not_exists["date_of_birth"],
            dependent_date_of_birth="",
            company_email=self.member_not_exists["email"],
            work_state=self.member_not_exists["work_state"],
            employee_first_name="",
            employee_last_name="",
            user_id=self.member_not_exists["user_id"],
        )
        try:
            member_response = self.stub.CheckEmployerEligibility(request)
            self.test_results.add_result(
                "check_employee_elig_member_non_exists",
                False,
                f"Response returns unexpected member: {member_response}",
            )
        except grpc.RpcError as e:
            self.test_results.add_result(
                "check_employee_elig_member_non_exists", True, f"{e}"
            )
        except Exception as e:
            self.test_results.add_result(
                "check_employee_elig_member_non_exists",
                False,
                f"Unexpected exception: {e}",
            )

    def check_health_plan_elig_member_exists(self):
        if self.is_client_specific:
            return
        if self.is_fileless:
            return
        request = e9ypb.HealthPlanEligibilityRequest(
            first_name=self.member_exists["first_name"],
            last_name=self.member_exists["last_name"],
            date_of_birth=self.member_exists["date_of_birth"],
            dependent_date_of_birth="",
            subscriber_id=self.member_exists["unique_corp_id"],
            employee_first_name="",
            employee_last_name="",
            user_id=self.member_exists["user_id"],
        )

        try:
            member_response = self.stub.CheckHealthPlanEligibility(request)
            if (
                member_response.member_1_id != self.member_exists["member_1_id"]
                or member_response.member_2_id != self.member_exists["member_2_id"]
            ):
                self.test_results.add_result(
                    "check_health_plan_elig_member_exists",
                    False,
                    f"Member response has different fields: {member_response}",
                )
            else:
                self.test_results.add_result(
                    "check_health_plan_elig_member_exists", True, ""
                )
        except grpc.RpcError as e:
            self.test_results.add_result(
                "check_health_plan_elig_member_exists", False, f"RpcError {e}"
            )
        except Exception as e:
            self.test_results.add_result(
                "check_health_plan_elig_member_exists",
                False,
                f"Unexpected exception: {e}",
            )

    def check_health_plan_elig_member_non_exists(self):
        if self.is_client_specific:
            return
        if self.is_fileless:
            return
        request = e9ypb.HealthPlanEligibilityRequest(
            first_name=self.member_not_exists["first_name"],
            last_name=self.member_not_exists["last_name"],
            date_of_birth=self.member_not_exists["date_of_birth"],
            dependent_date_of_birth="",
            subscriber_id=self.member_not_exists["unique_corp_id"],
            employee_first_name="",
            employee_last_name="",
            user_id=self.member_not_exists["user_id"],
        )
        try:
            member_response = self.stub.CheckHealthPlanEligibility(request)
            self.test_results.add_result(
                "check_health_plan_elig_member_non_exists",
                False,
                f"Response returns unexpected member: {member_response}",
            )
        except grpc.RpcError as e:
            self.test_results.add_result(
                "check_health_plan_elig_member_non_exists", True, f"{e}"
            )
        except Exception as e:
            self.test_results.add_result(
                "check_health_plan_elig_member_non_exists",
                False,
                f"Unexpected exception: {e}",
            )

    def check_standard_elig_member_exists(self):
        if self.is_client_specific:
            return
        if self.is_fileless:
            return
        request = e9ypb.StandardEligibilityRequest(
            date_of_birth=self.member_exists["date_of_birth"],
            company_email=self.member_exists["email"],
        )
        try:
            member_response = self.stub.CheckStandardEligibility(request)
            if (
                member_response.member_1_id != self.member_exists["member_1_id"]
                or member_response.member_2_id != self.member_exists["member_2_id"]
            ):
                self.test_results.add_result(
                    "check_standard_elig_member_exists",
                    False,
                    f"Member response has different fields: {member_response}",
                )
            else:
                self.test_results.add_result(
                    "check_standard_elig_member_exists", True, ""
                )
        except grpc.RpcError as e:
            self.test_results.add_result(
                "check_standard_elig_member_exists", False, f"RpcError {e}"
            )
        except Exception as e:
            self.test_results.add_result(
                "check_standard_elig_member_exists",
                False,
                f"Unexpected exception: {e}",
            )

    def check_standard_elig_member_non_exists(self):
        if self.is_client_specific:
            return
        if self.is_fileless:
            return
        request = e9ypb.StandardEligibilityRequest(
            date_of_birth=self.member_not_exists["date_of_birth"],
            company_email=self.member_not_exists["email"],
        )
        try:
            member_response = self.stub.CheckStandardEligibility(request)
            self.test_results.add_result(
                "check_standard_elig_member_non_exists",
                False,
                f"Response returns unexpected member: {member_response}",
            )
        except grpc.RpcError as e:
            self.test_results.add_result(
                "check_standard_elig_member_non_exists", True, f"{e}"
            )
        except Exception as e:
            self.test_results.add_result(
                "check_standard_elig_member_non_exists",
                False,
                f"Unexpected exception: {e}",
            )

    def check_alter_elig_member_exists(self):
        if self.is_client_specific:
            return
        if self.is_fileless:
            return
        wo_unique_corp_id_request = e9ypb.AlternateEligibilityRequest(
            date_of_birth=self.member_exists["date_of_birth"],
            first_name=self.member_exists["first_name"],
            last_name=self.member_exists["last_name"],
            work_state=self.member_exists["work_state"],
        )
        try:
            member_response = self.stub.CheckAlternateEligibility(
                wo_unique_corp_id_request
            )
            if (
                member_response.member_1_id != self.member_exists["member_1_id"]
                or member_response.member_2_id != self.member_exists["member_2_id"]
            ):
                self.test_results.add_result(
                    "check_alter_elig_member_exists_no_corp",
                    False,
                    f"Member response has different fields: {member_response}",
                )
            else:
                self.test_results.add_result(
                    "check_alter_elig_member_exists_no_corp", True, f"{member_response}"
                )
        except grpc.RpcError as e:
            self.test_results.add_result(
                "check_alter_elig_member_exists_no_corp", False, f"RpcError {e}"
            )
        except Exception as e:
            self.test_results.add_result(
                "check_alter_elig_member_exists_no_corp",
                False,
                f"Unexpected exception: {e}",
            )

        w_unique_corp_id_request = e9ypb.AlternateEligibilityRequest(
            date_of_birth=self.member_exists["date_of_birth"],
            unique_corp_id=self.member_exists["unique_corp_id"],
        )
        try:
            member_response = self.stub.CheckAlternateEligibility(
                w_unique_corp_id_request
            )
            if (
                member_response.member_1_id != self.member_exists["member_1_id"]
                or member_response.member_2_id != self.member_exists["member_2_id"]
            ):
                self.test_results.add_result(
                    "check_alter_elig_member_exists_corp",
                    False,
                    f"Member response has different fields: {member_response}",
                )
            else:
                self.test_results.add_result(
                    "check_alter_elig_member_exists_corp", True, f"{member_response}"
                )
        except grpc.RpcError as e:
            self.test_results.add_result(
                "check_alter_elig_member_exists_corp", False, f"RpcError {e}"
            )
        except Exception as e:
            self.test_results.add_result(
                "check_alter_elig_member_exists_corp",
                False,
                f"Unexpected exception: {e}",
            )

    def check_alter_elig_member_non_exists(self):
        if self.is_client_specific:
            return
        if self.is_fileless:
            return
        wo_unique_corp_id_request = e9ypb.AlternateEligibilityRequest(
            date_of_birth=self.member_not_exists["date_of_birth"],
            first_name=self.member_not_exists["first_name"],
            last_name=self.member_not_exists["last_name"],
            work_state=self.member_not_exists["work_state"],
        )
        try:
            member_response = self.stub.CheckAlternateEligibility(
                wo_unique_corp_id_request
            )
            self.test_results.add_result(
                "check_alter_elig_member_non_exists_no_corp",
                False,
                f"Response returns unexpected member: {member_response}",
            )
        except grpc.RpcError as e:
            self.test_results.add_result(
                "check_alter_elig_member_non_exists_no_corp", True, f"{e}"
            )
        except Exception as e:
            self.test_results.add_result(
                "check_alter_elig_member_non_exists_no_corp",
                False,
                f"Unexpected exception: {e}",
            )

        w_unique_corp_id_request = e9ypb.AlternateEligibilityRequest(
            date_of_birth=self.member_not_exists["date_of_birth"],
            unique_corp_id=self.member_not_exists["unique_corp_id"],
        )
        try:
            member_response = self.stub.CheckAlternateEligibility(
                w_unique_corp_id_request
            )
            self.test_results.add_result(
                "check_alter_elig_member_non_exists_corp",
                False,
                f"Response returns unexpected member: {member_response}",
            )
        except grpc.RpcError as e:
            self.test_results.add_result(
                "check_alter_elig_member_non_exists_corp", True, f"{e}"
            )
        except Exception as e:
            self.test_results.add_result(
                "check_alter_elig_member_non_exists_corp",
                False,
                f"Unexpected exception: {e}",
            )

    def check_client_specific_exists(self):
        if not self.is_client_specific:
            return
        if self.is_fileless:
            return
        request = e9ypb.ClientSpecificEligibilityRequest(
            date_of_birth=self.member_exists["date_of_birth"],
            unique_corp_id=self.member_exists["unique_corp_id"],
            organization_id=self.member_exists["organization_id"],
            is_employee=self.member_exists["is_employee"],
            dependent_date_of_birth=self.member_exists["dependent_date_of_birth"],
        )
        try:
            member_response = self.stub.CheckClientSpecificEligibility(request)
            if member_response.unique_corp_id != self.member_exists["unique_corp_id"]:
                self.test_results.add_result(
                    "check_client_specific_exists",
                    False,
                    f"Member response has different fields: {member_response}",
                )
            else:
                self.test_results.add_result(
                    "check_client_specific_exists", True, f"{member_response}"
                )
        except grpc.RpcError as e:
            self.test_results.add_result(
                "check_client_specific_exists", False, f"RpcError {e}"
            )
        except Exception as e:
            self.test_results.add_result(
                "check_client_specific_exists",
                False,
                f"Unexpected exception: {e}",
            )

    def check_client_specific_non_exists(self):
        if not self.is_client_specific:
            return
        request = e9ypb.ClientSpecificEligibilityRequest(
            date_of_birth=self.member_not_exists["date_of_birth"],
            unique_corp_id=self.member_not_exists["unique_corp_id"],
            organization_id=self.member_not_exists["organization_id"],
            is_employee=self.member_not_exists["is_employee"],
            dependent_date_of_birth=self.member_not_exists["dependent_date_of_birth"],
        )
        try:
            member_response = self.stub.CheckClientSpecificEligibility(request)
            self.test_results.add_result(
                "check_client_specific_non_exists",
                False,
                f"Response returns unexpected member: {member_response}",
            )
        except grpc.RpcError as e:
            self.test_results.add_result(
                "check_client_specific_non_exists", True, f"RpcError {e}"
            )
        except Exception as e:
            self.test_results.add_result(
                "check_client_specific_non_exists",
                False,
                f"Unexpected exception: {e}",
            )

    def check_no_dob(self):
        if self.is_client_specific:
            return
        if self.is_fileless:
            return
        request = e9ypb.NoDOBEligibilityRequest(
            first_name=self.member_exists["first_name"],
            last_name=self.member_exists["last_name"],
            email=self.member_exists["email"],
        )
        try:
            member_response = self.stub.CheckNoDOBEligibility(request)
            self.test_results.add_result(
                "check_no_dob",
                False,
                f"Response returns unexpected member: {member_response}",
            )
        except grpc.RpcError as e:
            self.test_results.add_result("check_no_dob", True, f"{e}")
        except Exception as e:
            self.test_results.add_result(
                "check_no_dob",
                False,
                f"Unexpected exception: {e}",
            )

    def check_over_elig_exists(self):
        if self.is_client_specific:
            return
        if self.is_fileless:
            return
        request = e9ypb.EligibilityOverEligibilityRequest(
            date_of_birth=self.member_exists["date_of_birth"],
            first_name=self.member_exists["first_name"],
            last_name=self.member_exists["last_name"],
            user_id="0",
        )

        try:
            member_list_response = self.stub.CheckEligibilityOverEligibility(request)
            if len(member_list_response.member_list) != 1:
                self.test_results.add_result(
                    "check_over_elig_exists",
                    False,
                    f"Wrong response {member_list_response}",
                )
                return
            member_response = member_list_response.member_list[0]
            if (
                member_response.member_1_id != self.member_exists["member_1_id"]
                or member_response.member_2_id != self.member_exists["member_2_id"]
            ):
                self.test_results.add_result(
                    "check_over_elig_exists",
                    False,
                    f"Member response has different fields: {member_response}",
                )
            else:
                self.test_results.add_result("check_over_elig_exists", True, "")
        except grpc.RpcError as e:
            self.test_results.add_result(
                "check_over_elig_exists", False, f"RpcError {e}"
            )
        except Exception as e:
            self.test_results.add_result(
                "check_over_elig_exists",
                False,
                f"Unexpected exception: {e}",
            )

        email_request = e9ypb.EligibilityOverEligibilityRequest(
            date_of_birth=self.member_exists["date_of_birth"],
            first_name=self.member_exists["first_name"],
            last_name=self.member_exists["last_name"],
            user_id="0",
            company_email=self.member_exists["email"],
        )
        try:
            member_list_response = self.stub.CheckEligibilityOverEligibility(
                email_request
            )
            if len(member_list_response.member_list) != 1:
                self.test_results.add_result(
                    "check_over_elig_exists_email",
                    False,
                    f"Wrong response {member_list_response}",
                )
                return
            member_response = member_list_response.member_list[0]
            if (
                member_response.member_1_id != self.member_exists["member_1_id"]
                or member_response.member_2_id != self.member_exists["member_2_id"]
            ):
                self.test_results.add_result(
                    "check_over_elig_exists_email",
                    False,
                    f"Member response has different fields: {member_response}",
                )
            else:
                self.test_results.add_result("check_over_elig_exists_email", True, "")
        except grpc.RpcError as e:
            self.test_results.add_result(
                "check_over_elig_exists_email", False, f"RpcError {e}"
            )
        except Exception as e:
            self.test_results.add_result(
                "check_over_elig_exists_email",
                False,
                f"Unexpected exception: {e}",
            )

        # Email filtering only works if it's set in DB
        if len(self.member_exists["email"]) > 0:
            email_no_match_request = e9ypb.EligibilityOverEligibilityRequest(
                date_of_birth=self.member_exists["date_of_birth"],
                first_name=self.member_exists["first_name"],
                last_name=self.member_exists["last_name"],
                user_id="0",
                company_email=self.member_exists["email"] + "_no_match",
            )
            try:
                member_list_response = self.stub.CheckEligibilityOverEligibility(
                    email_no_match_request
                )
                self.test_results.add_result(
                    "check_over_elig_exists_email_no_match",
                    False,
                    f"Response returns unexpected member: {member_list_response}",
                )
            except grpc.RpcError as e:
                self.test_results.add_result(
                    "check_over_elig_exists_email_no_match",
                    True,
                    f"{e}",
                )
            except Exception as e:
                self.test_results.add_result(
                    "check_over_elig_exists_email_no_match",
                    False,
                    f"Unexpected exception: {e}",
                )

        corp_id_request = e9ypb.EligibilityOverEligibilityRequest(
            date_of_birth=self.member_exists["date_of_birth"],
            first_name=self.member_exists["first_name"],
            last_name=self.member_exists["last_name"],
            user_id="0",
            company_email=self.member_exists["email"],
            unique_corp_id=self.member_exists["unique_corp_id"],
        )
        try:
            member_list_response = self.stub.CheckEligibilityOverEligibility(
                corp_id_request
            )
            if len(member_list_response.member_list) != 1:
                self.test_results.add_result(
                    "check_over_elig_exists_corp_id",
                    False,
                    f"Wrong response {member_list_response}",
                )
                return
            member_response = member_list_response.member_list[0]
            if (
                member_response.member_1_id != self.member_exists["member_1_id"]
                or member_response.member_2_id != self.member_exists["member_2_id"]
            ):
                self.test_results.add_result(
                    "check_over_elig_exists_corp_id",
                    False,
                    f"Member response has different fields: {member_response}",
                )
            else:
                self.test_results.add_result("check_over_elig_exists_corp_id", True, "")
        except grpc.RpcError as e:
            self.test_results.add_result(
                "check_over_elig_exists_corp_id", False, f"RpcError {e}"
            )
        except Exception as e:
            self.test_results.add_result(
                "check_over_elig_exists_corp_id",
                False,
                f"Unexpected exception: {e}",
            )

    def check_over_elig_non_exists(self):
        if self.is_client_specific:
            return
        if self.is_fileless:
            return
        request = e9ypb.EligibilityOverEligibilityRequest(
            date_of_birth=self.member_not_exists["date_of_birth"],
            first_name=self.member_not_exists["first_name"],
            last_name=self.member_not_exists["last_name"],
            user_id="0",
        )
        try:
            member_list_response = self.stub.CheckEligibilityOverEligibility(request)
            self.test_results.add_result(
                "check_over_elig_non_exists",
                False,
                f"Response returns unexpected member: {member_list_response}",
            )
        except grpc.RpcError as e:
            self.test_results.add_result(
                "check_over_elig_non_exists",
                True,
                f"{e}",
            )
        except Exception as e:
            self.test_results.add_result(
                "check_over_elig_non_exists",
                False,
                f"Unexpected exception: {e}",
            )

    def get_member_by_id_exists(self):
        if self.is_client_specific:
            return
        if self.is_fileless:
            return
        request = e9ypb.MemberIdRequest(
            id=self.member_exists["member_1_id"],
        )
        try:
            member_response = self.stub.GetMemberById(request)
            if (
                member_response.member_1_id != self.member_exists["member_1_id"]
                or member_response.member_2_id != self.member_exists["member_2_id"]
            ):
                self.test_results.add_result(
                    "get_member_by_id_exists",
                    False,
                    f"Member response has different fields: {member_response}",
                )
            else:
                self.test_results.add_result("get_member_by_id_exists", True, "")
        except grpc.RpcError as e:
            self.test_results.add_result(
                "get_member_by_id_exists",
                False,
                f"{e}",
            )
        except Exception as e:
            self.test_results.add_result(
                "get_member_by_id_exists",
                False,
                f"Unexpected exception: {e}",
            )

    def get_member_by_id_non_exists(self):
        if self.is_client_specific:
            return
        if self.is_fileless:
            return
        request = e9ypb.MemberIdRequest(
            id=self.member_not_exists["member_1_id"],
        )
        try:
            member_response = self.stub.GetMemberById(request)
            self.test_results.add_result(
                "get_member_by_id_non_exists",
                False,
                f"Response returns unexpected member: {member_response}",
            )
        except grpc.RpcError as e:
            self.test_results.add_result(
                "get_member_by_id_non_exists",
                True,
                f"{e}",
            )
        except Exception as e:
            self.test_results.add_result(
                "get_member_by_id_non_exists",
                False,
                f"Unexpected exception: {e}",
            )

    def get_member_by_org_identity_exists(self):
        if self.is_client_specific:
            return
        if self.is_fileless:
            return
        request = e9ypb.OrgIdentityRequest(
            organization_id=self.member_exists["organization_id"],
            unique_corp_id=self.member_exists["unique_corp_id"],
            dependent_id=self.member_exists["dependent_id"],
        )
        try:
            member_response = self.stub.GetMemberByOrgIdentity(request)
            if (
                member_response.member_1_id != self.member_exists["member_1_id"]
                or member_response.member_2_id != self.member_exists["member_2_id"]
            ):
                self.test_results.add_result(
                    "get_member_by_org_identity_exists",
                    False,
                    f"Member response has different fields: {member_response}",
                )
            else:
                self.test_results.add_result(
                    "get_member_by_org_identity_exists", True, ""
                )
        except grpc.RpcError as e:
            self.test_results.add_result(
                "get_member_by_org_identity_exists",
                False,
                f"{e}",
            )
        except Exception as e:
            self.test_results.add_result(
                "get_member_by_org_identity_exists",
                False,
                f"Unexpected exception: {e}",
            )

    def get_member_by_org_identity_non_exists(self):
        if self.is_client_specific:
            return
        if self.is_fileless:
            return
        request = e9ypb.OrgIdentityRequest(
            organization_id=self.member_not_exists["organization_id"],
            unique_corp_id=self.member_not_exists["unique_corp_id"],
            dependent_id=self.member_not_exists["dependent_id"],
        )
        try:
            member_response = self.stub.GetMemberByOrgIdentity(request)
            self.test_results.add_result(
                "get_member_by_org_identity_non_exists",
                False,
                f"Response returns unexpected member: {member_response}",
            )
        except grpc.RpcError as e:
            self.test_results.add_result(
                "get_member_by_org_identity_non_exists",
                True,
                f"{e}",
            )
        except Exception as e:
            self.test_results.add_result(
                "get_member_by_org_identity_non_exists",
                False,
                f"Unexpected exception: {e}",
            )

    def get_wallet_enablement_by_user_id_exists(self):
        if self.is_client_specific:
            return
        if self.is_fileless:
            return
        request = e9ypb.UserIdRequest(
            id=self.member_exists["user_id"],
        )
        try:
            enablement_response = self.stub.GetWalletEnablementByUserId(request)
            if (
                enablement_response.member_id != self.member_exists["member_1_id"]
                or enablement_response.member_2_id != self.member_exists["member_2_id"]
            ):
                self.test_results.add_result(
                    "get_wallet_enablement_by_user_id_exists",
                    False,
                    f"Wallet Enablement response has different fields: {enablement_response}",
                )
            else:
                self.test_results.add_result(
                    "get_wallet_enablement_by_user_id_exists", True, ""
                )
        except grpc.RpcError as e:
            self.test_results.add_result(
                "get_wallet_enablement_by_user_id_exists",
                False,
                f"{e}",
            )
        except Exception as e:
            self.test_results.add_result(
                "get_wallet_enablement_by_user_id_exists",
                False,
                f"Unexpected exception: {e}",
            )

    def get_wallet_enablement_by_user_id_non_exists(self):
        if self.is_client_specific:
            return
        if self.is_fileless:
            return
        request = e9ypb.UserIdRequest(
            id=self.member_not_exists["user_id"],
        )
        try:
            enablement_response = self.stub.GetWalletEnablementByUserId(request)
            self.test_results.add_result(
                "get_wallet_enablement_by_user_id_non_exists",
                False,
                f"Response returns unexpected verification: {enablement_response}",
            )
        except grpc.RpcError as e:
            self.test_results.add_result(
                "get_wallet_enablement_by_user_id_non_exists",
                True,
                f"RpcError {e}",
            )
        except Exception as e:
            self.test_results.add_result(
                "get_wallet_enablement_by_user_id_non_exists",
                False,
                f"Unexpected exception: {e}",
            )

    def get_verification_for_user_exists(self, member, identifier=None):
        request = e9ypb.GetVerificationForUserRequest(
            user_id=member["user_id"],
            organization_id=str(member["organization_id"]),
            active_verifications_only=True,
        )
        try:
            verification_response = self.stub.GetVerificationForUser(request)
            if (
                verification_response.user_id != member["user_id"]
                or verification_response.organization_id != member["organization_id"]
                or verification_response.verification_2_id is None
            ):
                self.test_results.add_result(
                    "get_verification_for_user_exists",
                    False,
                    f"Verification response has different fields: {verification_response}",
                    identifier,
                )
            else:
                self.test_results.add_result(
                    "get_verification_for_user_exists", True, "", identifier
                )
        except grpc.RpcError as e:
            self.test_results.add_result(
                "get_verification_for_user_exists", False, f"RpcError {e}", identifier
            )
        except Exception as e:
            self.test_results.add_result(
                "get_verification_for_user_exists",
                False,
                f"Unexpected exception: {e}",
                identifier,
            )

    def get_verification_for_user_non_exists(self, member, identifier=None):
        request = e9ypb.GetVerificationForUserRequest(
            user_id=member["user_id"],
            organization_id=str(member["organization_id"]),
            active_verifications_only=True,
        )
        try:
            verification_response = self.stub.GetVerificationForUser(request)
            self.test_results.add_result(
                "get_verification_for_user_non_exists",
                False,
                f"Response returns unexpected verification: {verification_response}",
                identifier,
            )
        except grpc.RpcError as e:
            self.test_results.add_result(
                "get_verification_for_user_non_exists",
                True,
                f"RpcError {e}",
                identifier,
            )
        except Exception as e:
            self.test_results.add_result(
                "get_verification_for_user_non_exists",
                False,
                f"Unexpected exception: {e}",
                identifier,
            )

    def get_all_verifications_for_user_exists(self, member, identifier=None):
        request = e9ypb.GetAllVerificationsForUserRequest(
            user_id=member["user_id"],
            organization_ids=[member["organization_id"]],
            active_verifications_only=True,
        )
        try:
            verification_list_response = self.stub.GetAllVerificationsForUser(request)
            if len(verification_list_response.verification_list) != 1:
                self.test_results.add_result(
                    "get_all_verifications_for_user_exists",
                    False,
                    f"Wrong response: {verification_list_response}",
                    identifier,
                )
                return

            verification_response = verification_list_response.verification_list[0]

            if (
                verification_response.user_id != member["user_id"]
                or verification_response.organization_id != member["organization_id"]
                or verification_response.verification_2_id is None
            ):
                self.test_results.add_result(
                    "get_all_verifications_for_user_exists",
                    False,
                    f"Verification response has different fields: {verification_response}",
                    identifier,
                )
            else:
                self.test_results.add_result(
                    "get_all_verifications_for_user_exists", True, "", identifier
                )
        except grpc.RpcError as e:
            self.test_results.add_result(
                "get_all_verifications_for_user_exists",
                False,
                f"RpcError {e}",
                identifier,
            )
        except Exception as e:
            self.test_results.add_result(
                "get_all_verifications_for_user_exists",
                False,
                f"Unexpected exception: {e}",
                identifier,
            )

    def get_all_verifications_for_user_non_exists(self, member, identifier=None):
        request = e9ypb.GetAllVerificationsForUserRequest(
            user_id=member["user_id"],
            organization_ids=[member["organization_id"]],
            active_verifications_only=True,
        )
        try:
            verification_list_response = self.stub.GetAllVerificationsForUser(request)
            self.test_results.add_result(
                "get_all_verifications_for_user_non_exists",
                False,
                f"Response returns unexpected list of verifications: {verification_list_response}",
                identifier,
            )
        except grpc.RpcError as e:
            self.test_results.add_result(
                "get_all_verifications_for_user_non_exists",
                True,
                f"RpcError {e}",
                identifier,
            )
        except Exception as e:
            self.test_results.add_result(
                "get_all_verifications_for_user_non_exists",
                False,
                f"Unexpected exception: {e}",
                identifier,
            )

    def create_verification_for_user_exists(self, member):
        verification_type = "STANDARD"
        if self.is_client_specific:
            verification_type = "CLIENT_SPECIFIC"
        elif self.is_fileless:
            verification_type = "FILELESS"
        request = e9ypb.CreateVerificationForUserRequest(
            user_id=str(member["user_id"]),
            eligibility_member_id=str(member["member_1_id"]),
            organization_id=member["organization_id"],
            verification_type=verification_type,
            unique_corp_id=member["unique_corp_id"],
            dependent_id=member["dependent_id"],
            first_name=member["first_name"],
            last_name=member["last_name"],
            date_of_birth=member["date_of_birth"],
            email=member["email"],
            work_state=member["work_state"],
        )
        try:
            verification_response = self.stub.CreateVerificationForUser(request)
            if (
                verification_response.user_id != member["user_id"]
                or verification_response.organization_id != member["organization_id"]
                or verification_response.verification_2_id is None
            ):
                self.test_results.add_result(
                    "create_verification_for_user_exists",
                    False,
                    f"Verification response has different fields: {verification_response}",
                )
            else:
                self.test_results.add_result(
                    "create_verification_for_user_exists", True, ""
                )
        except grpc.RpcError as e:
            self.test_results.add_result(
                "create_verification_for_user_exists", False, f"RpcError {e}"
            )
        except Exception as e:
            self.test_results.add_result(
                "create_verification_for_user_exists",
                False,
                f"Unexpected exception: {e}",
            )

    def create_verification_for_user_non_exists(self, member):
        verification_type = "STANDARD"
        if self.is_client_specific:
            verification_type = "CLIENT_SPECIFIC"
        elif self.is_fileless:
            verification_type = "FILELESS"
        request = e9ypb.CreateVerificationForUserRequest(
            user_id=str(member["user_id"]),
            eligibility_member_id=str(member["member_1_id"]),
            organization_id=member["organization_id"],
            verification_type=verification_type,
            unique_corp_id=member["unique_corp_id"],
            dependent_id=member["dependent_id"],
            first_name=member["first_name"],
            last_name=member["last_name"],
            date_of_birth=member["date_of_birth"],
            email=member["email"],
            work_state=member["work_state"],
        )
        try:
            verification_response = self.stub.CreateVerificationForUser(request)
            self.test_results.add_result(
                "create_verification_for_user_non_exists",
                False,
                f"Response returns unexpected verification: {verification_response}",
            )
        except grpc.RpcError as e:
            self.test_results.add_result(
                "create_verification_for_user_non_exists", True, f"RpcError {e}"
            )
        except Exception as e:
            self.test_results.add_result(
                "create_verification_for_user_non_exists",
                False,
                f"Unexpected exception: {e}",
            )

    def create_multiple_verifications_for_user_exists(self, member):
        if self.is_client_specific:
            return
        if self.is_fileless:
            return
        eligibility_member_id = Int64Value()
        eligibility_member_id.value = member["member_1_id"]
        verification_data = e9ypb.VerificationData(
            eligibility_member_id=eligibility_member_id,
            organization_id=member["organization_id"],
            unique_corp_id=member["unique_corp_id"],
            dependent_id=member["dependent_id"],
            email=member["email"],
            work_state=member["work_state"],
            member_1_id=member["member_1_id"],
            member_2_id=member["member_2_id"],
            member_2_version=member["member_2_version"],
        )
        user_id = Int64Value()
        user_id.value = member["user_id"]
        request = e9ypb.CreateMultipleVerificationsForUserRequest(
            user_id=user_id,
            verification_data_list=[verification_data],
            verification_type="STANDARD",
            first_name=member["first_name"],
            last_name=member["last_name"],
            date_of_birth=member["date_of_birth"],
        )

        try:
            verification_list_response = self.stub.CreateMultipleVerificationsForUser(
                request
            )
            if len(verification_list_response.verification_list) != 1:
                self.test_results.add_result(
                    "create_multiple_verifications_for_user_exists",
                    False,
                    f"Wrong response: {verification_list_response}",
                )
                return

            verification_response = verification_list_response.verification_list[0]
            if (
                verification_response.user_id != member["user_id"]
                or verification_response.organization_id != member["organization_id"]
                or verification_response.verification_2_id is None
            ):
                self.test_results.add_result(
                    "create_multiple_verifications_for_user_exists",
                    False,
                    f"Verification response has different fields: {verification_response}",
                )
            else:
                self.test_results.add_result(
                    "create_multiple_verifications_for_user_exists", True, ""
                )
        except grpc.RpcError as e:
            self.test_results.add_result(
                "create_multiple_verifications_for_user_exists", False, f"RpcError {e}"
            )
        except Exception as e:
            self.test_results.add_result(
                "create_multiple_verifications_for_user_exists",
                False,
                f"Unexpected exception: {e}",
            )

    def create_multiple_verifications_for_user_non_exists(self, member):
        if self.is_client_specific:
            return
        if self.is_fileless:
            return
        eligibility_member_id = Int64Value()
        eligibility_member_id.value = member["member_1_id"]
        verification_data = e9ypb.VerificationData(
            eligibility_member_id=eligibility_member_id,
            organization_id=member["organization_id"],
            unique_corp_id=member["unique_corp_id"],
            dependent_id=member["dependent_id"],
            email=member["email"],
            work_state=member["work_state"],
            member_1_id=member["member_1_id"],
            member_2_id=member["member_2_id"],
            member_2_version=member["member_2_version"],
        )
        user_id = Int64Value()
        user_id.value = member["user_id"]
        request = e9ypb.CreateMultipleVerificationsForUserRequest(
            user_id=user_id,
            verification_data_list=[verification_data],
            verification_type="STANDARD",
            first_name=member["first_name"],
            last_name=member["last_name"],
            date_of_birth=member["date_of_birth"],
        )
        try:
            verification_list_response = self.stub.CreateMultipleVerificationsForUser(
                request
            )
            self.test_results.add_result(
                "create_multiple_verifications_for_user_non_exists",
                False,
                f"Response returns unexpected list of verifications: {verification_list_response}",
            )
        except grpc.RpcError as e:
            self.test_results.add_result(
                "create_multiple_verifications_for_user_non_exists",
                True,
                f"RpcError {e}",
            )
        except Exception as e:
            self.test_results.add_result(
                "create_multiple_verifications_for_user_non_exists",
                False,
                f"Unexpected exception: {e}",
            )

    def create_failed_verification(self):
        verification_type = "STANDARD"
        if self.is_client_specific:
            verification_type = "CLIENT_SPECIFIC"
        elif self.is_fileless:
            verification_type = "FILELESS"
        request_no_org = e9ypb.CreateFailedVerificationRequest(
            user_id=str(self.member_exists["user_id"]),
            verification_type=verification_type,
            date_of_birth=self.member_exists["date_of_birth"],
            unique_corp_id=self.member_exists["unique_corp_id"],
            dependent_id=self.member_exists["dependent_id"],
            first_name=self.member_exists["first_name"],
            last_name=self.member_exists["last_name"],
            email=self.member_exists["email"],
            work_state=self.member_exists["work_state"],
            eligibility_member_id=str(self.member_exists["member_1_id"]),
            organization_id="",
        )
        try:
            verification_attempt_response = self.stub.CreateFailedVerification(
                request_no_org
            )
            if verification_attempt_response.user_id != str(
                self.member_exists["user_id"]
            ):
                self.test_results.add_result(
                    "create_failed_verification_no_org",
                    False,
                    f"Verification attempt response has different fields: {verification_attempt_response}",
                )
            else:
                self.test_results.add_result(
                    "create_failed_verification_no_org", True, ""
                )
        except grpc.RpcError as e:
            self.test_results.add_result(
                "create_failed_verification_no_org", False, f"RpcError {e}"
            )
        except Exception as e:
            self.test_results.add_result(
                "create_failed_verification_no_org",
                False,
                f"Unexpected exception: {e}",
            )

        request_org = e9ypb.CreateFailedVerificationRequest(
            user_id=str(self.member_exists["user_id"]),
            verification_type=verification_type,
            date_of_birth=self.member_exists["date_of_birth"],
            unique_corp_id=self.member_exists["unique_corp_id"],
            dependent_id=self.member_exists["dependent_id"],
            first_name=self.member_exists["first_name"],
            last_name=self.member_exists["last_name"],
            email=self.member_exists["email"],
            work_state=self.member_exists["work_state"],
            eligibility_member_id=str(self.member_exists["member_1_id"]),
            organization_id=str(self.member_exists["organization_id"]),
        )
        try:
            verification_attempt_response = self.stub.CreateFailedVerification(
                request_org
            )
            if (
                verification_attempt_response.user_id
                != str(self.member_exists["user_id"])
                or verification_attempt_response.organization_id
                != str(self.member_exists["organization_id"])
                or not verification_attempt_response.is_v2
            ):
                self.test_results.add_result(
                    "create_failed_verification_org",
                    False,
                    f"Verification attempt response has different fields: {verification_attempt_response}",
                )
            else:
                self.test_results.add_result("create_failed_verification_org", True, "")
        except grpc.RpcError as e:
            self.test_results.add_result(
                "create_failed_verification_org", False, f"RpcError {e}"
            )
        except Exception as e:
            self.test_results.add_result(
                "create_failed_verification_org",
                False,
                f"Unexpected exception: {e}",
            )

    def get_eligbile_features_for_user_exists(self):
        request = e9ypb.GetEligibleFeaturesForUserRequest(
            user_id=self.member_exists["user_id"],
            feature_type=1,
        )
        try:
            features_response = self.stub.GetEligibleFeaturesForUser(request)
            if features_response.has_population:
                self.test_results.add_result(
                    "get_eligbile_features_for_user_exists", True, ""
                )
            else:
                self.test_results.add_result(
                    "get_eligbile_features_for_user_exists",
                    False,
                    f"No features returned: {features_response}",
                )
        except grpc.RpcError as e:
            self.test_results.add_result(
                "get_eligbile_features_for_user_exists", False, f"RpcError {e}"
            )
        except Exception as e:
            self.test_results.add_result(
                "get_eligbile_features_for_user_exists",
                False,
                f"Unexpected exception: {e}",
            )

    def get_eligbile_features_for_user_non_exists(self):
        request = e9ypb.GetEligibleFeaturesForUserRequest(
            user_id=self.member_not_exists["user_id"],
            feature_type=1,
        )
        try:
            features_response = self.stub.GetEligibleFeaturesForUser(request)
            if not features_response.has_population:
                self.test_results.add_result(
                    "get_eligbile_features_for_user_non_exists", True, ""
                )
            else:
                self.test_results.add_result(
                    "get_eligbile_features_for_user_non_exists",
                    False,
                    f"Unexpected features returned: {features_response}",
                )
        except grpc.RpcError as e:
            self.test_results.add_result(
                "get_eligbile_features_for_user_non_exists", False, f"RpcError {e}"
            )
        except Exception as e:
            self.test_results.add_result(
                "get_eligbile_features_for_user_non_exists",
                False,
                f"Unexpected exception: {e}",
            )

    def get_eligbile_features_for_user_and_org_exists(self):
        request = e9ypb.GetEligibleFeaturesForUserAndOrgRequest(
            user_id=self.member_exists["user_id"],
            organization_id=self.member_exists["organization_id"],
            feature_type=1,
        )
        try:
            features_response = self.stub.GetEligibleFeaturesForUserAndOrg(request)
            if features_response.has_population:
                self.test_results.add_result(
                    "get_eligbile_features_for_user_and_org_exists", True, ""
                )
            else:
                self.test_results.add_result(
                    "get_eligbile_features_for_user_and_org_exists",
                    False,
                    f"No features returned: {features_response}",
                )
        except grpc.RpcError as e:
            self.test_results.add_result(
                "get_eligbile_features_for_user_and_org_exists", False, f"RpcError {e}"
            )
        except Exception as e:
            self.test_results.add_result(
                "get_eligbile_features_for_user_and_org_exists",
                False,
                f"Unexpected exception: {e}",
            )

    def get_eligbile_features_for_user_and_org_non_exists(self):
        request = e9ypb.GetEligibleFeaturesForUserAndOrgRequest(
            user_id=self.member_not_exists["user_id"],
            organization_id=self.member_not_exists["organization_id"],
            feature_type=1,
        )
        try:
            features_response = self.stub.GetEligibleFeaturesForUserAndOrg(request)
            if not features_response.has_population:
                self.test_results.add_result(
                    "get_eligbile_features_for_user_and_org_non_exists", True, ""
                )
            else:
                self.test_results.add_result(
                    "get_eligbile_features_for_user_and_org_non_exists",
                    False,
                    f"Features returned: {features_response}",
                )
        except grpc.RpcError as e:
            self.test_results.add_result(
                "get_eligbile_features_for_user_and_org_non_exists",
                False,
                f"RpcError {e}",
            )
        except Exception as e:
            self.test_results.add_result(
                "get_eligbile_features_for_user_and_org_non_exists",
                False,
                f"Unexpected exception: {e}",
            )

    def get_subpopulation_id_for_user_exists(self):
        request = e9ypb.GetSubPopulationIdForUserRequest(
            user_id=self.member_exists["user_id"]
        )
        try:
            subpopulation_response = self.stub.GetSubPopulationIdForUser(request)
            if subpopulation_response.sub_population_id.value:
                self.test_results.add_result(
                    "get_subpopulation_id_for_user_exists", True, ""
                )
            else:
                self.test_results.add_result(
                    "get_subpopulation_id_for_user_exists",
                    False,
                    f"No subpopulation returned: {subpopulation_response}",
                )
        except grpc.RpcError as e:
            self.test_results.add_result(
                "get_subpopulation_id_for_user_exists", False, f"RpcError {e}"
            )
        except Exception as e:
            self.test_results.add_result(
                "get_subpopulation_id_for_user_exists",
                False,
                f"Unexpected exception: {e}",
            )

    def get_subpopulation_id_for_user_non_exists(self):
        request = e9ypb.GetSubPopulationIdForUserRequest(
            user_id=self.member_not_exists["user_id"]
        )
        try:
            subpopulation_response = self.stub.GetSubPopulationIdForUser(request)
            if not subpopulation_response.sub_population_id.value:
                self.test_results.add_result(
                    "get_subpopulation_id_for_user_non_exists", True, ""
                )
            else:
                self.test_results.add_result(
                    "get_subpopulation_id_for_user_non_exists",
                    False,
                    f"Subpopulation returned: {subpopulation_response}",
                )
        except grpc.RpcError as e:
            self.test_results.add_result(
                "get_subpopulation_id_for_user_non_exists", False, f"RpcError {e}"
            )
        except Exception as e:
            self.test_results.add_result(
                "get_subpopulation_id_for_user_non_exists",
                False,
                f"Unexpected exception: {e}",
            )

    def get_subpopulation_id_for_user_and_org_exists(self):
        request = e9ypb.GetSubPopulationIdForUserAndOrgRequest(
            user_id=self.member_exists["user_id"],
            organization_id=self.member_exists["organization_id"],
        )
        try:
            subpopulation_response = self.stub.GetSubPopulationIdForUserAndOrg(request)
            if subpopulation_response.sub_population_id.value:
                self.test_results.add_result(
                    "get_subpopulation_id_for_user_and_org_exists", True, ""
                )
            else:
                self.test_results.add_result(
                    "get_subpopulation_id_for_user_and_org_exists",
                    False,
                    f"No subpopulation returned: {subpopulation_response}",
                )
        except grpc.RpcError as e:
            self.test_results.add_result(
                "get_subpopulation_id_for_user_and_org_exists", False, f"RpcError {e}"
            )
        except Exception as e:
            self.test_results.add_result(
                "get_subpopulation_id_for_user_and_org_exists",
                False,
                f"Unexpected exception: {e}",
            )

    def get_subpopulation_id_for_user_and_org_non_exists(self):
        request = e9ypb.GetSubPopulationIdForUserAndOrgRequest(
            user_id=self.member_not_exists["user_id"],
            organization_id=self.member_not_exists["organization_id"],
        )
        try:
            subpopulation_response = self.stub.GetSubPopulationIdForUserAndOrg(request)
            if not subpopulation_response.sub_population_id.value:
                self.test_results.add_result(
                    "get_subpopulation_id_for_user_and_org_non_exists", True, ""
                )
            else:
                self.test_results.add_result(
                    "get_subpopulation_id_for_user_and_org_non_exists",
                    False,
                    f"No subpopulation returned: {subpopulation_response}",
                )
        except grpc.RpcError as e:
            self.test_results.add_result(
                "get_subpopulation_id_for_user_and_org_non_exists",
                False,
                f"RpcError {e}",
            )
        except Exception as e:
            self.test_results.add_result(
                "get_subpopulation_id_for_user_and_org_non_exists",
                False,
                f"Unexpected exception: {e}",
            )

    def get_other_user_ids_in_family_exists(self):
        request = e9ypb.GetOtherUserIdsInFamilyRequest(
            user_id=self.member_exists["user_id"]
        )
        try:
            user_response = self.stub.GetOtherUserIdsInFamily(request)
            if user_response:
                self.test_results.add_result(
                    "get_other_user_ids_in_family_exists", True, ""
                )
            else:
                self.test_results.add_result(
                    "get_other_user_ids_in_family_exists",
                    False,
                    f"No other users returned: {user_response}",
                )
        except grpc.RpcError as e:
            self.test_results.add_result(
                "get_other_user_ids_in_family_exists", False, f"RpcError {e}"
            )
        except Exception as e:
            self.test_results.add_result(
                "get_other_user_ids_in_family_exists",
                False,
                f"Unexpected exception: {e}",
            )


def run(organization_id):
    if not organization_id in org_settings:
        raise Exception(f"Unsupported organization {organization_id}")
    testing = APITesting(organization_id)
    testing.run()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-o", "--organization_id", required=True, type=int, help="Organization ID"
    )
    args = parser.parse_args()
    run(args.organization_id)
