"""
Script to update organization external ID mappings.

This script checks if external IDs are in the payer or employer file and updates
the data_provider_organization_id accordingly.
"""

import argparse
import asyncio
import csv
import os
import sys
from typing import Any, Dict, List, Optional, Tuple

from db.mono.client import MavenMonoClient

EMPLOYER_PROVIDER_ORG_ID = 2865  # Provider ID for employer records
PAYER_PROVIDER_ORG_ID = 2869  # Provider ID for payer records


def load_csv_data(file_path: str) -> List[Dict[str, str]]:
    data = []
    try:
        with open(file_path, mode="r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            data = [row for row in reader]
    except Exception as e:
        print(f"Error loading CSV file {file_path}: {e}")
    return data


async def fetch_and_filter_records(mono_client, optum_idp, target_orgs):
    all_records = await mono_client.get_all_records_with_optum_idp(optum_idp)
    if not all_records:
        print(f"No records found with '{optum_idp}' as IDP")
        return []
    if target_orgs:
        records = [rec for rec in all_records if rec["organization_id"] in target_orgs]
        print(f"Filtered {len(records)} records for target organizations")
    else:
        records = all_records
        print(f"Processing all {len(records)} records with '{optum_idp}' as IDP")
    return records


def parse_external_id(external_id_value):
    if ":" in external_id_value:
        client_id, customer_id = [x.strip() for x in external_id_value.split(":", 1)]
    else:
        client_id, customer_id = external_id_value.strip(), None
    return client_id, customer_id


def exists_in_csv(
    data: List[Dict[str, str]], client_id: Optional[str], customer_id: Optional[str]
) -> bool:
    for row in data:
        if client_id and row.get("CLIENT_ID") == client_id:
            return True
        if customer_id and row.get("CUST_ID") == customer_id:
            return True
    return False


async def validate_and_update_record(
    mono_client,
    record,
    org_id,
    external_id_value,
    found_in_payer,
    found_in_employer,
    dry_run,
):
    if not (found_in_payer or found_in_employer):
        error_msg = f"ERROR: Org {org_id}, External ID {external_id_value} - Not found"
        print(error_msg)
        return {
            "success": 0,
            "failure": 1,
            "error": {
                "organization_id": org_id,
                "external_id": external_id_value,
                "error": "Not found",
            },
        }

    if found_in_payer and found_in_employer:
        error_msg = f"ERROR: Org {org_id}, External ID {external_id_value} - Found in both files"
        print(error_msg)
        return {
            "success": 0,
            "failure": 1,
            "error": {
                "organization_id": org_id,
                "external_id": external_id_value,
                "error": "Found in both",
            },
        }

    new_provider_org_id = (
        PAYER_PROVIDER_ORG_ID if found_in_payer else EMPLOYER_PROVIDER_ORG_ID
    )
    file_source = "payer" if found_in_payer else "employer"

    print(
        f"Organization {org_id}: External ID {external_id_value} found in {file_source} file"
    )
    print(f"Setting provider to {new_provider_org_id}")

    if dry_run:
        print(f"[DRY RUN] Would update org {org_id}, external ID {external_id_value}")
        return {"success": 1, "failure": 0, "error": None}

    try:
        updated_count = await mono_client.update_org_provider(
            record_id=record["id"], new_provider_org_id=new_provider_org_id
        )
        print(
            f"Updated record ID {record['id']} for org {org_id}, external ID {external_id_value} ({updated_count} records)"
        )
        return {"success": 1, "failure": 0, "error": None}
    except Exception as e:
        error_msg = f"ERROR: Org {org_id}, External ID {external_id_value} - Update failed: {str(e)}"
        print(error_msg)
        return {
            "success": 0,
            "failure": 1,
            "error": {
                "organization_id": org_id,
                "external_id": external_id_value,
                "error": f"Update failed: {str(e)}",
            },
        }


async def process_organization_external_ids(
    mono_client: MavenMonoClient,
    target_orgs: Optional[List[int]],
    payer_data: List[Dict[str, str]],
    employer_data: List[Dict[str, str]],
    optum_idp: str = "OPTUM",
    dry_run: bool = False,
) -> Tuple[int, int, List[Dict[str, Any]]]:

    records = await fetch_and_filter_records(mono_client, optum_idp, target_orgs)
    success_count, failure_count, errors = 0, 0, []

    for record in records:
        org_id = record["organization_id"]
        external_id_value = record["external_id"]

        client_id, customer_id = parse_external_id(external_id_value)

        found_in_payer = exists_in_csv(payer_data, client_id, customer_id)
        found_in_employer = exists_in_csv(employer_data, client_id, customer_id)

        result = await validate_and_update_record(
            mono_client,
            record,
            org_id,
            external_id_value,
            found_in_payer,
            found_in_employer,
            dry_run,
        )

        success_count += result["success"]
        failure_count += result["failure"]
        if result["error"]:
            errors.append(result["error"])

    return success_count, failure_count, errors


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Update organization external ID mappings"
    )
    parser.add_argument(
        "--payer-file", type=str, help="Path to payer CSV (default: payer_data.csv)"
    )
    parser.add_argument(
        "--employer-file",
        type=str,
        help="Path to employer CSV (default: employer_data.csv)",
    )
    parser.add_argument(
        "--target-orgs",
        type=str,
        help="Comma-separated list of target organization IDs (optional)",
    )
    parser.add_argument(
        "--optum-idp",
        type=str,
        default="OPTUM",
        help="The IDP value for Optum (default: OPTUM)",
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Run without making changes"
    )

    return parser.parse_args()


def get_input_confirmation(args):
    """Get user confirmation before proceeding."""
    print("\n=== Organization Mapping Update ===")
    print(f"Payer file: {args.payer_file}")
    print(f"Employer file: {args.employer_file}")
    print(f"Optum IDP: {args.optum_idp}")
    print(f"  - {PAYER_PROVIDER_ORG_ID} (for external IDs found in payer file)")
    print(f"  - {EMPLOYER_PROVIDER_ORG_ID} (for external IDs found in employer file)")

    if args.target_orgs:
        print(f"Target Organizations: {args.target_orgs}")
    else:
        print("Target Organizations: ALL organizations with Optum as IDP")

    if args.dry_run:
        print("DRY RUN mode - no changes will be made")
    else:
        print("LIVE mode - changes will be committed to database")

    continue_operation = input("\nContinue? Y/N: ")
    if continue_operation.upper() not in ["YES", "Y"]:
        print("\nProcess terminated.")
        sys.exit(0)


async def run(target_orgs=None, dry_run=True):
    """
    Run the script programmatically with parameters.

    Args:
        target_orgs: List of target organization IDs or comma-separated string
        dry_run: Whether to run without making changes

    Returns:
        Tuple of (success_count, failure_count, errors)
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))

    payer_file = os.path.join(script_dir, "payer_data.csv")
    employer_file = os.path.join(script_dir, "employer_data.csv")

    parsed_target_orgs = None
    if target_orgs:
        if isinstance(target_orgs, str):
            parsed_target_orgs = [
                int(org_id.strip()) for org_id in target_orgs.split(",")
            ]
        else:
            parsed_target_orgs = target_orgs

    payer_data = load_csv_data(payer_file)
    employer_data = load_csv_data(employer_file)

    mono_client = MavenMonoClient()

    success_count, failure_count, errors = await process_organization_external_ids(
        mono_client=mono_client,
        target_orgs=parsed_target_orgs,
        payer_data=payer_data,
        employer_data=employer_data,
        optum_idp="OPTUM",
        dry_run=dry_run,
    )

    print(f"\n{'-' * 40}")
    print(f"Success: {success_count}, Failures: {failure_count}")
    if errors:
        print(f"Errors: {len(errors)}")

    return success_count, failure_count, errors


async def main():
    args = parse_args()
    script_dir = os.path.dirname(os.path.abspath(__file__))

    if args.payer_file is None:
        args.payer_file = os.path.join(script_dir, "payer_data.csv")
    if args.employer_file is None:
        args.employer_file = os.path.join(script_dir, "employer_data.csv")

    target_orgs = None
    if args.target_orgs:
        target_orgs = [int(org_id.strip()) for org_id in args.target_orgs.split(",")]

    get_input_confirmation(args)

    payer_data = load_csv_data(args.payer_file)
    employer_data = load_csv_data(args.employer_file)

    mono_client = MavenMonoClient()

    success_count, failure_count, errors = await process_organization_external_ids(
        mono_client=mono_client,
        target_orgs=target_orgs,
        payer_data=payer_data,
        employer_data=employer_data,
        optum_idp=args.optum_idp,
        dry_run=args.dry_run,
    )

    print(f"\n{'-' * 40}")
    print(f"Success: {success_count}, Failures: {failure_count}")
    if errors:
        print(f"Errors: {len(errors)}")


if __name__ == "__main__":
    asyncio.run(main())
