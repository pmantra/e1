import csv
import os
from datetime import datetime

import pandas as pd


def split_files(output_dir, input_file_name) -> object:
    # Create our output dir if it does not exist
    p = os.path.join(os.getcwd(), output_dir)
    if not os.path.exists(p):
        os.makedirs(p)

    print("Spliting original CSV file")
    customer_id_count = {}
    contents = pd.read_csv(input_file_name)
    for customer_id, split_contents in contents.groupby("customerId"):
        # Write results to file
        output_file_name = f"{datetime.now().strftime('%m_%d_%Y')}_{customer_id}.csv"
        split_contents.to_csv(f"{p}/{output_file_name}", index=False)

        # Add the counts to our results
        customer_id_count[customer_id] = len(split_contents.index)

    print("-------\nWe have split the file successfully")
    print("-------\nCounts of records found per customerID: ")

    # Write out a report of our counts
    with open(f"{p}/report.csv", "w") as f:
        writer = csv.writer(f)
        writer.writerow(["Customer ID", "Count"])
        print("Customer ID, Count")

        for k, v in customer_id_count.items():
            writer.writerow([k, v])
            print(f"{k}, {v}")


def get_input_args():
    file_name = input("Please enter the file name you wish to split: ")
    print(" You entered : " + file_name)
    continue_operation = input("Continue? Y/N: ")
    if continue_operation.upper() not in ["YES", "Y"]:
        print("\n Process terminated.")
        exit()

    return file_name


if __name__ == "__main__":
    file_name = get_input_args()
    output_dir = (
        f"{file_name.split('.csv')[0]}_{datetime.now().strftime('%m_%d_%Y_%H_%M_%S')}"
    )
    print(f"\nResultant files will be written to the following directory: {output_dir}")
    split_files(output_dir, file_name)
