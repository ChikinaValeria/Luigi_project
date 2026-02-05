import luigi
import pandas as pd
import json
import os

DATA_DIR = "data"
OUTPUT_DIR = "output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Task 1 - loading of sales
class LoadSales(luigi.Task):
    input_file = luigi.Parameter(default=os.path.join(DATA_DIR, "sales.csv"))
    output_file = luigi.Parameter(default=os.path.join(OUTPUT_DIR, "sales_loaded.csv"))

    def output(self):
        return luigi.LocalTarget(self.output_file)

    def run(self):
        sales_df = pd.read_csv(self.input_file)
        sales_df.to_csv(self.output_file, index=False)
        print(f"Sales data loaded to {self.output_file}")

# Task 2 - loading customers
class LoadCustomers(luigi.Task):
    input_file = luigi.Parameter(default=os.path.join(DATA_DIR, "customers.json"))
    output_file = luigi.Parameter(default=os.path.join(OUTPUT_DIR, "customers_loaded.csv"))

    def output(self):
        return luigi.LocalTarget(self.output_file)

    def run(self):
        with open(self.input_file, "r", encoding="utf-8") as f:
            customers_data = json.load(f)
        customers_df = pd.json_normalize(customers_data)
        customers_df.to_csv(self.output_file, index=False)
        print(f"Customers data loaded to {self.output_file}")


# Task 3 - data merge amd validation
class MergeAndValidate(luigi.Task):
    output_file = luigi.Parameter(default=os.path.join(OUTPUT_DIR, "merged_validated.csv"))

    def requires(self):
        return [LoadSales(), LoadCustomers()]

    def output(self):
        return luigi.LocalTarget(self.output_file)

    def run(self):
        sales_df = pd.read_csv(self.input()[0].path)
        customers_df = pd.read_csv(self.input()[1].path)

        merged_df = pd.merge(sales_df, customers_df, how="left", on="customer_id")

        # delete incorrect data
        merged_df = merged_df.dropna(subset=["customer_id", "amount"])
        merged_df = merged_df[merged_df["amount"] >= 0]

        merged_df.to_csv(self.output_file, index=False)
        print(f"Merged and validated data saved to {self.output_file}")


# Task 4 - data transformation and aggregation
class TransformAndAggregate(luigi.Task):
    output_file = luigi.Parameter(default=os.path.join(OUTPUT_DIR, "aggregated.csv"))

    def requires(self):
        return MergeAndValidate()

    def output(self):
        return luigi.LocalTarget(self.output_file)

    def run(self):
        df = pd.read_csv(self.input().path)
        total_sales = df["amount"].sum()
        total_customers = df["customer_id"].nunique()

        agg_df = pd.DataFrame({
            "total_sales": [total_sales],
            "total_customers": [total_customers]
        })

        agg_df.to_csv(self.output_file, index=False)
        print(f"Aggregated data saved to {self.output_file}")


# Task 5 - report generation
class GenerateReport(luigi.Task):
    output_file = luigi.Parameter(default=os.path.join(OUTPUT_DIR, "report.txt"))

    def requires(self):
        return TransformAndAggregate()

    def output(self):
        return luigi.LocalTarget(self.output_file)

    def run(self):
        df = pd.read_csv(self.input().path)
        total_sales = df["total_sales"].iloc[0]
        total_customers = df["total_customers"].iloc[0]

        report_lines = [
            "----- Sales Report -----",
            f"Total sales amount: {total_sales}",
            f"Total unique customers: {total_customers}",
            "------------------------"
        ]

        with open(self.output_file, "w", encoding="utf-8") as f:
            f.write("\n".join(report_lines))

        print(f"Report generated at {self.output_file}")


if __name__ == "__main__":
    luigi.build([GenerateReport()], local_scheduler=True)
