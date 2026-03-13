# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "19895240-7ad6-4925-b3f9-b95549db2e47",
# META       "default_lakehouse_name": "lh_CooperHealth_CentralData",
# META       "default_lakehouse_workspace_id": "c5f0b499-f295-4325-92a8-87b7d3800459",
# META       "known_lakehouses": [
# META         {
# META           "id": "19895240-7ad6-4925-b3f9-b95549db2e47"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window
import matplotlib.pyplot as plt

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact_sale = spark.table("dbo.fact_sale")
dim_date = spark.table("dbo.dimension_date")
dim_customer = spark.table("dbo.dimension_customer")
dim_stock = spark.table("dbo.dimension_stock_item")
dim_employee = spark.table("dbo.dimension_employee")
dim_city = spark.table("dbo.dimension_city")

print("fact_sale rows:", fact_sale.count())
print("dimension_date rows:", dim_date.count())


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(fact_sale.limit(10))
display(dim_date.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

# Measures we want, normalized (lowercase, no underscores/spaces)
wanted_measures = {"quantity", "totalexcludingtax", "totalincludingtax", "profit", "unitprice"}

# Build a list of *fact* columns to include, explicitly qualified with alias "f"
measure_cols = [
    F.col(f"f.{col_name}").alias(col_name)   # keep the original column name in output
    for col_name in fact_sale.columns
    if col_name.lower().replace("_", "").replace(" ", "") in wanted_measures
]

sales_df = (
    fact_sale.alias("f")
    .join(dim_date.alias("d"), F.col("f.InvoiceDateKey") == F.col("d.date"), "left")
    .join(dim_customer.alias("c"), F.col("f.customerkey") == F.col("c.customerkey"), "left")
    .join(dim_stock.alias("s"), F.col("f.stockitemkey") == F.col("s.stockitemkey"), "left")
    .join(dim_employee.alias("e"), F.col("f.salespersonkey") == F.col("e.employeekey"), "left")
    .select(
        F.col("d.calendaryear").alias("year"),
        F.col("d.calendarmonthnumber").alias("month_num"),
        F.col("d.month").alias("month"),
        F.col("c.customer").alias("customer"),
        F.col("s.stockitem").alias("stock_item"),
        F.col("e.employee").alias("employee"),
        *measure_cols
    )
)

display(sales_df.limit(10))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sales_df = sales_df.withColumn(
    "period",
    F.concat_ws("-", F.col("year").cast("string"), F.lpad(F.col("month_num").cast("string"), 2, "0"))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sales_df.columns

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

monthly_sales = (
    sales_df
    .groupBy("year", "month_num", "month", "period")
    .agg(
        F.sum("TotalIncludingTax").alias("sales_amount"),
        F.sum("Profit").alias("profit_amount"),
        F.sum("Quantity").alias("units_sold")
    )
    .orderBy("year", "month_num")
)

display(monthly_sales)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Visuals

# CELL ********************

monthly_pd = monthly_sales.toPandas()

import matplotlib.pyplot as plt

plt.figure(figsize=(12,5))
plt.plot(monthly_pd["period"], monthly_pd["sales_amount"], marker="o")
plt.xticks(rotation=45, ha="right")
plt.title("Monthly Sales (Total Including Tax)")
plt.xlabel("Period (YYYY-MM)")
plt.ylabel("Sales Amount")
plt.tight_layout()
plt.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
