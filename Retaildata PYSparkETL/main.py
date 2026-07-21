from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# RAW DATASET USED: blob:https://github.com/58609e96-09f4-4e72-8b36-4f7435f4e277

# file paths
INPUT_PATH = "/Users/nicoostermann/PycharmProjects/Projekte/PYSparkETL RetailData/online-retail-dataset.csv"
OUTPUT_CLEANED = "/Users/nicoostermann/PycharmProjects/Projekte/PYSparkETL RetailData/output/cleaned_retail"
OUTPUT_REVENUE_COUNTRY = "/Users/nicoostermann/PycharmProjects/Projekte/PYSparkETL RetailData/output/revenue_by_country"
OUTPUT_TOP_PRODUCTS = "/Users/nicoostermann/PycharmProjects/Projekte/PYSparkETL RetailData/output/top_products"

# create spark session
spark = SparkSession.builder.appName("RetailETL").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# load csv file
df = spark.read.csv(INPUT_PATH, header=True, inferSchema=True)

print("RAW SCHEMA")
df.printSchema()

print("RAW SAMPLE")
df.show(5, truncate=False)

# clean important columns and fix the date format
df_cleaned = (
    df
    .withColumn("InvoiceDate", F.to_timestamp(F.col("InvoiceDate"), "M/d/yyyy H:mm"))
    .withColumn("InvoiceNo", F.trim(F.col("InvoiceNo")))
    .withColumn("StockCode", F.trim(F.col("StockCode")))
    .withColumn("Description", F.trim(F.col("Description")))
    .withColumn("Country", F.upper(F.trim(F.col("Country"))))
    .dropna(subset=["InvoiceNo", "StockCode", "Description", "InvoiceDate", "CustomerID", "Country"])
    .filter(F.col("Quantity") > 0)
    .filter(F.col("UnitPrice") > 0)
    .filter(~F.col("InvoiceNo").startswith("C"))
    .dropDuplicates()
)

# add some useful columns for later analysis
df_cleaned = df_cleaned.withColumn("Revenue", F.round(F.col("Quantity") * F.col("UnitPrice"), 2))
df_cleaned = df_cleaned.withColumn("InvoiceYear", F.year(F.col("InvoiceDate")))
df_cleaned = df_cleaned.withColumn("InvoiceMonth", F.month(F.col("InvoiceDate")))
df_cleaned = df_cleaned.withColumn("InvoiceDay", F.dayofmonth(F.col("InvoiceDate")))
df_cleaned = df_cleaned.withColumn("InvoiceHour", F.hour(F.col("InvoiceDate")))

print("CLEANED SCHEMA")
df_cleaned.printSchema()

print("CLEANED SAMPLE")
df_cleaned.show(10, truncate=False)

print("CLEANED ROW COUNT")
print(df_cleaned.count())

# revenue per country
revenue_by_country = (
    df_cleaned
    .groupBy("Country")
    .agg(
        F.round(F.sum("Revenue"), 2).alias("TotalRevenue"),
        F.count("*").alias("TotalTransactions")
    )
    .orderBy(F.desc("TotalRevenue"))
)

# best products by revenue
top_products = (
    df_cleaned
    .groupBy("StockCode", "Description")
    .agg(
        F.sum("Quantity").alias("TotalQuantitySold"),
        F.round(F.sum("Revenue"), 2).alias("TotalRevenue")
    )
    .orderBy(F.desc("TotalRevenue"))
)

print("REVENUE BY COUNTRY")
revenue_by_country.show(10, truncate=False)

print("TOP PRODUCTS")
top_products.show(10, truncate=False)

# save outputs
df_cleaned.write.mode("overwrite").parquet(OUTPUT_CLEANED)
revenue_by_country.write.mode("overwrite").option("header", True).csv(OUTPUT_REVENUE_COUNTRY)
top_products.write.mode("overwrite").option("header", True).csv(OUTPUT_TOP_PRODUCTS)

print("ETL DONE")
print(f"cleaned data -> {OUTPUT_CLEANED}")
print(f"country revenue -> {OUTPUT_REVENUE_COUNTRY}")
print(f"top products -> {OUTPUT_TOP_PRODUCTS}")

spark.stop()