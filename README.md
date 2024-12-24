# Lesson 2: PySpark Tutorial - Checking Deaths After AE Reported Event Ends

## Objective:
Create a listing that checks if there are any deaths occurring after the AE (Adverse Event) reported event ends using PySpark. Add a column `Message` with "Yes" or "No" based on the presence of death after AE end. Include the following columns in the final listing:

- AE End Date
- AE Logline
- Maximum Disposition Date
- AETERM
- AEDECODE
- AESTARTDATE
- AEENDDATE
- RFICDT

### Data Sources:
- **AE**: Adverse Event domain.
- **DD**: Death Disposition domain.
- **DM**: Demographics domain.

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, to_date, greatest

# Initialize Spark session
spark = SparkSession.builder.appName("Lesson 2 - AE Deaths Check").getOrCreate()

# Sample DataFrames for AE, DD, and DM
ae_data = [
    ("SUBJ001", "Headache", "HEADACHE", "2024-01-01", "2024-01-10", "Log1"),
    ("SUBJ002", "Nausea", "NAUSEA", "2024-02-01", "2024-02-10", "Log2"),
    ("SUBJ003", "Dizziness", "DIZZINESS", "2024-03-01", "2024-03-15", "Log3")
]
dd_data = [
    ("SUBJ001", "2024-01-15"),
    ("SUBJ002", "2024-02-12"),
    ("SUBJ004", "2024-04-01")
]
dm_data = [
    ("SUBJ001", "2023-12-25"),
    ("SUBJ002", "2024-01-15"),
    ("SUBJ003", "2024-02-20"),
    ("SUBJ004", "2024-03-01")
]

# Define schema and create DataFrames
ae_df = spark.createDataFrame(ae_data, ["USUBJID", "AETERM", "AEDECODE", "AESTARTDATE", "AEENDDATE", "AE_LOG"])
dd_df = spark.createDataFrame(dd_data, ["USUBJID", "DDDAT"])
dm_df = spark.createDataFrame(dm_data, ["USUBJID", "RFICDT"])

# Convert date columns to date type
ae_df = ae_df.withColumn("AESTARTDATE", to_date(col("AESTARTDATE")))
ae_df = ae_df.withColumn("AEENDDATE", to_date(col("AEENDDATE")))
dd_df = dd_df.withColumn("DDDAT", to_date(col("DDDAT")))
dm_df = dm_df.withColumn("RFICDT", to_date(col("RFICDT")))

# Join AE, DD, and DM DataFrames
merged_df = ae_df.join(dd_df, on="USUBJID", how="left")
merged_df = merged_df.join(dm_df, on="USUBJID", how="left")

# Calculate Maximum Disposition Date (DDDAT)
merged_df = merged_df.withColumn("MaxDispositionDate", col("DDDAT"))

# Add Message Column: Check if death occurred after AE end date
merged_df = merged_df.withColumn(
    "Message",
    when((col("DDDAT").isNotNull()) & (col("DDDAT") > col("AEENDDATE")), "Yes").otherwise("No")
)

# Select and rename required columns
final_listing = merged_df.select(
    col("USUBJID"),
    col("AETERM"),
    col("AEDECODE"),
    col("AESTARTDATE"),
    col("AEENDDATE"),
    col("AE_LOG").alias("AE Logline"),
    col("MaxDispositionDate").alias("Maximum Disposition Date"),
    col("RFICDT"),
    col("Message")
)

# Show the final listing
final_listing.show()
```

### Explanation:
1. **Data Preparation**:
   - Convert date columns to the appropriate `date` format using `to_date`.
2. **Joining DataFrames**:
   - Use `join` to merge AE, DD, and DM DataFrames on `USUBJID`.
3. **Calculating `Message`**:
   - Use `when` and `otherwise` to create a conditional column indicating if a death occurred after the AE end date.
4. **Final Listing**:
   - Select and rename columns for better clarity.

### Output:
The resulting DataFrame (`final_listing`) will look like this:

| USUBJID  | AETERM     | AEDECODE   | AESTARTDATE | AEENDDATE   | AE Logline | Maximum Disposition Date | RFICDT     | Message |
|----------|------------|------------|-------------|-------------|------------|---------------------------|------------|---------|
| SUBJ001  | Headache   | HEADACHE   | 2024-01-01  | 2024-01-10  | Log1       | 2024-01-15                | 2023-12-25 | Yes     |
| SUBJ002  | Nausea     | NAUSEA     | 2024-02-01  | 2024-02-10  | Log2       | 2024-02-12                | 2024-01-15 | Yes     |
| SUBJ003  | Dizziness  | DIZZINESS  | 2024-03-01  | 2024-03-15  | Log3       | null                      | 2024-02-20 | No      |
