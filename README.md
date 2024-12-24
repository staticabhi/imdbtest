# Lesson 4: Advanced Clinical Data Analysis Using PySpark

## Title:
Performing Advanced Clinical Data Analysis with PySpark: Leveraging Partitioning, Joins, and UDFs for Insights into Adverse Events, Concomitant Medications, and Medical History

## Objective:
Create a complex listing combining data from the following clinical domains:
- **MH**: Medical History
- **CM**: Concomitant Medications
- **DM**: Demographics
- **AE**: Adverse Events

The listing will:
1. Analyze adverse events (AE) in the context of a subject’s medical history (MH) and medications (CM).
2. Use advanced PySpark operations like `window functions` (via partitioning) and `user-defined functions` (UDFs) for meaningful derivations.
3. Include derived metrics such as the count of overlapping medications and a flag indicating severe AEs based on custom criteria.

### Key Columns:
- **Subject ID (USUBJID)**
- **Medical History Term (MHDECOD)**
- **Concomitant Medication (CMTRT)**
- **Adverse Event Term (AETERM)**
- **AE Severity Flag**: A derived column indicating if the AE was severe (Yes/No).
- **Overlap Count**: Number of overlapping medications during the AE period.

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, count, when, udf
from pyspark.sql.window import Window
from pyspark.sql.types import StringType

# Initialize Spark session
spark = SparkSession.builder.appName("Advanced Clinical Data Analysis").getOrCreate()

# Sample DataFrames for MH, CM, DM, and AE
mh_data = [
    ("SUBJ001", "Hypertension", "2023-12-15"),
    ("SUBJ002", "Arthritis", "2024-02-01"),
    ("SUBJ003", "Heart Disease", "2024-03-10")
]
cm_data = [
    ("SUBJ001", "Paracetamol", "2024-01-01", "2024-01-10"),
    ("SUBJ002", "Ibuprofen", "2024-02-10", "2024-02-15"),
    ("SUBJ003", "Aspirin", "2024-03-15", "2024-03-20")
]
ae_data = [
    ("SUBJ001", "Headache", "Moderate", "2024-01-05", "2024-01-07"),
    ("SUBJ002", "Nausea", "Severe", "2024-02-12", "2024-02-14"),
    ("SUBJ003", "Dizziness", "Mild", "2024-03-16", "2024-03-17")
]
dm_data = [
    ("SUBJ001", "2023-12-25"),
    ("SUBJ002", "2024-01-15"),
    ("SUBJ003", "2024-02-20")
]

# Define schema and create DataFrames
mh_df = spark.createDataFrame(mh_data, ["USUBJID", "MHDECOD", "MHSTDTC"])
cm_df = spark.createDataFrame(cm_data, ["USUBJID", "CMTRT", "CMSTDTC", "CMENDTC"])
ae_df = spark.createDataFrame(ae_data, ["USUBJID", "AETERM", "AESEV", "AESTARTDATE", "AEENDDATE"])
dm_df = spark.createDataFrame(dm_data, ["USUBJID", "RFICDT"])

# Convert date columns to date type
def convert_dates(df, cols):
    for col_name in cols:
        df = df.withColumn(col_name, to_date(col(col_name)))
    return df

mh_df = convert_dates(mh_df, ["MHSTDTC"])
cm_df = convert_dates(cm_df, ["CMSTDTC", "CMENDTC"])
ae_df = convert_dates(ae_df, ["AESTARTDATE", "AEENDDATE"])
dm_df = convert_dates(dm_df, ["RFICDT"])

# Join DataFrames
merged_df = ae_df.join(cm_df, on="USUBJID", how="inner")
merged_df = merged_df.join(mh_df, on="USUBJID", how="left")
merged_df = merged_df.join(dm_df, on="USUBJID", how="left")

# Define a UDF to flag severe AEs
def severity_flag(severity):
    return "Yes" if severity == "Severe" else "No"

severity_flag_udf = udf(severity_flag, StringType())
merged_df = merged_df.withColumn("AE Severity Flag", severity_flag_udf(col("AESEV")))

# Calculate Overlap Count using a Window function
window_spec = Window.partitionBy("USUBJID").orderBy("AESTARTDATE")
merged_df = merged_df.withColumn(
    "Overlap Count",
    count(when((col("CMSTDTC") <= col("AEENDDATE")) & (col("CMENDTC") >= col("AESTARTDATE")), 1)).over(window_spec)
)

# Select and rename columns for final listing
final_listing = merged_df.select(
    col("USUBJID").alias("Subject ID"),
    col("MHDECOD").alias("Medical History Term"),
    col("CMTRT").alias("Concomitant Medication"),
    col("AETERM").alias("Adverse Event Term"),
    col("AE Severity Flag"),
    col("Overlap Count"),
    col("AESTARTDATE"),
    col("AEENDDATE")
)

# Show the final listing
final_listing.show()
```

### Explanation:
1. **Data Preparation**:
   - Convert date columns to the correct format for operations.
2. **Advanced Joins**:
   - Combine data from multiple domains using `join`.
3. **User-Defined Functions**:
   - Define a UDF to flag severe AEs based on the severity column.
4. **Window Functions**:
   - Calculate the count of overlapping medications during AE periods using partitioning.
5. **Final Output**:
   - Select and rename columns for better clarity.

### Sample Output:
| Subject ID | Medical History Term | Concomitant Medication | Adverse Event Term | AE Severity Flag | Overlap Count | AESTARTDATE | AEENDDATE   |
|------------|-----------------------|-------------------------|--------------------|------------------|---------------|-------------|-------------|
| SUBJ001    | Hypertension         | Paracetamol            | Headache           | No               | 1             | 2024-01-05  | 2024-01-07  |
| SUBJ002    | Arthritis            | Ibuprofen              | Nausea             | Yes              | 1             | 2024-02-12  | 2024-02-14  |
| SUBJ003    | Heart Disease        | Aspirin                | Dizziness          | No               | 1             | 2024-03-16  | 2024-03-17  |

This listing combines advanced PySpark techniques to derive meaningful insights from clinical data.
