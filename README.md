# Lesson 2: Adding ECG Timing Columns Relative to AE Start Date

In this tutorial, we will create a listing that adds two new columns:

1. **ECG Before AESTARTDATE**: The latest ECG record before the AE start date.
2. **ECG After AESTARTDATE**: The earliest ECG record after the AE start date.

Additionally, the output will include the following columns:

- **AE Logline**
- **AETERM**
- **AEDECODE**
- **AESTARTDATE**
- **AEENDDATE**

We will achieve this by joining data from the following domains:

1. **AE (Adverse Events)**
2. **EG (Electrocardiograms)**

---

## Steps

### Step 1: Import Required Libraries
Import the necessary libraries for data processing.

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max, min, when
```

### Step 2: Initialize Spark Session
Create a Spark session for handling the data.

```python
spark = SparkSession.builder \
    .appName("AE and ECG Timing") \
    .getOrCreate()
```

### Step 3: Load the Datasets
Load the AE and EG datasets into separate Spark DataFrames. Ensure the datasets are available in the specified paths.

```python
# Load datasets
ae_df = spark.read.csv("AE.csv", header=True, inferSchema=True)
eg_df = spark.read.csv("EG.csv", header=True, inferSchema=True)
```

### Step 4: Preprocess Data
Ensure the necessary columns are properly formatted and parse date columns for easier processing.

```python
# Convert date columns to date format
ae_df = ae_df.withColumn("AESTARTDATE", col("AESTARTDATE").cast("date"))
ae_df = ae_df.withColumn("AEENDDATE", col("AEENDDATE").cast("date"))
eg_df = eg_df.withColumn("EGDATE", col("EGDATE").cast("date"))
```

### Step 5: Join AE and EG Data
Join the AE and EG datasets on `subject_id` to combine ECG data with AE data.

```python
# Join AE and EG datasets
joined_df = ae_df.join(eg_df, on="subject_id", how="left")
```

### Step 6: Derive ECG Timing Columns

#### Step 6.1: ECG Before AESTARTDATE
Identify the latest ECG record before the AE start date for each AE logline using a window function.

```python
from pyspark.sql.window import Window

# Define a window partitioned by subject_id and AE Logline
window_spec_before = Window.partitionBy("subject_id", "AE Logline").orderBy(col("EGDATE").desc())

# Filter and select the latest ECG before AESTARTDATE
joined_df = joined_df.withColumn(
    "ECG_Before_AESTARTDATE",
    max(when(col("EGDATE") < col("AESTARTDATE"), col("EGDATE"))).over(window_spec_before)
)
```

#### Step 6.2: ECG After AESTARTDATE
Identify the earliest ECG record after the AE start date for each AE logline using a similar window function.

```python
# Define a window partitioned by subject_id and AE Logline
window_spec_after = Window.partitionBy("subject_id", "AE Logline").orderBy(col("EGDATE"))

# Filter and select the earliest ECG after AESTARTDATE
joined_df = joined_df.withColumn(
    "ECG_After_AESTARTDATE",
    min(when(col("EGDATE") >= col("AESTARTDATE"), col("EGDATE"))).over(window_spec_after)
)
```

### Step 7: Select Required Columns
Filter the DataFrame to include only the required columns in the final listing.

```python
final_listing = joined_df.select(
    "subject_id",
    "AE Logline",
    "AETERM",
    "AEDECODE",
    "AESTARTDATE",
    "AEENDDATE",
    "ECG_Before_AESTARTDATE",
    "ECG_After_AESTARTDATE"
)
```

### Step 8: Save the Listing
Export the final listing to a CSV file for review.

```python
# Save to CSV
final_listing.write.csv("AE_ECG_Timing_Listing.csv", header=True, mode="overwrite")
```

---

## Sample Output
Below is an example of the output generated by the above steps:

| subject_id | AE Logline | AETERM           | AEDECODE | AESTARTDATE | AEENDDATE  | ECG_Before_AESTARTDATE | ECG_After_AESTARTDATE |
|------------|------------|------------------|----------|-------------|------------|------------------------|-----------------------|
| 101        | 1          | Headache         | HEAD     | 2024-01-10  | 2024-01-15 | 2024-01-05             | 2024-01-12            |
| 102        | 2          | Nausea           | NAUS     | 2024-01-15  | 2024-01-20 | 2024-01-12             | 2024-01-17            |

---

## Key Takeaways
- **Data Integration**: Combined AE and EG domains using `join`.
- **ECG Timing Calculation**: Derived the latest and earliest ECG records relative to the AE start date.
- **Final Output**: Generated a listing with essential columns for clinical review.

This approach can be adapted for other timing-related analyses in clinical datasets.
