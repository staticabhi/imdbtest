# Lesson 4: Advanced Clinical Data Analysis Using Pandas

## Title:
Performing Advanced Clinical Data Analysis with Pandas: Combining Medical History, Medications, Demographics, and Adverse Events for Deriving Insights

## Objective:
Create a complex listing combining data from the following clinical domains:
- **MH**: Medical History
- **CM**: Concomitant Medications
- **DM**: Demographics
- **AE**: Adverse Events

The listing will:
1. Analyze adverse events (AE) in the context of a subject’s medical history (MH) and medications (CM).
2. Use advanced Pandas operations like `groupby`, custom functions, and conditional derivations for insights.
3. Include derived metrics such as the count of overlapping medications and a flag indicating severe AEs based on custom criteria.

### Key Columns:
- **Subject ID (USUBJID)**
- **Medical History Term (MHDECOD)**
- **Concomitant Medication (CMTRT)**
- **Adverse Event Term (AETERM)**
- **AE Severity Flag**: A derived column indicating if the AE was severe (Yes/No).
- **Overlap Count**: Number of overlapping medications during the AE period.

```python
import pandas as pd

# Sample DataFrames for MH, CM, DM, and AE
mh_data = {
    "USUBJID": ["SUBJ001", "SUBJ002", "SUBJ003"],
    "MHDECOD": ["Hypertension", "Arthritis", "Heart Disease"],
    "MHSTDTC": ["2023-12-15", "2024-02-01", "2024-03-10"]
}

cm_data = {
    "USUBJID": ["SUBJ001", "SUBJ002", "SUBJ003"],
    "CMTRT": ["Paracetamol", "Ibuprofen", "Aspirin"],
    "CMSTDTC": ["2024-01-01", "2024-02-10", "2024-03-15"],
    "CMENDTC": ["2024-01-10", "2024-02-15", "2024-03-20"]
}

ae_data = {
    "USUBJID": ["SUBJ001", "SUBJ002", "SUBJ003"],
    "AETERM": ["Headache", "Nausea", "Dizziness"],
    "AESEV": ["Moderate", "Severe", "Mild"],
    "AESTARTDATE": ["2024-01-05", "2024-02-12", "2024-03-16"],
    "AEENDDATE": ["2024-01-07", "2024-02-14", "2024-03-17"]
}

dm_data = {
    "USUBJID": ["SUBJ001", "SUBJ002", "SUBJ003"],
    "RFICDT": ["2023-12-25", "2024-01-15", "2024-02-20"]
}

# Convert to DataFrames
mh_df = pd.DataFrame(mh_data)
cm_df = pd.DataFrame(cm_data)
ae_df = pd.DataFrame(ae_data)
dm_df = pd.DataFrame(dm_data)

# Convert date columns to datetime format
for df, cols in [(mh_df, ["MHSTDTC"]), (cm_df, ["CMSTDTC", "CMENDTC"]), (ae_df, ["AESTARTDATE", "AEENDDATE"]), (dm_df, ["RFICDT"])]:
    for col in cols:
        df[col] = pd.to_datetime(df[col])

# Merge DataFrames
merged_df = ae_df.merge(cm_df, on="USUBJID", how="inner")
merged_df = merged_df.merge(mh_df, on="USUBJID", how="left")
merged_df = merged_df.merge(dm_df, on="USUBJID", how="left")

# Define a function to flag severe AEs
def severity_flag(severity):
    return "Yes" if severity == "Severe" else "No"

merged_df["AE Severity Flag"] = merged_df["AESEV"].apply(severity_flag)

# Calculate Overlap Count
def calculate_overlap(row):
    overlap = (
        (row["CMSTDTC"] <= row["AEENDDATE"]) &
        (row["CMENDTC"] >= row["AESTARTDATE"])
    )
    return overlap.sum() if isinstance(overlap, pd.Series) else int(overlap)

merged_df["Overlap Count"] = merged_df.apply(calculate_overlap, axis=1)

# Select and rename columns for final listing
final_listing = merged_df[[
    "USUBJID", "MHDECOD", "CMTRT", "AETERM", "AE Severity Flag", "Overlap Count", "AESTARTDATE", "AEENDDATE"
]].rename(columns={
    "USUBJID": "Subject ID",
    "MHDECOD": "Medical History Term",
    "CMTRT": "Concomitant Medication",
    "AETERM": "Adverse Event Term",
    "AESTARTDATE": "AE Start Date",
    "AEENDDATE": "AE End Date"
})

# Display the final listing
print(final_listing)
```

### Explanation:
1. **Data Preparation**:
   - Convert date columns to `datetime` format for accurate comparisons.
2. **Merging DataFrames**:
   - Combine data from multiple domains using `merge`.
3. **Custom Functions**:
   - Use `apply` to flag severe AEs and calculate overlaps between medications and AE periods.
4. **Final Output**:
   - Select and rename columns for better clarity.

### Sample Output:
| Subject ID | Medical History Term | Concomitant Medication | Adverse Event Term | AE Severity Flag | Overlap Count | AE Start Date | AE End Date   |
|------------|-----------------------|-------------------------|--------------------|------------------|---------------|---------------|---------------|
| SUBJ001    | Hypertension         | Paracetamol            | Headache           | No               | 1             | 2024-01-05    | 2024-01-07    |
| SUBJ002    | Arthritis            | Ibuprofen              | Nausea             | Yes              | 1             | 2024-02-12    | 2024-02-14    |
| SUBJ003    | Heart Disease        | Aspirin                | Dizziness          | No               | 1             | 2024-03-16    | 2024-03-17    |

This listing provides advanced insights into the relationship between adverse events, medical history, and concomitant medications for each subject.
