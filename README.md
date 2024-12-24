# Lesson 3: Creating a Listing for Concomitant Medications and Medical History (Pandas)

## Objective:
Create a listing to analyze Concomitant Medications (CM) and Medical History (MH) using Pandas. The listing will include derived columns and key variables from the following domains:
- **CM**: Concomitant Medications domain.
- **MH**: Medical History domain.

### Key Variables:
1. **Subject ID (USUBJID)**
2. **CMTRT**: Name of Concomitant Medication
3. **CMINDC**: Indication for Concomitant Medication
4. **CMSTDTC**: Start Date of Concomitant Medication
5. **MHDECOD**: Medical History Term (Standardized)
6. **MHSTDTC**: Start Date of Medical History Event
7. **Overlap**: A derived column indicating if the Concomitant Medication overlaps with a Medical History event (Yes/No).

### Steps:
1. Import and prepare the datasets.
2. Merge CM and MH domains on `USUBJID`.
3. Derive the `Overlap` column based on date comparisons.
4. Create the final listing.

```python
import pandas as pd

# Sample DataFrames for CM and MH
cm_data = {
    'USUBJID': ["SUBJ001", "SUBJ002", "SUBJ003"],
    'CMTRT': ["Paracetamol", "Ibuprofen", "Aspirin"],
    'CMINDC': ["Fever", "Pain", "Blood Thinner"],
    'CMSTDTC': ["2024-01-01", "2024-02-10", "2024-03-15"]
}
mh_data = {
    'USUBJID': ["SUBJ001", "SUBJ002", "SUBJ003", "SUBJ004"],
    'MHDECOD': ["Hypertension", "Arthritis", "Heart Disease", "Diabetes"],
    'MHSTDTC': ["2023-12-15", "2024-02-01", "2024-03-10", "2024-01-05"]
}

# Create DataFrames
cm_df = pd.DataFrame(cm_data)
mh_df = pd.DataFrame(mh_data)

# Convert date columns to datetime
to_datetime_cols = ['CMSTDTC', 'MHSTDTC']
for df, cols in [(cm_df, ['CMSTDTC']), (mh_df, ['MHSTDTC'])]:
    for col in cols:
        df[col] = pd.to_datetime(df[col])

# Merge CM and MH on USUBJID
merged_df = pd.merge(cm_df, mh_df, on='USUBJID', how='inner')

# Derive Overlap column
merged_df['Overlap'] = merged_df.apply(
    lambda row: "Yes" if row['CMSTDTC'] >= row['MHSTDTC'] else "No", axis=1
)

# Select and rename columns for final listing
final_listing = merged_df[[
    'USUBJID', 'CMTRT', 'CMINDC', 'CMSTDTC', 'MHDECOD', 'MHSTDTC', 'Overlap'
]].rename(columns={
    'USUBJID': 'Subject ID',
    'CMTRT': 'Concomitant Medication',
    'CMINDC': 'Indication',
    'CMSTDTC': 'CM Start Date',
    'MHDECOD': 'Medical History Term',
    'MHSTDTC': 'MH Start Date'
})

# Display the final listing
print(final_listing)
```

### Explanation:
1. **Data Preparation**:
   - Convert date columns to `datetime` format for accurate comparisons.
2. **Merging DataFrames**:
   - Perform an inner join on `USUBJID` to align CM and MH data for each subject.
3. **Deriving Overlap**:
   - Compare `CMSTDTC` (CM Start Date) and `MHSTDTC` (MH Start Date) to determine if the Concomitant Medication overlaps with a Medical History event.
4. **Final Listing**:
   - Select and rename columns to create a clear, readable listing.

### Output:
The resulting DataFrame (`final_listing`) will look like this:

| Subject ID | Concomitant Medication | Indication   | CM Start Date | Medical History Term | MH Start Date | Overlap |
|------------|-------------------------|--------------|---------------|-----------------------|---------------|---------|
| SUBJ001    | Paracetamol            | Fever        | 2024-01-01    | Hypertension         | 2023-12-15    | Yes     |
| SUBJ002    | Ibuprofen              | Pain         | 2024-02-10    | Arthritis            | 2024-02-01    | Yes     |
| SUBJ003    | Aspirin                | Blood Thinner| 2024-03-15    | Heart Disease        | 2024-03-10    | Yes     |

This listing provides insights into the relationship between concomitant medications and medical history events for each subject.
