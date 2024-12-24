# Lesson 1: Python Pandas Tutorial - Checking Deaths After AE Reported Event Ends

## Objective:
Create a listing that checks if there are any deaths occurring after the AE (Adverse Event) reported event ends. Add a column `Message` with "Yes" or "No" based on the presence of death after AE end. Include the following columns in the final listing:

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
import pandas as pd

# Sample DataFrames for AE, DD, and DM
ae_data = {
    'USUBJID': ["SUBJ001", "SUBJ002", "SUBJ003"],
    'AETERM': ["Headache", "Nausea", "Dizziness"],
    'AEDECODE': ["HEADACHE", "NAUSEA", "DIZZINESS"],
    'AESTARTDATE': ["2024-01-01", "2024-02-01", "2024-03-01"],
    'AEENDDATE': ["2024-01-10", "2024-02-10", "2024-03-15"],
    'AE_LOG': ["Log1", "Log2", "Log3"]
}
dd_data = {
    'USUBJID': ["SUBJ001", "SUBJ002", "SUBJ004"],
    'DDDAT': ["2024-01-15", "2024-02-12", "2024-04-01"]
}
dm_data = {
    'USUBJID': ["SUBJ001", "SUBJ002", "SUBJ003", "SUBJ004"],
    'RFICDT': ["2023-12-25", "2024-01-15", "2024-02-20", "2024-03-01"]
}

# Create DataFrames
ae_df = pd.DataFrame(ae_data)
dd_df = pd.DataFrame(dd_data)
dm_df = pd.DataFrame(dm_data)

# Convert dates to datetime format
ae_df['AESTARTDATE'] = pd.to_datetime(ae_df['AESTARTDATE'])
ae_df['AEENDDATE'] = pd.to_datetime(ae_df['AEENDDATE'])
dd_df['DDDAT'] = pd.to_datetime(dd_df['DDDAT'])
dm_df['RFICDT'] = pd.to_datetime(dm_df['RFICDT'])

# Merge AE, DD, and DM domains
ae_dd_df = pd.merge(ae_df, dd_df, on='USUBJID', how='left')
final_df = pd.merge(ae_dd_df, dm_df, on='USUBJID', how='left')

# Calculate Maximum Disposition Date
final_df['MaxDispositionDate'] = final_df['DDDAT']

# Check if death occurred after AE end date
final_df['Message'] = final_df.apply(
    lambda row: "Yes" if pd.notna(row['DDDAT']) and row['DDDAT'] > row['AEENDDATE'] else "No",
    axis=1
)

# Select required columns
final_listing = final_df[[
    'USUBJID', 'AETERM', 'AEDECODE', 'AESTARTDATE', 'AEENDDATE', 'AE_LOG',
    'MaxDispositionDate', 'RFICDT', 'Message'
]]

# Rename columns for clarity
final_listing.rename(columns={
    'AE_LOG': 'AE Logline',
    'MaxDispositionDate': 'Maximum Disposition Date'
}, inplace=True)

print(final_listing)
```

### Explanation:
1. **Data Preparation**:
   - Convert all date columns to `datetime` for accurate comparisons.
2. **Merging DataFrames**:
   - Use `pd.merge` to join the AE, DD, and DM domains on `USUBJID`.
3. **Calculating `Message`**:
   - Use `apply` with a lambda function to check if `DDDAT` (Death Date) is after `AEENDDATE`.
   - Assign "Yes" if death occurs after AE ends; otherwise, "No".
4. **Final Listing**:
   - Select required columns and rename them for better readability.

### Output:
The resulting DataFrame (`final_listing`) will look like this:

| USUBJID  | AETERM     | AEDECODE   | AESTARTDATE | AEENDDATE   | AE Logline | Maximum Disposition Date | RFICDT     | Message |
|----------|------------|------------|-------------|-------------|------------|---------------------------|------------|---------|
| SUBJ001  | Headache   | HEADACHE   | 2024-01-01  | 2024-01-10  | Log1       | 2024-01-15                | 2023-12-25 | Yes     |
| SUBJ002  | Nausea     | NAUSEA     | 2024-02-01  | 2024-02-10  | Log2       | 2024-02-12                | 2024-01-15 | Yes     |
| SUBJ003  | Dizziness  | DIZZINESS  | 2024-03-01  | 2024-03-15  | Log3       | NaT                       | 2024-02-20 | No      |
