# Lesson 3: Python Pandas Tutorial - Advanced Analysis with Clinical Data

## Objective:
Perform advanced analysis on clinical datasets using Python Pandas. Specifically, create a listing that:
1. Filters subjects who experienced an Adverse Event (AE) and later died.
2. Computes the duration between AE End Date and Death Date.
3. Adds the following columns:
   - `Subject ID`
   - `AETERM`
   - `AEDECODE`
   - `AEENDDATE`
   - `DDDAT` (Death Date)
   - `Duration (days)`
   - `RFICDT` (Reference Start Date)

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
    'AEENDDATE': ["2024-01-10", "2024-02-10", "2024-03-15"]
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

# Convert date columns to datetime
ae_df['AEENDDATE'] = pd.to_datetime(ae_df['AEENDDATE'])
dd_df['DDDAT'] = pd.to_datetime(dd_df['DDDAT'])
dm_df['RFICDT'] = pd.to_datetime(dm_df['RFICDT'])

# Merge DataFrames
merged_df = pd.merge(ae_df, dd_df, on='USUBJID', how='inner')
merged_df = pd.merge(merged_df, dm_df, on='USUBJID', how='left')

# Calculate Duration between AE End Date and Death Date
merged_df['Duration (days)'] = (merged_df['DDDAT'] - merged_df['AEENDDATE']).dt.days

# Select and rename columns
final_listing = merged_df[[
    'USUBJID', 'AETERM', 'AEDECODE', 'AEENDDATE', 'DDDAT', 'Duration (days)', 'RFICDT'
]]
final_listing.rename(columns={
    'USUBJID': 'Subject ID'
}, inplace=True)

# Display the final listing
print(final_listing)
```

### Explanation:
1. **Data Preparation**:
   - Convert all date columns to `datetime` format for accurate calculations.
2. **Data Merging**:
   - Inner join AE and DD domains to filter subjects who experienced an AE and later died.
   - Left join with DM to include reference start dates.
3. **Duration Calculation**:
   - Calculate the difference between `DDDAT` (Death Date) and `AEENDDATE` (AE End Date) in days.
4. **Final Listing**:
   - Select and rename columns for better readability.

### Output:
The resulting DataFrame (`final_listing`) will look like this:

| Subject ID | AETERM    | AEDECODE   | AEENDDATE   | DDDAT      | Duration (days) | RFICDT     |
|------------|-----------|------------|-------------|------------|-----------------|------------|
| SUBJ001    | Headache  | HEADACHE   | 2024-01-10  | 2024-01-15 | 5               | 2023-12-25 |
| SUBJ002    | Nausea    | NAUSEA     | 2024-02-10  | 2024-02-12 | 2               | 2024-01-15 |

This listing provides critical insights into subjects who died after experiencing an AE and the duration between these events.
