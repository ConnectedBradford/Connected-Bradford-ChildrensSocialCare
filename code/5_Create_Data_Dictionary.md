# How to create a data dictionary for your FDM

## Prerequisites

##### Install Notes 
Before you do anything else, if you haven't already, please open a terminal on the Jupyter Launcher and run the below (you'll only have to do this once):

conda install -c conda-forge scipy

and then: 

conda install -c conda-forge tqdm

#### Import the packages you've already installed


```python
# Intsall required packages
from google.cloud import bigquery
import pandas as pd
from tqdm import tqdm
```

#### Please read before you the below script

Before you run the script in the cell below, please scroll down to the bottom and change the name of your dataset_id in the cell beneath this one. This is the part in brackets after 'create_data_dict'
Your dataset_id is the name of the dataset - the part that follows: `yhcr-prd-phm-bia-core.`


```python
def get_num_description_column(col_name, table_name):
    """
    Takes a column name and a table name, returning a string with descriptive 
    statistics for the column specified.

    Calculates the mean, median, max, min and IQR for the specified column using 
    BigQuery SQL and returns a string with the results concatenated together.

    Args:
        col_name (str): The name of a the numeric column.
        table_name (str): The name of the table containing the specified column.

    Returns:
        str: A string with the mean, median, max, min and IQR for the specified column.
    """
    sql_query = f"""
        WITH stats AS (
            SELECT
                AVG({col_name}) AS mean,
                MAX({col_name}) AS max,
                MIN({col_name}) AS min,
                APPROX_QUANTILES({col_name}, 2)[OFFSET(1)] AS median,
                APPROX_QUANTILES({col_name}, 4)[OFFSET(1)] AS q1,
                APPROX_QUANTILES({col_name}, 4)[OFFSET(3)] AS q3
            FROM `{table_name}`
        )
        SELECT CONCAT('Mean: ', CAST(mean AS STRING),  
                      ', Median: ', CAST(median AS STRING), 
                      ', Max: ', CAST(max AS STRING), 
                      ', Min: ', CAST(min AS STRING),  
                      ', IQR: ', CAST(q3 - q1 AS STRING))
        FROM stats
    """
    return pd.read_gbq(sql_query).iloc[0,0]


def get_date_description_column(col_name, table_name):
    """
    Takes a column name and a table name, returning a string with the min and max 
    dates.

    Calculates the min and max dates for the specified column using BigQuery SQL 
    and returns a string with the results concatenated together.

    Args:
        col_name (str): The name of a date column.
        table_name (str): The name of the table containing the specified column.

    Returns:
        str: A string with the min and max dates for the specified column.
    """
    sql_query = f"""
        WITH stats AS (
            SELECT 
                MAX({col_name}) AS max_date, 
                MIN({col_name}) AS min_date 
            FROM `{table_name}`
        )
        SELECT
            CONCAT('From: ', CAST(min_date AS STRING), 
                   ' To: ', CAST(max_date AS STRING))
        FROM stats
    """
    return pd.read_gbq(sql_query).iloc[0,0]


def get_bool_description_column(col_name, table_name):
    """
    Takes a column name and a table name, returning a the count of `True` and 
    `False` values.

    Calculates the count of `True` and `False` values for the specified column 
    using BigQuery SQL and returns a string with the results concatenated 
    together.

    Args:
        col_name (str): The name of the boolean column.
        table_name (str): The name of the table containing the specified column.

    Returns:
        str: A string with the count of `True` and `False` values for the 
             specified column.
    """
    sql_query = f"""
        WITH stats AS (
            SELECT
                COUNTIF({col_name} = TRUE) AS true_count,
                COUNTIF({col_name} = FALSE) AS false_count
            FROM `{table_name}`
        )
        SELECT
            CONCAT('False: ', CAST(false_count AS STRING), 
                   ', True: ', CAST(true_count AS STRING))
        FROM stats
    """
    return pd.read_gbq(sql_query).iloc[0,0]


def get_string_description_column(col_name, table_name):
    sql_query = f"""
        WITH top_entries AS (
            SELECT {col_name}, COUNT(*) AS count
            FROM `{table_name}`
            GROUP BY {col_name}
            ORDER BY count DESC
            LIMIT 5 
        ),
        total_entries AS (
            SELECT COUNT(DISTINCT {col_name}) AS total_count
            FROM `{table_name}` 
        )
        SELECT IF((SELECT total_count FROM total_entries) > 5, 
                   CONCAT('Top 5: ', STRING_AGG(CONCAT({col_name}, ': ', 
                          CAST(count AS STRING)), ', ')), 
                   STRING_AGG(CONCAT({col_name}, ': ', 
                              CAST(count AS STRING)), ', '))
        FROM top_entries
    """
    return pd.read_gbq(sql_query).iloc[0,0]


def create_data_dict(dataset_id):
    
    """
    Create a data dictionary table for a BigQuery dataset.
    
    Takes the ID of a BigQuery dataset and creates a data_dict table in the same 
    dataset. `data_dict` contains information about tables in the 
    dataset with names prefixed " tbl_" or "cb_":
    table name, column name, data type, and a summary 
    description of each column. `description` column includes summary statistics 
    for numeric columns (mean, median, IQR, min,  max), the number of unique 
    values and top 5 values for string columns, the  date range for date 
    columns, and the count of True and False values for boolean columns. 
    
    Args:
        dataset_id (str): The ID of the BigQuery dataset.
        
    Output:
        None - `data_dict` table is uploaded to biqquery dataset at "dataset_id"
    """
    
    client = bigquery.Client()
    dataset_ref = client.dataset(dataset_id)
    tables = list(client.list_tables(dataset_ref))
    rows = []
    table_count = 0
    output_dict = {
        "table_name": [],
        "column_name": [],
        "data_type": [],
        "description": []
    }
    for table in tables:
        if table.table_id.startswith("tbl_") or table.table_id.startswith("cb_"):
            table_count += 1
            print(f"Processing table {table_count} of {len(tables)}: {table.table_id}")
            table_ref = dataset_ref.table(table.table_id)
            table = client.get_table(table_ref)
            for schema_field in tqdm(table.schema):
                output_dict["table_name"].append(table.table_id)
                output_dict["column_name"].append(schema_field.name)
                output_dict["data_type"].append(schema_field.field_type)
                full_table_id = f"{dataset_id}.{table.table_id}"
                if schema_field.field_type == "STRING":
                    output_dict["description"].append(
                        get_string_description_column(schema_field.name, 
                                                      full_table_id) 
                    )
                elif schema_field.field_type in ["INTEGER", "FLOAT", "NUMERIC"]:
                    output_dict["description"].append(
                        get_num_description_column(schema_field.name, 
                                                      full_table_id) 
                    )
                elif schema_field.field_type in ["DATE", "TIMESTAMP", "DATETIME"]:
                    output_dict["description"].append(
                        get_date_description_column(schema_field.name, 
                                                      full_table_id) 
                    )
                elif schema_field.field_type in ["BOOL", "BOOLEAN"]:
                    output_dict["description"].append(
                        get_bool_description_column(schema_field.name, 
                                                      full_table_id) 
                    )
    output_df = pd.DataFrame(output_dict)
    output_df.to_gbq(f"{dataset_id}.data_dictionary", progress_bar=False)
    print("Finished creating data_dict table")
```

##### Change the below cell before you do anything else


```python
create_data_dict("CB_FDM_AdultSocialCare")
```

    Processing table 1 of 12: tbl_assessments


    100%|██████████| 8/8 [00:07<00:00,  1.11it/s]


    Processing table 2 of 12: tbl_assessments_data_dict


    100%|██████████| 3/3 [00:02<00:00,  1.19it/s]


    Processing table 3 of 12: tbl_contacts


    100%|██████████| 11/11 [00:09<00:00,  1.21it/s]


    Processing table 4 of 12: tbl_contacts_data_dict


    100%|██████████| 3/3 [00:02<00:00,  1.23it/s]


    Processing table 5 of 12: tbl_services


    100%|██████████| 10/10 [00:07<00:00,  1.42it/s]


    Processing table 6 of 12: tbl_services_data_dict


    100%|██████████| 3/3 [00:02<00:00,  1.11it/s]


    Finished creating data_dict table



```python
print(create_data_dict)
```

    <function create_data_dict at 0x7fc5c1adc320>


#### Now print the data dictionary to view it

The data_dictionary is now available to use and view in wihtin your dataset. If you would like to display the data dictionary in Vertex, follow the instructions below


```python
#Print table
# Make sure to change the name of your dataset_id below before running this cell
# Tip:if you just want to dispaly a few rows to see how it looks, remove the '#' by 'LIMIT 10' under the below query
%%bigquery
SELECT
*
FROM
`yhcr-prd-phm-bia-core.CB_FDM_AdultSocialCare.data_dictionary`
#LIMIT 10
```


    Query is running:   0%|          |



    Downloading:   0%|          |





<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>table_name</th>
      <th>column_name</th>
      <th>data_type</th>
      <th>description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>tbl_assessments</td>
      <td>AssessmentDate</td>
      <td>STRING</td>
      <td>Top 5: 26/02/2019: 106, 29/11/2017: 101, 16/07...</td>
    </tr>
    <tr>
      <th>1</th>
      <td>tbl_assessments</td>
      <td>AssessType</td>
      <td>STRING</td>
      <td>Top 5: Adults Assessment: 25644, OT Adult Asse...</td>
    </tr>
    <tr>
      <th>2</th>
      <td>tbl_assessments</td>
      <td>IDAssessment</td>
      <td>STRING</td>
      <td>Top 5: 226466: 1, 128947: 1, 231275: 1, 225229...</td>
    </tr>
    <tr>
      <th>3</th>
      <td>tbl_assessments</td>
      <td>MonthAndYearOfBirth</td>
      <td>STRING</td>
      <td>Top 5: Mar-34: 182, Jan-36: 181, Sep-29: 178, ...</td>
    </tr>
    <tr>
      <th>4</th>
      <td>tbl_assessments</td>
      <td>PostCodeDistric</td>
      <td>STRING</td>
      <td>Top 5: BD6 : 2903, BD20 : 2653, BD4 : 2556, BD...</td>
    </tr>
    <tr>
      <th>5</th>
      <td>tbl_assessments_data_dict</td>
      <td>variable_name</td>
      <td>STRING</td>
      <td>Top 5: MonthAndYearOfBirth: 1, AssessType: 1, ...</td>
    </tr>
    <tr>
      <th>6</th>
      <td>tbl_assessments_data_dict</td>
      <td>data_type</td>
      <td>STRING</td>
      <td>STRING: 5, DATETIME: 2, INTEGER: 1</td>
    </tr>
    <tr>
      <th>7</th>
      <td>tbl_assessments_data_dict</td>
      <td>description</td>
      <td>STRING</td>
      <td>Top 5: 1179 Unique Values - Examples: Nov-49, ...</td>
    </tr>
    <tr>
      <th>8</th>
      <td>tbl_contacts</td>
      <td>ContactDate</td>
      <td>STRING</td>
      <td>Top 5: 28/01/2019: 240, 12/02/2019: 235, 11/02...</td>
    </tr>
    <tr>
      <th>9</th>
      <td>tbl_contacts</td>
      <td>ContactSource</td>
      <td>STRING</td>
      <td>Top 5: Self Referral: 38587, Family/Friend/Nei...</td>
    </tr>
    <tr>
      <th>10</th>
      <td>tbl_contacts</td>
      <td>ContactRoute</td>
      <td>STRING</td>
      <td>Top 5: Community / Other Route: 86724, Hospita...</td>
    </tr>
    <tr>
      <th>11</th>
      <td>tbl_contacts</td>
      <td>ContactReason</td>
      <td>STRING</td>
      <td>Top 5: Access Point Screening: 46886, Blue Bad...</td>
    </tr>
    <tr>
      <th>12</th>
      <td>tbl_contacts</td>
      <td>ContactOutcome</td>
      <td>STRING</td>
      <td>Top 5: Progress to Assessment: 20041, Advice/I...</td>
    </tr>
    <tr>
      <th>13</th>
      <td>tbl_contacts</td>
      <td>IDContact</td>
      <td>STRING</td>
      <td>Top 5: 626325: 1, 619134: 1, 619136: 1, 619135: 1</td>
    </tr>
    <tr>
      <th>14</th>
      <td>tbl_contacts</td>
      <td>MonthAndYearOfBirth</td>
      <td>STRING</td>
      <td>Top 5: Jan-37: 361, Jan-35: 337, Jan-38: 332, ...</td>
    </tr>
    <tr>
      <th>15</th>
      <td>tbl_contacts</td>
      <td>PostCodeDistrict</td>
      <td>STRING</td>
      <td>Top 5: BD6 : 7568, BD4 : 7207, BD8 : 6856, BD2...</td>
    </tr>
    <tr>
      <th>16</th>
      <td>tbl_contacts_data_dict</td>
      <td>variable_name</td>
      <td>STRING</td>
      <td>Top 5: src_PostCodeDistrict: 1, src_MonthAndYe...</td>
    </tr>
    <tr>
      <th>17</th>
      <td>tbl_contacts_data_dict</td>
      <td>data_type</td>
      <td>STRING</td>
      <td>STRING: 8, INTEGER: 1, DATETIME: 1</td>
    </tr>
    <tr>
      <th>18</th>
      <td>tbl_contacts_data_dict</td>
      <td>description</td>
      <td>STRING</td>
      <td>Top 5: 440 Unique Values - Examples: BD8 , BD1...</td>
    </tr>
    <tr>
      <th>19</th>
      <td>tbl_services</td>
      <td>Servicestartdate</td>
      <td>STRING</td>
      <td>Top 5: 04/04/2016: 2278, 22/08/2016: 828, 25/0...</td>
    </tr>
    <tr>
      <th>20</th>
      <td>tbl_services</td>
      <td>ServiceEndDate</td>
      <td>STRING</td>
      <td>Top 5: 24/02/2019: 522, 02/04/2017: 373, 15/12...</td>
    </tr>
    <tr>
      <th>21</th>
      <td>tbl_services</td>
      <td>CPLIid</td>
      <td>STRING</td>
      <td>Top 5: 15466: 1, 7463: 1, 3257: 1, 23410: 1, 4...</td>
    </tr>
    <tr>
      <th>22</th>
      <td>tbl_services</td>
      <td>ServiceType</td>
      <td>STRING</td>
      <td>Top 5: Home Care: 14104, Reablement: 7772, Sha...</td>
    </tr>
    <tr>
      <th>23</th>
      <td>tbl_services</td>
      <td>FinalisedPSR</td>
      <td>STRING</td>
      <td>Top 5: Physical Support - Personal Care Suppor...</td>
    </tr>
    <tr>
      <th>24</th>
      <td>tbl_services</td>
      <td>MonthAndYearOfBirth</td>
      <td>STRING</td>
      <td>Top 5: May-86: 406, Jun-76: 376, Jan-79: 359, ...</td>
    </tr>
    <tr>
      <th>25</th>
      <td>tbl_services</td>
      <td>PostCodeDistrict</td>
      <td>STRING</td>
      <td>Top 5: BD20 : 2687, BD9 : 2625, BD6 : 2611, BD...</td>
    </tr>
    <tr>
      <th>26</th>
      <td>tbl_services_data_dict</td>
      <td>variable_name</td>
      <td>STRING</td>
      <td>Top 5: src_CPLIid: 1, src_PostCodeDistrict: 1,...</td>
    </tr>
    <tr>
      <th>27</th>
      <td>tbl_services_data_dict</td>
      <td>data_type</td>
      <td>STRING</td>
      <td>STRING: 7, DATETIME: 2, INTEGER: 1</td>
    </tr>
    <tr>
      <th>28</th>
      <td>tbl_services_data_dict</td>
      <td>description</td>
      <td>STRING</td>
      <td>Top 5: 40965 Unique Values - Examples: 63557, ...</td>
    </tr>
    <tr>
      <th>29</th>
      <td>tbl_assessments</td>
      <td>person_id</td>
      <td>INTEGER</td>
      <td>Mean: 12802846.5251679, Median: 13064718, Max:...</td>
    </tr>
    <tr>
      <th>30</th>
      <td>tbl_contacts</td>
      <td>person_id</td>
      <td>INTEGER</td>
      <td>Mean: 12607032.577930043, Median: 13034506, Ma...</td>
    </tr>
    <tr>
      <th>31</th>
      <td>tbl_services</td>
      <td>person_id</td>
      <td>INTEGER</td>
      <td>Mean: 12714624.158696322, Median: 13055822, Ma...</td>
    </tr>
    <tr>
      <th>32</th>
      <td>tbl_assessments</td>
      <td>tbl_assessments_start_date</td>
      <td>DATETIME</td>
      <td>From: 2017-01-02 00:00:00 To: 2019-12-30 00:00:00</td>
    </tr>
    <tr>
      <th>33</th>
      <td>tbl_assessments</td>
      <td>tbl_assessments_end_date</td>
      <td>DATETIME</td>
      <td>From: 2017-01-02 00:00:00 To: 2019-12-30 00:00:00</td>
    </tr>
    <tr>
      <th>34</th>
      <td>tbl_contacts</td>
      <td>tbl_contacts_start_date</td>
      <td>DATETIME</td>
      <td>From: 2017-01-01 00:00:00 To: 2019-12-30 00:00:00</td>
    </tr>
    <tr>
      <th>35</th>
      <td>tbl_contacts</td>
      <td>tbl_contacts_end_date</td>
      <td>DATETIME</td>
      <td>From: 2017-01-01 00:00:00 To: 2019-12-30 00:00:00</td>
    </tr>
    <tr>
      <th>36</th>
      <td>tbl_services</td>
      <td>tbl_services_start_date</td>
      <td>DATETIME</td>
      <td>From: 2008-08-22 00:00:00 To: 2019-12-31 00:00:00</td>
    </tr>
    <tr>
      <th>37</th>
      <td>tbl_services</td>
      <td>tbl_services_end_date</td>
      <td>DATETIME</td>
      <td>From: 2017-01-01 00:00:00 To: 2020-09-01 00:00:00</td>
    </tr>
  </tbody>
</table>
</div>




```python
#Altenrative option to save and print as a dataframe
from google.cloud import bigquery

bqclient = bigquery.Client()
table = bigquery.TableReference.from_string("CB_FDM_AdultSocialCare.data_dictionary")
rows = bqclient.list_rows(
    table,
    selected_fields=[
        bigquery.SchemaField("table_name", "STRING"),
        bigquery.SchemaField("column_name", "STRING"),
        bigquery.SchemaField("data_type", "STRING"),
        bigquery.SchemaField("description", "STRING"),
    ],
)
data_dictionary_dataframe = rows.to_dataframe()

print(data_dictionary_dataframe.head())                     
```
