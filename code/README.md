# Background notes

#### person and observation_period tables used building the file: 'fdm_builder_basics_tutorial.ipynb' - which can be found in the code folder

#### care_site is not applicable for this dataset, as all are Bradford Council

#### data_dictionary created using the data dictionary script: '5_Create_Data_Dictionary' which can be found in the code folder


# visit_occurence template

The below is built using BigQuery SQL syntax

Each visit visit_occurence table will be unique to dataset, so you may not have to follow the earlier steps of the process below.
Only begin this work when the person and observation tables have been built and source tables reformatted and validated.  


# Prep table

I created an ‘eventdatetime’ table first from all source tables to UNION ALL dates as well as concatenating date and time fields and into the required FDM format from strings, which is "%Y%m%d%H:%M" or can also be "%Y%m%d%T" – this may not be required  to concatenate any fields for your visit_occurence build. You may have to if the TIME and DATE fields are separate in the source tables. 

# Part 1 - UNION ALL source tables on time and/or date fields

If there is no end date field, use start date for this as well

CREATE TABLE `yhcr-prd-phm-bia-core.DATASET_ID.tbl_eventdatetime `
AS
SELECT
person_id,
safe.parse_datetime ("%Y%m%d%H:%M ", [INSERT START DATE FIELD] || IFNULL(NULLIF(TRIM(start_date_field), ''), '00:00'))  eventstartdatetime,
safe.parse_datetime("%Y%m%d%T", [INSERT END DATE FIELD] || IFNULL(NULLIF(TRIM(end_date_field), ''), '00:00')) eventenddatetime,
[ANY_RECORD IDENFIFIER FIELD] as record_identifier
 '[INSERT SOURCE TABLE NAME]' as source_table       
  FROM `yhcr-prd-phm-bia-core.DATASET_ID.tbl_source_table` 
  where person_id is not null
UNION ALL

Continue above step for as many step tables as you have. UNION ALL function requires the same number of fields, using the same naming throughout in your script. 
Record identifier is not essential. Remove in above and below steps if not possible.


# Part 2 - Validating datetimes

As an extra measure, I ran the newly created ‘eventdatetime’ through the FDM Python tool. This, should remove any invalid dates in relation to a person’s birth and death datetimes. This produces the fields you can see below: fdm_start_date and fdm_end_date

CREATE TABLE
`yhcr-prd-phm-bia-core.DATASET_ID_eventdates_valid`
as
select distinct a.person_id
, a.fdm_start_date
, a.fdm_end_date
,eventstartdatetime
,eventenddatetime
,record_identifier 
,source_table
from  `yhcr-prd-phm-bia-core.DATASET_ID.tbl_eventdatetime` a
,`yhcr-prd-phm-bia-core.DATASET_ID.person` e 
where e.person_id = a.person_id 
and e.death_datetime is null 
and a.fdm_start_date >= e.birth_datetime
and a.fdm_end_date <= e.birth_datetime
and a.fdm_start_date <= (Select extract_date from `yhcr-prd-phm-bia-core.CY_LOOKUPS.tbl_Dataset_ExtractDateRef` where DataSetName = 'DATASET_ID' )
ORDER BY 
person_id asc

# Part 3 - Create visit_occurence table

You’ll need to do a bit of research via Athena  to determine the visit_concept_id. For example, I’ve renamed any event (‘visit’)  from an outpatient table ‘9202’ as ‘9202’ = a standard outpatient visit on Athena. Similarly, 9201 is Inpatient and 9203 is Emergency Room.

https://athena.ohdsi.org/search-terms/terms?domain=Visit&page=1&pageSize=15&query= 

Background on definitions here: https://ohdsi.github.io/CommonDataModel/cdm60.html#VISIT_OCCURRENCE 

For care_site_id You will want to check the care_site_id in the Master FDM dataset and assign here or add to this script to assign multiple care_site_ids if there is more than one in your dataset.

As well as using the ‘source_table’ field to create the visit_concept_id I’ve used it to define visit_source_value – this is the text value that matches the above visit_type_concept_id.

The below is an example for BRI

CREATE TABLE `yhcr-prd-phm-bia-core.DATASET_ID.visit_occurence`
AS
SELECT  
DENSE_RANK() OVER(ORDER BY a.person_id, fdm_start_date) as visit_occurence_id,
a.person_id, 
case
when source_table is null then 'null'
when source_table = 'tbl_op_susplus' then '9202'
when source_table = 'SUS_BRI_OUTPATIENTS_15_19'then '9202'
when source_table = 'SUS_BRI_OUTPATIENTS_20_22' then '9202'
when source_table = 'SUS_BRI_APC_20_22' then '9201'
when source_table = 'SUS_BRI_APC_15_19_pt1'then '9201'
when source_table = 'tbl_apc_finished_susplus' then '9201'
when source_table = 'tbl_ec_backward_compatible_susplus' then '9203'
when source_table = 'SUS_BRI_EC_BackwardCompatible_15_19' then '9203'
when source_table = 'SUS_BRI_EC_BackwardCompatible_2020_2022' then '9203'
end as visit_concept_id, 
EXTRACT(DATE FROM fdm_start_date) as visit_start_date,
fdm_start_date as visit_start_datetime,
EXTRACT(DATE FROM fdm_end_date) as visit_end_date,
fdm_end_date as visit_end_datetime,
null as visit_type_concept_id,
null as provider_id, 
204 as care_site_id, 
case
when source_table is null then 'null'
when source_table = 'tbl_op_susplus' then 'Outpatient Visit'
when source_table = 'SUS_BRI_OUTPATIENTS_15_19'then 'Outpatient Visit'
when source_table = 'SUS_BRI_OUTPATIENTS_20_22' then 'Outpatient Visit'
when source_table = 'SUS_BRI_APC_20_22' then 'Inpatient Visit'
when source_table = 'SUS_BRI_APC_15_19_pt1'then 'Inpatient Visit'
when source_table = 'tbl_apc_finished_susplus' then 'Inpatient Visit'
when source_table = 'tbl_ec_backward_compatible_susplus' then 'Emergency Room Visit'
when source_table = 'SUS_BRI_EC_BackwardCompatible_15_19' then 'Emergency Room Visit'
when source_table = 'SUS_BRI_EC_BackwardCompatible_2020_2022' then 'Emergency Room Visit'
end as visit_source_value, 
0 as visit_source_concept_id, 
0 as admitted_from_concept_id,
null as admitted_from_source_value,
0 as discharge_to_concept_id,
null as discharge_to_source_value,  
null as preceding_visit_occurrence_id,
Generated_Record_Identifier
FROM
  `yhcr-prd-phm-bia-core.CY_MYSPACE_RS.bri_sus_fdm_eventdates_valid_v2` a
  inner join `yhcr-prd-phm-bia-core.CY_FDM_SUS_BRI.person` b
  on a.person_id = b.person_id
  ORDER BY a.person_id, visit_occurence_id


