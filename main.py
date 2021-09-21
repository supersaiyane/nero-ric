#!/usr/bin/python3.9
import datetime
from google.cloud import bigquery
from datetime import date

target_data_project_id = 'som-nero-phi-naras-ric'

target_data_dataset_id = 'dev_naras_ric_pat_info_latest'
target_data_bucket = 'dev-naras-ric-pat-info-bucket-latest'

source_data_project_id = 'som-rit-phi-starr-prod'
source_data_dataset_id = 'shc_clarity_filtered_latest'
source_data_pat_dataset_id = 'stride'
date_to = date.today() - datetime.timedelta(days=1)
date_to_str = date_to.strftime("%Y-%m-%d")

date_from = date.today() - datetime.timedelta(days=7)
date_from_str = date_from.strftime("%Y-%m-%d")

def create_pdm():
    print ("--create_pdm")

    client = bigquery.Client(project=source_data_project_id)
    
    sql = """
	CREATE OR REPLACE TABLE `{target_data_project_id}.{target_data_dataset_id}.yn_nod_15days_pdm` AS
	SELECT 
	  op.pat_id, p.pat_map_id, p.mrn,
	  COALESCE (r.result_time, op.order_time) taken_time,
	  FLOOR(DATE_DIFF (r.result_time, p.birth_date, MONTH)/12) age,
	  c.name lab_name,
	  c.common_name group_lab_name,
	  c.base_name,
	  ord_num_value,
	  r.reference_unit
	FROM `{source_data_project_id}.{source_data_dataset_id}.order_results` r 
    JOIN `{source_data_project_id}.{source_data_dataset_id}.clarity_component` c ON r.component_id = c.component_id
	JOIN `{source_data_project_id}.{source_data_dataset_id}.order_proc` op ON r.order_proc_id = op.order_proc_id
	JOIN `{source_data_project_id}.{source_data_dataset_id}.order_proc_3` op3 ON op.order_proc_id = op3.order_id
	JOIN `{source_data_project_id}.{source_data_dataset_id}.zc_ordering_mode` zom ON op3.ordering_mode_c = zom.ordering_mode_c
	JOIN `{source_data_project_id}.{source_data_pat_dataset_id}.pat_map` p ON UPPER(op.pat_id) = p.shc_pat_id
	WHERE c.base_name = 'A1C'
	AND r.Result_Status_C = 3
	AND FLOOR(DATE_DIFF (COALESCE (r.result_time, op.order_time), p.birth_date, MONTH)/12) BETWEEN 50 AND 85
	AND COALESCE (r.result_time, op.order_time) BETWEEN '{date_from}' AND '{date_to}'
	AND zom.name       = 'Outpatient'
	AND ORD_NUM_VALUE >= 6.5  

	UNION ALL

    SELECT 
	  op.pat_id, p.pat_map_id, p.mrn,
	  COALESCE (r.result_time, op.order_time) taken_time,
	  FLOOR(DATE_DIFF (r.result_time, p.birth_date, MONTH)/12) age,
	  c.name lab_name,
	  c.common_name group_lab_name,
	  c.base_name,
	  ord_num_value,
	  r.reference_unit
	FROM `{source_data_project_id}.{source_data_dataset_id}.order_results` r 
    JOIN `{source_data_project_id}.{source_data_dataset_id}.clarity_component` c ON r.component_id = c.component_id
	JOIN `{source_data_project_id}.{source_data_dataset_id}.order_proc` op ON r.order_proc_id = op.order_proc_id
	JOIN `{source_data_project_id}.{source_data_dataset_id}.order_proc_3` op3 ON op.order_proc_id = op3.order_id
	JOIN `{source_data_project_id}.{source_data_dataset_id}.zc_ordering_mode` zom ON op3.ordering_mode_c = zom.ordering_mode_c
	JOIN `{source_data_project_id}.{source_data_pat_dataset_id}.pat_map` p ON UPPER(op.pat_id) = p.shc_pat_id
	WHERE c.BASE_NAME   = 'GLUF'
	AND r.Result_Status_C = 3
	AND FLOOR(DATE_DIFF (COALESCE (r.result_time, op.order_time), p.birth_date, MONTH)/12) BETWEEN 50 AND 85
	AND COALESCE (r.result_time, op.order_time) BETWEEN '{date_from}' AND '{date_to}'
	AND zom.name       = 'Outpatient'
	AND ORD_NUM_VALUE >= 125

	UNION ALL

	SELECT 
	  op.pat_id, p.pat_map_id, p.mrn,
	  COALESCE (r.result_time, op.order_time) taken_time,
	  FLOOR(DATE_DIFF (r.result_time, p.birth_date, MONTH)/12) age,
	  c.name lab_name,
	  c.common_name group_lab_name,
	  c.base_name,
	  ord_num_value,
	  r.reference_unit
	FROM `{source_data_project_id}.{source_data_dataset_id}.order_results` r 
    JOIN `{source_data_project_id}.{source_data_dataset_id}.clarity_component` c ON r.component_id = c.component_id
	JOIN `{source_data_project_id}.{source_data_dataset_id}.order_proc` op ON r.order_proc_id = op.order_proc_id
	JOIN `{source_data_project_id}.{source_data_dataset_id}.order_proc_3` op3 ON op.order_proc_id = op3.order_id
	JOIN `{source_data_project_id}.{source_data_dataset_id}.zc_ordering_mode` zom ON op3.ordering_mode_c = zom.ordering_mode_c
	JOIN `{source_data_project_id}.{source_data_pat_dataset_id}.pat_map` p ON UPPER(op.pat_id) = p.shc_pat_id
	WHERE 
	c.base_name in ('GLT2', 'GTT2', 'GDM2')
	AND r.Result_Status_C = 3
	AND FLOOR(DATE_DIFF (COALESCE (r.result_time, op.order_time), p.birth_date, MONTH)/12) BETWEEN 50 AND 85
	AND COALESCE (r.result_time, op.order_time) BETWEEN '{date_from}' AND '{date_to}'
	AND zom.name = 'Outpatient'
	AND ORD_NUM_VALUE > 199

	UNION ALL

	SELECT 
	  op.pat_id, p.pat_map_id, p.mrn,
	  COALESCE (r.result_time, op.order_time) taken_time,
	  FLOOR(DATE_DIFF (r.result_time, p.birth_date, MONTH)/12) age,
	  c.name lab_name,
	  c.common_name group_lab_name,
	  c.base_name,
	  ord_num_value,
	  r.reference_unit
	FROM `{source_data_project_id}.{source_data_dataset_id}.order_results` r 
    JOIN `{source_data_project_id}.{source_data_dataset_id}.clarity_component` c ON r.component_id = c.component_id
	JOIN `{source_data_project_id}.{source_data_dataset_id}.order_proc` op ON r.order_proc_id = op.order_proc_id
	JOIN `{source_data_project_id}.{source_data_dataset_id}.order_proc_3` op3 ON op.order_proc_id = op3.order_id
	JOIN `{source_data_project_id}.{source_data_dataset_id}.zc_ordering_mode` zom ON op3.ordering_mode_c = zom.ordering_mode_c
	JOIN `{source_data_project_id}.{source_data_pat_dataset_id}.pat_map` p ON UPPER(op.pat_id) = p.shc_pat_id
	WHERE 
	c.base_name = 'GLU'
	AND r.Result_Status_C = 3
	AND FLOOR(DATE_DIFF (COALESCE (r.result_time, op.order_time), p.birth_date, MONTH)/12) BETWEEN 50 AND 85
	AND COALESCE (r.result_time, op.order_time) BETWEEN '{date_from}' AND '{date_to}'
	AND zom.name = 'Outpatient'
	AND ORD_NUM_VALUE >= 200;

	""".format_map({'source_data_project_id': source_data_project_id,
                    'source_data_dataset_id': source_data_dataset_id,
                    'source_data_pat_dataset_id': source_data_pat_dataset_id,
                    'target_data_project_id': target_data_project_id,
                    'target_data_dataset_id': target_data_dataset_id,
                    'date_from': date_from_str,
                    'date_to': date_to_str
                    })

    print(sql)
    client.query(sql).result()

def index_pdm():
    print ("--index_pdm")
    
    client = bigquery.Client(project=target_data_project_id)

    sql = """
		CREATE OR REPLACE TABLE `{target_data_project_id}.{target_data_dataset_id}.yn_nod_index_pdm` AS
		SELECT *
		FROM
		  (SELECT pat_id, pat_map_id,
		    mrn,
		    age,
		    taken_time,
		    ord_num_value,
		    reference_unit,
		    group_lab_name,
		    lab_name,
		    base_name,
		    RANK() OVER (PARTITION BY pat_map_id ORDER BY pat_map_id, taken_time) AS rnk
		  FROM
		    (SELECT pat_id, pat_map_id,
		      mrn,
		      age,
		      taken_time,
		      ord_num_value,
		      reference_unit,
		      group_lab_name,
		      lab_name,
		      base_name
		    FROM `{target_data_project_id}.{target_data_dataset_id}.yn_nod_15days_pdm`
		    ORDER BY pat_map_id,
		      taken_time
		    )
		  )
		WHERE rnk = 1;
	""".format_map({'target_data_project_id': target_data_project_id,
                    'target_data_dataset_id': target_data_dataset_id})

    print(sql)
    client.query(sql).result()

def yn_nod_3yr_labs():
    
    print ("--yn_nod_3yr_labs")

    client = bigquery.Client(project=target_data_project_id)

    sql = """
		CREATE OR REPLACE TABLE `{target_data_project_id}.{target_data_dataset_id}.yn_nod_3yr_labs` AS
		SELECT 
		  op.pat_id, pat_map_id, mrn,
		  COALESCE (r.result_time, op.order_time) taken_time,
		  c.name lab_name,
		  c.common_name group_lab_name,
		  c.base_name,
		  r.ord_num_value,
		  r.reference_unit
		FROM `{source_data_project_id}.{source_data_dataset_id}.order_results` r 
        JOIN `{source_data_project_id}.{source_data_dataset_id}.clarity_component` c ON r.component_id = c.component_id
		JOIN `{source_data_project_id}.{source_data_dataset_id}.order_proc` op ON r.order_proc_id = op.order_proc_id
		JOIN `{source_data_project_id}.{source_data_dataset_id}.order_proc_3` op3 ON op.order_proc_id = op3.order_id
		JOIN `{source_data_project_id}.{source_data_dataset_id}.zc_ordering_mode` zom ON op3.ordering_mode_c = zom.ordering_mode_c
		JOIN `{target_data_project_id}.{target_data_dataset_id}.yn_nod_index_pdm` index_pdm ON op.pat_id = index_pdm.pat_id
		WHERE
		(c.base_name = 'GLUF' OR c.base_name in ('GLT2', 'GTT2', 'GDM2') OR c.base_name = 'A1C' OR c.base_name = 'GLU')
		AND r.result_status_C = 3
		AND CAST(COALESCE (r.result_time, op.order_time) AS DATE) > DATE_ADD(CAST(index_pdm.taken_time AS DATE), INTERVAL -1095 DAY) --3 years before index pdm until index pdm taken_time
		AND CAST(COALESCE (r.result_time, op.order_time) AS DATE) < CAST(index_pdm.taken_time AS DATE)
		AND zom.name = 'Outpatient';
	""".format_map({'source_data_project_id': source_data_project_id,
                    'source_data_dataset_id': source_data_dataset_id,
                    'target_data_project_id': target_data_project_id,
                    'target_data_dataset_id': target_data_dataset_id
                    })

    print(sql)
    client.query(sql).result()

def yn_nod_3yr_labs1():
    
    print ("--yn_nod_3yr_labs1")

    client = bigquery.Client(project=target_data_project_id)

    sql = """
		CREATE OR REPLACE TABLE `{target_data_project_id}.{target_data_dataset_id}.yn_nod_3yr_labs1` AS
		SELECT DISTINCT pat_map_id,
		  taken_time,
		  ord_num_value,
		  reference_unit,
		  lab_name,
		  base_name
		FROM `{target_data_project_id}.{target_data_dataset_id}.yn_nod_3yr_labs`
		ORDER BY pat_map_id,
		  taken_time,
		  lab_name;
	""".format_map({'target_data_project_id': target_data_project_id,
                    'target_data_dataset_id': target_data_dataset_id})

    print(sql)
    client.query(sql).result()

def yn_nod_normal_lab_pats():
    
    print ("--yn_nod_normal_lab_pats")

    client = bigquery.Client(project=target_data_project_id)

    sql = """
		CREATE OR REPLACE TABLE `{target_data_project_id}.{target_data_dataset_id}.yn_nod_normal_lab_pats` AS
		SELECT DISTINCT pat_map_id from (
		    SELECT pat_map_id,
		      taken_time,
		      ord_num_value,
		      reference_unit,
		      lab_name,
		      base_name
		    FROM `{target_data_project_id}.{target_data_dataset_id}.yn_nod_3yr_labs1`
		    WHERE base_name   = 'GLUF'
		    AND ord_num_value <= 125
		    UNION ALL
		    SELECT pat_map_id,
		      taken_time,
		      ord_num_value,
		      reference_unit,
		      lab_name,
		      base_name
		    FROM `{target_data_project_id}.{target_data_dataset_id}.yn_nod_3yr_labs1`
		    WHERE base_name    = 'GLU'
		    AND ord_num_value < 200 
		    UNION ALL
		    SELECT pat_map_id,
		      taken_time,
		      ord_num_value,
		      reference_unit,
		      lab_name,
		      base_name
		    FROM `{target_data_project_id}.{target_data_dataset_id}.yn_nod_3yr_labs1`
		    WHERE base_name  IN ('GLT2', 'GTT2', 'GDM2')
		    AND ord_num_value <= 199
		    UNION ALL
		    SELECT pat_map_id,
		      taken_time,
		      ord_num_value,
		      reference_unit,
		      lab_name,
		      base_name
		    FROM `{target_data_project_id}.{target_data_dataset_id}.yn_nod_3yr_labs1`
		    WHERE base_name    = 'A1C'
		    AND ord_num_value < 6.5
		    );
	""".format_map({'target_data_project_id': target_data_project_id,
                    'target_data_dataset_id': target_data_dataset_id})

    print(sql)
    client.query(sql).result()

def yn_nod_pats_icd():
    
    print ("--yn_nod_pats_icd")

    client = bigquery.Client(project=target_data_project_id)

    sql = """
		CREATE OR REPLACE TABLE `{target_data_project_id}.{target_data_dataset_id}.yn_nod_pats_icd` AS
		SELECT DISTINCT y.pat_map_id, y.pat_id, y.mrn, taken_time AS index_pdm_time, d.contact_date AS enc_date,  current_icd10_list
		FROM `{target_data_project_id}.{target_data_dataset_id}.yn_nod_index_pdm` y 
        JOIN `{target_data_project_id}.{target_data_dataset_id}.yn_nod_normal_lab_pats` n ON y.pat_map_id = n.pat_map_id
		JOIN `{source_data_project_id}.{source_data_dataset_id}.pat_enc_dx` d ON y.pat_id = d.pat_id
		JOIN `{source_data_project_id}.{source_data_dataset_id}.clarity_edg` ce ON d.dx_id = ce.dx_id
		WHERE (current_icd10_list like '%R73%' OR current_icd10_list like '%Z13.1%' OR current_icd10_list like '%Z00.0%')
		AND d.contact_date >= DATE_ADD(taken_time, INTERVAL -30 DAY)

		UNION ALL

		SELECT DISTINCT y.pat_map_id, y.pat_id, y.mrn, taken_time AS index_pdm_time, d.noted_date AS enc_date,  current_icd10_list
		FROM `{target_data_project_id}.{target_data_dataset_id}.yn_nod_index_pdm` y 
        JOIN `{target_data_project_id}.{target_data_dataset_id}.yn_nod_normal_lab_pats` n ON y.pat_map_id = n.pat_map_id
		JOIN `{source_data_project_id}.{source_data_dataset_id}.problem_list` d ON y.pat_id = d.pat_id
		JOIN `{source_data_project_id}.{source_data_dataset_id}.clarity_edg` ce ON d.dx_id = ce.dx_id
		WHERE (current_icd10_list LIKE '%R73%' 
                OR current_icd10_list LIKE '%Z13.1%' 
                OR current_icd10_list LIKE '%Z00.0%'
                OR current_icd10_list LIKE '%Z00%' 
                OR current_icd10_list LIKE '%Z01%'
                OR current_icd10_list LIKE '%Z02%'
                OR current_icd10_list LIKE '%Z03%'
                OR current_icd10_list LIKE '%Z04%'
                OR current_icd10_list LIKE '%Z08%'
                OR current_icd10_list LIKE '%Z09%'
                OR current_icd10_list LIKE '%Z12%'
                OR current_icd10_list LIKE '%Z13%'
		)
		AND d.noted_date >= DATE_ADD(taken_time, INTERVAL -30 DAY);
	""".format_map({'source_data_project_id': source_data_project_id,
                    'source_data_dataset_id': source_data_dataset_id,
                    'target_data_project_id': target_data_project_id,
                    'target_data_dataset_id': target_data_dataset_id
                    })

    print(sql)
    client.query(sql).result()

def yn_nod_pats_list_filtered():
    
    print ("--yn_nod_pats_list_filtered")

    client = bigquery.Client(project=target_data_project_id)

    sql = """
		CREATE OR REPLACE TABLE `{target_data_project_id}.{target_data_dataset_id}.yn_nod_final_pats_list` AS
		SELECT  p.pat_map_id, labs.taken_time, labs.ord_num_value, labs.reference_unit, labs.lab_name, labs.base_name, pat_map.mrn
		FROM
		  ( SELECT pat_map_id 
            FROM `{target_data_project_id}.{target_data_dataset_id}.yn_nod_pats_icd`
            EXCEPT DISTINCT
            SELECT DISTINCT pat_map_id
            FROM `{target_data_project_id}.{target_data_dataset_id}.yn_nod_3yr_labs1`
            WHERE base_name    = 'A1C'
            AND ord_num_value >= 6.5
		  ) p
		JOIN `{source_data_project_id}.{source_data_pat_dataset_id}.pat_map` pat_map ON p.pat_map_id = pat_map.pat_map_id
		JOIN `{target_data_project_id}.{target_data_dataset_id}.yn_nod_3yr_labs1` labs ON p.pat_map_id = labs.pat_map_id

		UNION ALL

		SELECT  p.pat_map_id, labs.taken_time, labs.ord_num_value, labs.reference_unit, labs.lab_name, labs.base_name, pat_map.mrn
		FROM 
		  ( SELECT pat_map_id 
            FROM `{target_data_project_id}.{target_data_dataset_id}.yn_nod_pats_icd`
		    EXCEPT DISTINCT
		    SELECT DISTINCT pat_map_id
		    FROM `{target_data_project_id}.{target_data_dataset_id}.yn_nod_3yr_labs1`
		    WHERE base_name    = 'A1C'
		    AND ord_num_value >= 6.5
		  ) p
		JOIN `{source_data_project_id}.{source_data_pat_dataset_id}.pat_map` pat_map ON p.pat_map_id = pat_map.pat_map_id
		JOIN `{target_data_project_id}.{target_data_dataset_id}.yn_nod_index_pdm` labs ON p.pat_map_id = labs.pat_map_id;
	""".format_map({'source_data_project_id': source_data_project_id,
                    'source_data_dataset_id': source_data_dataset_id,
                    'source_data_pat_dataset_id': source_data_pat_dataset_id,
                    'target_data_project_id': target_data_project_id,
                    'target_data_dataset_id': target_data_dataset_id
                    })

    print(sql)
    client.query(sql).result()

def yn_nod_pats_list_unfiltered():
    
    print ("--yn_nod_pats_list_unfiltered")

    client = bigquery.Client(project=target_data_project_id)

    sql = """
		CREATE OR REPLACE TABLE `{target_data_project_id}.{target_data_dataset_id}.yn_nod_final_pats_list_unfiltered` AS
		SELECT  p.pat_map_id, labs.taken_time, labs.ord_num_value, labs.reference_unit, labs.lab_name, labs.base_name, pat_map.mrn
		FROM
		  ( SELECT DISTINCT y.pat_map_id 
            FROM `{target_data_project_id}.{target_data_dataset_id}.yn_nod_index_pdm` y 
            JOIN `{target_data_project_id}.{target_data_dataset_id}.yn_nod_normal_lab_pats` n ON y.pat_map_id = n.pat_map_id 
		    EXCEPT DISTINCT
		    SELECT DISTINCT pat_map_id
		    FROM `{target_data_project_id}.{target_data_dataset_id}.yn_nod_3yr_labs1`
		    WHERE base_name    = 'A1C'
		    AND ord_num_value >= 6.5
		  ) p
		JOIN `{source_data_project_id}.{source_data_pat_dataset_id}.pat_map` pat_map ON p.pat_map_id = pat_map.pat_map_id
		JOIN `{target_data_project_id}.{target_data_dataset_id}.yn_nod_3yr_labs1` labs ON p.pat_map_id = labs.pat_map_id
		UNION ALL
		SELECT  p.pat_map_id, labs.taken_time, labs.ord_num_value, labs.reference_unit, labs.lab_name, labs.base_name, pat_map.mrn
		FROM 
		  ( SELECT DISTINCT y.pat_map_id 
            FROM `{target_data_project_id}.{target_data_dataset_id}.yn_nod_index_pdm` y 
            JOIN `{target_data_project_id}.{target_data_dataset_id}.yn_nod_normal_lab_pats` n 
            ON y.pat_map_id = n.pat_map_id 
		    EXCEPT DISTINCT
		    SELECT DISTINCT pat_map_id
		    FROM `{target_data_project_id}.{target_data_dataset_id}.yn_nod_3yr_labs1`
		    WHERE base_name    = 'A1C'
		    AND ord_num_value >= 6.5
		  ) p
		JOIN `{source_data_project_id}.{source_data_pat_dataset_id}.pat_map` pat_map ON p.pat_map_id = pat_map.pat_map_id
		JOIN `{target_data_project_id}.{target_data_dataset_id}.yn_nod_index_pdm` labs ON p.pat_map_id = labs.pat_map_id;
	""".format_map({
                    'source_data_project_id': source_data_project_id,
                    'source_data_pat_dataset_id': source_data_pat_dataset_id,
                    'target_data_project_id': target_data_project_id,
                    'target_data_dataset_id': target_data_dataset_id
                    })

    print(sql)
    client.query(sql).result()

def write_pats_list(table, suffix):

    print ("--write_pats_list_" + suffix)
    client = bigquery.Client(project=target_data_project_id)
    
    sql = """
        EXPORT DATA OPTIONS(
        uri= 'gs://{target_data_bucket}/nod_pats_{suffix}_{date_from}_{date_to}_*.csv',
        format='CSV',
        overwrite=true,
        header=true,
        field_delimiter=',') AS
    SELECT p.mrn, p.lab_name, p.base_name, p.ord_num_value, p.reference_unit, p.taken_time 
	FROM `{target_data_project_id}.{target_data_dataset_id}.{table}` p    
	ORDER BY mrn, taken_time;
	""".format_map({
		            'target_data_bucket':target_data_bucket,
                    'target_data_project_id': target_data_project_id,
                    'target_data_dataset_id': target_data_dataset_id,
                    'date_from': date_from_str,
                    'date_to': date_to_str,
					'suffix': suffix,
					'table': table
                    })
                    
    print(sql)
    client.query(sql).result()

def main(data, context):
    try:
        try:
            create_pdm()
        except Exception as e:
            raise Exception('create_pdm ' + str(e))
        try:
            index_pdm()
        except Exception as e:
            raise Exception('index_pdm ' + str(e))
        try:
            yn_nod_3yr_labs()
        except Exception as e:
            raise Exception('yn_nod_3yr_labs ' + str(e))
        try:
            yn_nod_3yr_labs1()
        except Exception as e:
            raise Exception('yn_nod_3yr_labs1 ' + str(e))
        try:
            yn_nod_normal_lab_pats()
        except Exception as e:
            raise Exception('yn_nod_normal_lab_pats ' + str(e))
        try:
            yn_nod_pats_icd()
        except Exception as e:
            raise Exception('yn_nod_pats_icd ' + str(e))
        try:
            yn_nod_pats_list_filtered()
        except Exception as e:
            raise Exception('yn_nod_pats_list_filtered ' + str(e))
        try:
            yn_nod_pats_list_unfiltered()
        except Exception as e:
            raise Exception('yn_nod_pats_list_unfiltered ' + str(e))
        try:
            write_pats_list('yn_nod_final_pats_list', 'filtered')
        except Exception as e:
            raise Exception('write_pats_list_filtered ' + str(e))
        try:
            write_pats_list('yn_nod_final_pats_list_unfiltered', 'unfiltered')
        except Exception as e:
            raise Exception('write_pats_list_unfiltered ' + str(e))
    except Exception as err:
        print('Error in function ' + str(err))

if __name__ == '__main__':
    main('data', 'context')