--- 1.Copy table from S3 --
copy into LAKE{environment_suffix}.AUDITLOG.AUDITLOG_RAW(
  filename
, file_row_number
, value
, load_date
, extract_date)
  from (
  select metadata$filename as filename
       , metadata$file_row_number as file_row_number
       , t.$1 as value
       , sysdate() as load_date
       , substr(metadata$filename, 19, 26) as extract_date
  from  @LAKE{environment_suffix}.AUDITLOG.ENDPOINT_S3{environment_suffix}_AUDITLOG/
  (file_format => LAKE{environment_suffix}.AUDITLOG.JSON) t
  );

--- 2. Flatten data ---
INSERT INTO LAKE{environment_suffix}.AUDITLOG.AUDITLOG_CURRENT(FILENAME,
                                      FILE_ROW_NUMBER,
                                      LOAD_DATE,
                                      EXTRACT_DATE,
                                      DAG, 
                                      DS, 
                                      ENVIRONMENT_NAME, 
                                      PARAMS,
                                      SCHEDULED_DATETIME,
                                      START_DATETIME,
                                      RUN_ID,
                                      RUN_TYPE,
                                      RUNEXCEPTION,
                                      TASK,
                                      TASK_STATUS,
                                      TASK_INSTANCE_KEY_STR,
                                      TI,
                                      TS
                                       )
SELECT t.filename, 
        t.file_row_number, 
        t.load_date, 
        t.extract_date, 
        t.VALUE:dag, 
        t.VALUE:ds, 
        t.VALUE:environment_name, 
        t.VALUE:params, 
        t.VALUE:scheduled_datetime, 
        t.VALUE:start_datetime, 
        t.VALUE:run_id,
        t.VALUE:run_type,
        t.VALUE:runexception,
        t.VALUE:task,
        t.VALUE:task_status,
        t.VALUE:task_instance_key_str,
        t.VALUE:ti,
        t.VALUE:ts
FROM LAKE{environment_suffix}.AUDITLOG.AUDITLOG_RAW t
WHERE 
t.EXTRACT_DATE > (select coalesce(max(SOURCE_MAX_DATETIME), '2022-01-01') from {environment}.AIRFLOW_AUDIT.AIRFLOW_WATERMARK where dag_name = 'auditlog_download' and task_name = 'uploading_data') ;


--- 3. Insert Watermark ---
INSERT INTO {environment}.AIRFLOW_AUDIT.AIRFLOW_WATERMARK (DAG_NAME, TASK_NAME, RUN_TYPE, RUN_DATETIME, SOURCE_MAX_DATETIME) 
SELECT '{dag_name}', '{task_name}' , '{run_type}', SYSDATE() , MAX(EXTRACT_DATE) 
FROM LAKE{environment_suffix}.AUDITLOG.AUDITLOG_RAW;
