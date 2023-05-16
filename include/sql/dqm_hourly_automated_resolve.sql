insert into {environment}.DQM.DQM_AUTOMATED_RESOLVE  (SNAPSHOT_ROW_ID, ID, RESOLVED_BY, RESOLVED_DATE, LOAD_DATE)
(
select
  A.row_id
  , A.ID
  , 'SYSTEM' AS RESOLVED_BY
  , CONVERT_TIMEZONE('UTC','America/Los_Angeles', sysdate()::TIMESTAMP) as RESOLVED_DATE
  , CONVERT_TIMEZONE('UTC','America/Los_Angeles', sysdate()::TIMESTAMP) as LOAD_DATE 
  --, B.ID
  --, C.ID
  from
  {environment}.DQM.DQM_SNAPSHOT A
  left join {environment}.DQM.DQM_SNAPSHOT_HOURLY_CURRENT_VW B on A.ID = B.ID 
  join {environment}.DQM.DQM_SNAPSHOT_UNRESOLVED_VW C on A.ID = C.ID
  where B.ID is null 
  --order by C.ID DESC
   );
  
  update {environment}.DQM.DQM_SNAPSHOT_UNRESOLVED set active_flag = 'N', load_date = CONVERT_TIMEZONE('UTC','America/Los_Angeles', sysdate()::TIMESTAMP)  where row_id IN 
(
select
  C.row_id as row_id
  from
  {environment}.DQM.DQM_SNAPSHOT A
  left join {environment}.DQM.DQM_SNAPSHOT_HOURLY_CURRENT_VW B on A.ID = B.ID 
  join {environment}.DQM.DQM_SNAPSHOT_UNRESOLVED_VW C on A.ID = C.ID
  where B.ID is null 
  --order by C.ID DESC
    );