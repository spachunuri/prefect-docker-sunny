copy into lake{environment_suffix}.CODEFRESH.CODEFRESH_RAW (FILENAME, FILE_ROW_NUMBER,FILE_CONTENT_KEY,START_SCAN_TIME,FILE_LAST_MODIFIED,"VALUE", LOAD_DATE,EXTRACT_DATE) from 
    (select METADATA$FILENAME
        ,METADATA$FILE_ROW_NUMBER
        ,METADATA$FILE_CONTENT_KEY
        ,METADATA$START_SCAN_TIME
        ,METADATA$FILE_LAST_MODIFIED
        , t.$1
        , SYSDATE()
        , METADATA$FILE_LAST_MODIFIED
    from @lake{environment_suffix}.CODEFRESH.CODEFRESH_DATA
    (file_format=> 'lake{environment_suffix}.CODEFRESH.JSON', pattern=>'^.*\.json$') t);

insert into lake{environment_suffix}.CODEFRESH.CODEFRESH_FLATTENED  
select FILENAME, FILE_ROW_NUMBER,FILE_CONTENT_KEY,START_SCAN_TIME,FILE_LAST_MODIFIED,
                A.KEY,
                A.value:covered::varchar as COVERED, 
                A.value:pct::varchar as PCT,
                A.value:skipped::varchar as SKIPPED,
                A.value:total::varchar as TOTAL,
                LOAD_DATE,
                EXTRACT_DATE from 
                lake{environment_suffix}.CODEFRESH.CODEFRESH_RAW
                , lateral flatten( input => "VALUE":total ) A
where FILENAME not in (
select FILENAME from lake{environment_suffix}.CODEFRESH."CODEFRESH_FLATTENED");