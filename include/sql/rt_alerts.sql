-- 0. Truncating snapshot staging table

TRUNCATE TABLE {environment}.RT_ALERTS.RT_ALERTS_SNAPSHOT;

-- 1. Load snapshot staging table with base population of all action types completed in the past 90 days

INSERT INTO {environment}.RT_ALERTS.RT_ALERTS_SNAPSHOT (
    filenumber,
    office,
    file_status,
    escrow_date,
    est_settlement_date,
    bin,
    team_order,
    team,
    notification_type,
    fileactionsid,
    actiondefid,
    action_name,
    sendcoordinatortypeid,
    sentuserid,
    receivecoordinatortypeid,
    receiveduserid,
    action_started_at
)
-- Create base population of all action types completed in the past 90 days
WITH base_action_group AS
(
SELECT DISTINCT
    CASE
    WHEN (ar.receivecoordinatortypeid = 3 OR ar.receiveinternaluserteamid IN (508,63919)) THEN 'Order Squad'
    WHEN (ar.receivecoordinatortypeid = 52 OR ar.receiveinternaluserteamid IN (49536, 63920)) THEN 'Order Squad - QA'
    WHEN (ar.receivecoordinatortypeid = 34 OR ar.receiveinternaluserteamid IN (28309, 63922)) THEN 'Payoff Squad - Processing'
    WHEN (ar.receivecoordinatortypeid = 33 OR ar.receiveinternaluserteamid IN (28310, 63923)) THEN 'Payoff Squad - Tracking'
    WHEN (ar.receivecoordinatortypeid = 30 OR ar.receiveinternaluserteamid IN (28305, 63917)) THEN 'Loan Squad - Processing'
    WHEN (ar.receivecoordinatortypeid = 29 OR ar.receiveinternaluserteamid IN (28306, 63918)) THEN 'Loan Squad - Tracking'
    WHEN (ar.receivecoordinatortypeid = 13 OR ar.receiveinternaluserteamid IN (4506,63916)) THEN 'HUB'
    WHEN (ar.receivecoordinatortypeid = 72 OR ar.receiveinternaluserteamid = 86093) THEN 'HUB Review - Local'
    WHEN (ar.receivecoordinatortypeid = 71 OR ar.receiveinternaluserteamid = 86094) THEN 'HUB Review - Enterprise'
    WHEN (ar.receivecoordinatortypeid = 25 OR ar.receiveinternaluserteamid IN (25598,63936)) THEN 'Funding Squad'
    WHEN (ar.receivecoordinatortypeid = 7 OR ar.receiveinternaluserteamid = 512) THEN 'Recording Squad'
    WHEN (ar.receivecoordinatortypeid = 24 OR ar.receiveinternaluserteamid IN (25599, 63915)) THEN 'Disbursement Squad'
    ELSE NULL END AS bin,
    ar.actiondefid
FROM {environment}.INFOMART.ACTIONROLLUP_RAW_VW ar
WHERE TRUE
AND ar.receiveddate::DATE >= (sysdate()::date - 90)
AND ar.live = 1
AND bin IS NOT NULL
),

-- Step 1 for filtering out duplicate actions
file_action_prep AS
(
SELECT
    fileid,
    actiondefid,
    COUNT(actiondefid)
FROM {environment}.INFOMART.ACTIONROLLUP_RAW_VW
WHERE TRUE
AND live = 1
GROUP BY fileid, actiondefid
HAVING COUNT(actiondefid) > 1
AND COUNT(CASE WHEN receiveddate IS NOT NULL THEN 1 ELSE NULL END) >=1
),

-- Step 2 for filtering duplicate actions
file_action_prep_2 AS
(
SELECT
    fap.fileid,
    fap.actiondefid,
    ar.fileactionsid
FROM file_action_prep fap
LEFT JOIN {environment}.INFOMART.ACTIONROLLUP_RAW_VW ar ON fap.fileid = ar.fileid AND fap.actiondefid = ar.actiondefid
),

-- Identifying the actions that are within each of the bins in Resware
bin_prep AS
(
SELECT
    o.filenumber,
    CASE WHEN o.orgid = 1 THEN 'EP' ELSE 'AHC' END AS office,
    o.file_status,
    o.escrow_date,
    o.est_settlement_date,
    CASE
--Order Squad Bin --
    WHEN TRUE
    AND (ar.receivecoordinatortypeid = 3 OR ar.receiveinternaluserteamid IN (508,63919))
    AND o.statusid NOT IN (3, 4, 6, 7,8,9) /** Fall Through, Open In Error, On Hold, Test, Cancelled, Closed **/
    THEN 'Order Squad'
--Order Squad QA Bin --
    WHEN TRUE
    AND (ar.receivecoordinatortypeid = 52 OR ar.receiveinternaluserteamid IN (49536, 63920))
    AND o.statusid NOT IN (3, 4, 6, 7,8,9) /** Fall Through, Open In Error, On Hold, Test, Cancelled, Closed **/
    THEN 'Order Squad - QA'
--Payoff Processing Bin --
    WHEN TRUE
    AND (ar.receivecoordinatortypeid = 34 OR ar.receiveinternaluserteamid IN (28309, 63922))
    AND o.statusid NOT IN (3, 4, 5, 6, 7,8,9) /** Fall Through, Cancellation Pending, Open In Error, On Hold, Test, Cancelled, Closed **/
    THEN 'Payoff Squad - Processing'
--Payoff Tracking Bin NEEDS WORK--
    WHEN TRUE
    AND (ar.receivecoordinatortypeid = 33 OR ar.receiveinternaluserteamid IN (28310, 63923))
    AND o.statusid NOT IN (3, 4, 5, 6, 7,8,9) /** Fall Through, Cancellation Pending, Open In Error, On Hold, Test, Cancelled, Closed **/
    AND CASE WHEN o.file_status <> 'Closed' THEN 1 WHEN o.file_status = 'Closed' AND am.displayname ILIKE '%Final%' THEN 1 ELSE 0 END = 1
    AND CASE WHEN ar.receivedue IS NULL THEN 0 ELSE 1 END = 1
    THEN 'Payoff Squad - Tracking'
--Loan Processing Bin--
    WHEN TRUE
    AND (ar.receivecoordinatortypeid = 30 OR ar.receiveinternaluserteamid IN (28305, 63917))
    AND o.statusid NOT IN (3, 4, 6, 7,8,9) /** Fall Through, Open In Error, On Hold, Test, Cancelled, Closed **/
    THEN 'Loan Squad - Processing'
--Loan Tracking Bin--
    WHEN TRUE
    AND (ar.receivecoordinatortypeid = 29 OR ar.receiveinternaluserteamid IN (28306, 63918))
    AND o.statusid NOT IN (3, 4, 6, 7,8,9) /** Fall Through, Open In Error, On Hold, Test, Cancelled, Closed **/
    THEN 'Loan Squad - Tracking'
--Jedi Bin--
    WHEN TRUE
    AND (ar.receivecoordinatortypeid = 13 OR ar.receiveinternaluserteamid IN (4506,63916))
    AND o.statusid NOT IN (3, 4, 6, 7,8,9) /** Fall Through, Open In Error, On Hold, Test, Cancelled, Closed **/
    THEN 'HUB'
--Jedi Local Bin--
    WHEN TRUE
    AND (ar.receivecoordinatortypeid = 72 OR ar.receiveinternaluserteamid = 86093)
    AND o.statusid NOT IN (3, 4, 6, 7,8,9) /** Fall Through, Open In Error, On Hold, Test, Cancelled, Closed **/
    THEN 'HUB Review - Local'
--Jedi Enterprise Bin--
    WHEN TRUE
    AND (ar.receivecoordinatortypeid = 71 OR ar.receiveinternaluserteamid = 86094)
    AND o.statusid NOT IN (3, 4, 6, 7,8,9) /** Fall Through, Open In Error, On Hold, Test, Cancelled, Closed **/
    THEN 'HUB Review - Enterprise'
--Funding Squad Bin --
    WHEN TRUE
    AND (ar.receivecoordinatortypeid = 25 OR ar.receiveinternaluserteamid IN (25598,63936))
    AND o.statusid NOT IN (3, 4, 6, 7) /** Fall Through, Open In Error, On Hold, Test **/
    THEN 'Funding Squad'
--Recording Squad Bin --
    WHEN TRUE
    AND (ar.receivecoordinatortypeid = 7 OR ar.receiveinternaluserteamid = 512)
    AND o.statusid NOT IN (3, 4, 6, 7) /** Fall Through, Open In Error, On Hold, Test **/
    THEN 'Recording Squad'
--Disbursement Squad Bin --
    WHEN TRUE
    AND (ar.receivecoordinatortypeid = 24 OR ar.receiveinternaluserteamid IN (25599, 63915))
    AND o.statusid NOT IN (3, 4, 6, 7) /** Fall Through, Open In Error, On Hold, Test **/
    THEN 'Disbursement Squad'
--Disbursement Squad - TX Bin --
    WHEN TRUE
    AND (ar.receivecoordinatortypeid = 78 OR ar.receiveinternaluserteamid = 70256)
    AND o.statusid NOT IN (3, 4, 6, 7) /** Fall Through, Open In Error, On Hold, Test **/
    THEN 'Disbursement Squad - TX'
    ELSE NULL END AS bin,
    
    CASE
    WHEN bin IN ('Order Squad', 'Order Squad - QA') 
    THEN 'Order Squad'

    WHEN bin IN ('Payoff Squad - Processing', 'Payoff Squad - Tracking') 
    THEN 'Payoff Squad'
    
    WHEN bin IN ('Loan Squad - Processing', 'Loan Squad - Tracking') 
    THEN 'Loan Squad'
    
    WHEN bin IN ('HUB', 'HUB Review - Local', 'HUB Review - Enterprise') 
    THEN 'Jedi Squad'
    
    WHEN bin = 'Funding Squad' 
    THEN 'Funding Squad'
    
    WHEN bin = 'Recording Squad' 
    THEN 'Recording Squad'
    
    WHEN bin IN ('Disbursement Squad', 'Disbursement Squad - TX') 
    THEN 'Disbursement Squad'
    ELSE NULL END AS team,
    
    CASE
    WHEN team = 'Order Squad' 
    THEN 1

    WHEN team = 'Payoff Squad' 
    THEN 2
    
    WHEN team = 'Loan Squad' 
    THEN 3
    
    WHEN team = 'Jedi Squad' 
    THEN 4
    
    WHEN team = 'Funding Squad' 
    THEN 5
    
    WHEN team = 'Recording Squad' 
    THEN 6
    
    WHEN team = 'Disbursement Squad' 
    THEN 7
    ELSE NULL END AS team_order,
    
    ar.fileactionsid,
    ar.actiondefid,
    am.displayname AS action_name,
    ar.sendcoordinatortypeid,
    ar.sentuserid,
    ar.receivecoordinatortypeid,
    ar.receiveduserid,
    ar.sentdate AS action_started_at,
    ar.receivedue AS action_due_at,
    ar.receiveddate AS action_completed_at
    
FROM {environment}.INFOMART.ORDERS_VW o
LEFT JOIN {environment}.INFOMART.ACTIONROLLUP_RAW_VW ar ON o.fileid = ar.fileid
LEFT JOIN {environment}.REFERENCE.ACTIONDEF_MAPPING am ON ar.actiondefid = am.actiondefid 
WHERE TRUE
AND ar.receiveddate IS NULL
AND ar.sendcoordinatortypeid IS NOT NULL
AND ar.live = 1
AND ar.actiondefid IN (SELECT actiondefid FROM base_action_group)
AND CASE 
    WHEN action_due_at IS NULL THEN 1 WHEN action_due_at IS NOT NULL AND action_due_at::date <= CONVERT_TIMEZONE(  'UTC', 'America/Los_Angeles', sysdate() ) ::date THEN 1 
    ELSE 0 END = 1
AND CASE 
    WHEN action_started_at IS NULL AND action_due_at IS NULL THEN 0 
    ELSE 1 END = 1
AND CASE
    WHEN DATEDIFF(DAY,action_started_at,CONVERT_TIMEZONE(  'UTC', 'America/Los_Angeles', sysdate() ) ) >= 30 AND action_due_at IS NULL THEN 0
    WHEN action_started_at IS NULL AND DATEDIFF(DAY,action_due_at,CONVERT_TIMEZONE(  'UTC', 'America/Los_Angeles', sysdate() ) ) >= 30 THEN 0
    WHEN action_started_at IS NULL AND action_due_at IS NULL THEN 0
    WHEN DATEDIFF(DAY,action_started_at,CONVERT_TIMEZONE(  'UTC', 'America/Los_Angeles', sysdate() ) ) >= 30 AND DATEDIFF(DAY,action_due_at,CONVERT_TIMEZONE(  'UTC', 'America/Los_Angeles', sysdate() ) ) >= 30 THEN 0
    ELSE 1 END = 1
AND bin IS NOT NULL
AND ar.fileactionsid NOT IN (SELECT fileactionsid FROM file_action_prep_2)
)

-- Adding RT alerting criteria
SELECT
    bp.filenumber,
    bp.office,
    bp.file_status,
    bp.escrow_date,
    bp.est_settlement_date,
    bp.bin,
    bp.team_order,
    bp.team,
    
    CASE
    WHEN bp.team = 'Order Squad' AND DATEDIFF(MINUTE, bp.action_started_at, CONVERT_TIMEZONE(  'UTC', 'America/Los_Angeles', sysdate() ) ) >= 60 
    THEN 'Action In Bin For 1+ Hours'
    
    WHEN bp.bin = 'Payoff Squad - Processing' AND DATEDIFF(MINUTE, bp.action_started_at, CONVERT_TIMEZONE(  'UTC', 'America/Los_Angeles', sysdate() ) ) >= 60 
    THEN 'Action In Bin For 1+ Hours'
    
    WHEN bp.bin = 'Payoff Squad - Tracking' AND DATEDIFF(DAY,CONVERT_TIMEZONE(  'UTC', 'America/Los_Angeles', sysdate() ) ,bp.est_settlement_date) <= 15 
    THEN 'Est Closing Is Within 15 Days'
    
    WHEN bp.team = 'Loan Squad' AND bp.actiondefid IN (1354, 1443) AND DATEDIFF(MINUTE, bp.action_started_at, CONVERT_TIMEZONE(  'UTC', 'America/Los_Angeles', sysdate() ) ) >= 30 
    THEN 'Action In Bin For 30+ Minutes'
    
    WHEN bp.team = 'Loan Squad' AND bp.actiondefid = 1337 AND DATEDIFF(MINUTE, bp.action_started_at, CONVERT_TIMEZONE(  'UTC', 'America/Los_Angeles', sysdate() ) ) >= 120 
    THEN 'Action In Bin For 2+ Hours'
    
    WHEN bp.team = 'Jedi Squad' AND (bp.action_name ILIKE '%Urgent Escalation%' OR bp.actiondefid = 3395) AND DATEDIFF(MINUTE, bp.action_started_at, CONVERT_TIMEZONE(  'UTC', 'America/Los_Angeles', sysdate() ) ) >= 180 
    THEN 'Action In Bin For 3+ Hours'
    
    WHEN bp.team = 'Jedi Squad' AND bp.actiondefid IN (2792, 2785) AND DATEDIFF(DAY,CONVERT_TIMEZONE(  'UTC', 'America/Los_Angeles', sysdate() ) ,bp.est_settlement_date) <= 6 
    THEN 'Est Closing Is Within 6 Days'
    
    WHEN bp.team = 'Jedi Squad' AND bp.actiondefid IN (2474, 2420, 2422, 2432, 2429) AND DATEDIFF(DAY,CONVERT_TIMEZONE(  'UTC', 'America/Los_Angeles', sysdate() ) , bp.est_settlement_date) <= 10 
    THEN 'Est Closing Is Within 10 Days'
    
    WHEN bp.team = 'Funding Squad' AND bp.actiondefid = 2675 AND DATEDIFF(MINUTE, bp.action_started_at, CONVERT_TIMEZONE(  'UTC', 'America/Los_Angeles', sysdate() ) ) >= 60 
    THEN 'Action In Bin For 1+ Hours'
    
    WHEN bp.team = 'Recording Squad' AND bp.actiondefid IN (301, 298, 1199, 1197) AND DATEDIFF(MINUTE, bp.action_started_at,CONVERT_TIMEZONE(  'UTC', 'America/Los_Angeles', sysdate() ) ) >= 60 
    THEN 'Action In Bin For 1+ Hours'
    
    WHEN bp.team = 'Recording Squad' AND bp.actiondefid = 746  AND DATEDIFF(MINUTE, bp.action_started_at,CONVERT_TIMEZONE(  'UTC', 'America/Los_Angeles', sysdate() ) ) >= 120 
    THEN 'Action In Bin For 2+ Hours'
    
    WHEN bp.team = 'Disbursement Squad' AND bp.actiondefid IN (1417, 1291, 2638) AND DATEDIFF(MINUTE, bp.action_started_at,CONVERT_TIMEZONE(  'UTC', 'America/Los_Angeles', sysdate() ) ) >= 60 
    THEN 'Action In Bin For 1+ Hours' 
    ELSE NULL END AS notification_type,

    bp.fileactionsid,
    bp.actiondefid,
    bp.action_name,
    bp.sendcoordinatortypeid,
    bp.sentuserid,
    bp.receivecoordinatortypeid,
    bp.receiveduserid,
    bp.action_started_at

FROM bin_prep bp
WHERE notification_type IS NOT NULL
ORDER BY bp.bin;

-- 2. Load new rows that exist in snapshot but not in current into current

INSERT INTO {environment}.RT_ALERTS.RT_ALERTS_CURRENT (
    filenumber,
    office,
    file_status,
    escrow_date,
    est_settlement_date,
    bin,
    team_order,
    team,
    notification_type,
    fileactionsid,
    actiondefid,
    action_name,
    sendcoordinatortypeid,
    sentuserid,
    receivecoordinatortypeid,
    receiveduserid,
    action_started_at,
    alert_triggered_at
)
SELECT 
    RT_ALERTS_SNAPSHOT.filenumber,
    RT_ALERTS_SNAPSHOT.office,
    RT_ALERTS_SNAPSHOT.file_status,
    RT_ALERTS_SNAPSHOT.escrow_date,
    RT_ALERTS_SNAPSHOT.est_settlement_date,
    RT_ALERTS_SNAPSHOT.bin,
    RT_ALERTS_SNAPSHOT.team_order,
    RT_ALERTS_SNAPSHOT.team,
    RT_ALERTS_SNAPSHOT.notification_type,
    RT_ALERTS_SNAPSHOT.fileactionsid,
    RT_ALERTS_SNAPSHOT.actiondefid,
    RT_ALERTS_SNAPSHOT.action_name,
    RT_ALERTS_SNAPSHOT.sendcoordinatortypeid,
    RT_ALERTS_SNAPSHOT.sentuserid,
    RT_ALERTS_SNAPSHOT.receivecoordinatortypeid,
    RT_ALERTS_SNAPSHOT.receiveduserid,
    RT_ALERTS_SNAPSHOT.action_started_at,
    CONVERT_TIMEZONE( 'UTC', 'America/Los_Angeles', sysdate() ) 
    
FROM {environment}.RT_ALERTS.RT_ALERTS_SNAPSHOT RT_ALERTS_SNAPSHOT
LEFT OUTER JOIN {environment}.RT_ALERTS.RT_ALERTS_CURRENT RT_ALERTS_CURRENT 
    ON RT_ALERTS_SNAPSHOT.fileactionsid = RT_ALERTS_CURRENT.fileactionsid 
        AND RT_ALERTS_SNAPSHOT.action_started_at = RT_ALERTS_CURRENT.action_started_at
WHERE RT_ALERTS_CURRENT.fileactionsid IS NULL;

--3. Load rows in current that are not in snapshot into resolved because they are resolved 

INSERT INTO {environment}.RT_ALERTS.RT_ALERTS_RESOLVED (
    filenumber,
    office,
    file_status,
    escrow_date,
    est_settlement_date,
    bin,
    team_order,
    team,
    notification_type,
    fileactionsid,
    actiondefid,
    action_name,
    sendcoordinatortypeid,
    sentuserid,
    receivecoordinatortypeid,
    receiveduserid,
    action_started_at,
    action_completed_at,
    alert_triggered_at,
    alert_resolved_at
)
SELECT 
    RT_ALERTS_CURRENT.filenumber,
    RT_ALERTS_CURRENT.office,
    RT_ALERTS_CURRENT.file_status,
    RT_ALERTS_CURRENT.escrow_date,
    RT_ALERTS_CURRENT.est_settlement_date,
    RT_ALERTS_CURRENT.bin,
    RT_ALERTS_CURRENT.team_order,    
    RT_ALERTS_CURRENT.team,
    RT_ALERTS_CURRENT.notification_type,
    RT_ALERTS_CURRENT.fileactionsid,
    RT_ALERTS_CURRENT.actiondefid,
    RT_ALERTS_CURRENT.action_name,
    RT_ALERTS_CURRENT.sendcoordinatortypeid,
    RT_ALERTS_CURRENT.sentuserid,
    RT_ALERTS_CURRENT.receivecoordinatortypeid,
    RT_ALERTS_CURRENT.receiveduserid,
    RT_ALERTS_CURRENT.action_started_at,
    ACTIONROLLUP_RAW_VW.receiveddate,
    RT_ALERTS_CURRENT.alert_triggered_at,
    CONVERT_TIMEZONE(  'UTC', 'America/Los_Angeles', sysdate() ) 
FROM {environment}.RT_ALERTS.RT_ALERTS_CURRENT RT_ALERTS_CURRENT 
LEFT OUTER JOIN {environment}.RT_ALERTS.RT_ALERTS_SNAPSHOT RT_ALERTS_SNAPSHOT
    ON RT_ALERTS_SNAPSHOT.fileactionsid = RT_ALERTS_CURRENT.fileactionsid 
        AND RT_ALERTS_SNAPSHOT.action_started_at = RT_ALERTS_CURRENT.action_started_at
LEFT OUTER JOIN PROD.INFOMART.ACTIONROLLUP_RAW_VW ACTIONROLLUP_RAW_VW 
    ON RT_ALERTS_CURRENT.fileactionsid = ACTIONROLLUP_RAW_VW.fileactionsid
        AND COALESCE(RT_ALERTS_CURRENT.action_started_at, '1901-01-01') = ACTIONROLLUP_RAW_VW.sentdate
WHERE RT_ALERTS_SNAPSHOT.fileactionsid IS NULL;

-- 4. Delete resolved alerts from current after copying them to resolved.

DELETE FROM {environment}.RT_ALERTS.RT_ALERTS_CURRENT RT_ALERTS_CURRENT
WHERE NOT EXISTS (
    SELECT 1 
    FROM {environment}.RT_ALERTS.RT_ALERTS_SNAPSHOT RT_ALERTS_SNAPSHOT 
    WHERE RT_ALERTS_SNAPSHOT.fileactionsid = RT_ALERTS_CURRENT.fileactionsid 
        AND RT_ALERTS_SNAPSHOT.action_started_at = RT_ALERTS_CURRENT.action_started_at)
        