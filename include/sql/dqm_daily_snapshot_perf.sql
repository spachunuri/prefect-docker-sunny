create or replace table {environment}.DQM.DQM_FACT as 
(
select 
A.M1_start_date, A.M1_end_date, A.M1_Startedby_id, A.M1_Endby_id, A.M1_Action_id,
A.M7_start_date, A.M7_end_date, A.M7_Startedby_id, A.M7_Endby_id, A.M7_Action_id,
A.M8_start_date, A.M8_end_date, A.M8_Startedby_id, A.M8_Endby_id, A.M8_Action_id,
A.open_order_entry_start_date, A.open_order_entry_end_date, A.open_order_entry_Startedby_id, A.open_order_entry_Endby_id, A.open_order_entry_Action_id,
B.*,
D.market as MARKET,
D.operational as MARKET_OPERATIONAL,
E.name as ORGANIZATION_NAME,
F.buyer_primary_email,
F.buyer_secondary_email,
G.CURATIVES_VALUES,
H. PRODUCT,
--G.curative_count,
C.REVENUE

from
{environment}.DQM.DQM_KEY_ACTIONS A
join prod.infomart.orders_vw B on A.fileid = B.fileid
  left join {environment}.DQM.DQM_LEDGER_FACT C on B.fileid = C.fileid
  join "PROD"."INFOMART"."GEOGRAPHY_VW" D on B.RESWARE_COUNTY_ID = D.RESWARE_COUNTY_ID
  join lake.resware.office_current E on B.orgid = E.officeid
  left join (select distinct A.fileid, listagg(A.emailprimary, '') within group (ORDER BY fileid ) as buyer_primary_email , listagg(A.emailsecondary, '') within group (ORDER BY fileid ) buyer_secondary_email from lake.resware.buyerseller_current A join lake.resware.generaluser_current B on A.buyersellerid = B.userid where A.buyersellertypeid = 1 and B.enabled = 1 group by 1) F on B.fileid = F.fileid
  left join {environment}.DQM.DQM_ORDER_CURATIVES G on B.fileid = G.fileid
  left join PROD.INFOMART.PRODUCT_VW H on H.RESWARE_PRODUCT_ID = B.RESWARE_PRODUCT_ID
 -- where B.filenumber not in ('116031FL','115774NC','116279NC')
);