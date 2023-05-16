insert into {environment}.DQM.DQM_SNAPSHOT_HOURLY_HIST select * from {environment}.DQM.DQM_SNAPSHOT_HOURLY;
truncate table {environment}.DQM.DQM_SNAPSHOT_HOURLY;
insert into {environment}.DQM.DQM_SNAPSHOT_HOURLY  (ID, DQM_SOURCE_ID, VALUES_CHECKED, RULE_ID, IDENTIFIED_DATE, LOAD_DATE)
(
select
'DQM1.0'||'-'||filenumber as ID
, filenumber as DQM_SOURCE_ID
, OBJECT_CONSTRUCT_KEEP_NULL('m8_end_date', m8_end_date,'file_status', file_status, 'resware_product_name', resware_product_name) as VALUES_CHECKED
, 'DQM1.0' as RULE_ID
--, 'TRUE' as FLAGGED
, CONVERT_TIMEZONE('UTC','America/Los_Angeles', sysdate()::TIMESTAMP) as IDENTIFIED_DATE
--, '1900-01-01' as RESOLVED_DATE
, CONVERT_TIMEZONE('UTC','America/Los_Angeles', sysdate()::TIMESTAMP) as LOAD_DATE
from {environment}.DQM.DQM_FACT 
where
(m8_end_date is not null and statusid != 9 and resware_product_id not in (32, 33, 35)  )
  
  );
 
insert into {environment}.DQM.DQM_SNAPSHOT_HOURLY (ID, DQM_SOURCE_ID, VALUES_CHECKED, RULE_ID, IDENTIFIED_DATE, LOAD_DATE)
(
select
'DQM2.0'||'-'||filenumber as ID
, filenumber as DQM_SOURCE_ID
, OBJECT_CONSTRUCT_KEEP_NULL('file_status', file_status, 'disbursement_date', disbursement_date) as VALUES_CHECKED
, 'DQM2.0' as RULE_ID
--, 'TRUE' as FLAGGED
, CONVERT_TIMEZONE('UTC','America/Los_Angeles', sysdate()::TIMESTAMP) as IDENTIFIED_DATE
--, '1900-01-01' as RESOLVED_DATE
, CONVERT_TIMEZONE('UTC','America/Los_Angeles', sysdate()::TIMESTAMP) as LOAD_DATE
from {environment}.DQM.DQM_FACT 
where
--  filenumber = '112666TX'
 -- and 
( disbursement_date is null and statusid = 9 )
);

insert into {environment}.DQM.DQM_SNAPSHOT_HOURLY (ID, DQM_SOURCE_ID, VALUES_CHECKED, RULE_ID, IDENTIFIED_DATE, LOAD_DATE)
(
select
'DQM3.0'||'-'||filenumber as ID
, filenumber as DQM_SOURCE_ID
, OBJECT_CONSTRUCT_KEEP_NULL('file_status', file_status, 'disbursement_date', disbursement_date, 'resware_product_name', resware_product_name, 'revenue', ifnull(revenue,0)) as VALUES_CHECKED
, 'DQM3.0' as RULE_ID
--, 'TRUE' as FLAGGED
, CONVERT_TIMEZONE('UTC','America/Los_Angeles', sysdate()::TIMESTAMP) as IDENTIFIED_DATE
--, '1900-01-01' as RESOLVED_DATE
, CONVERT_TIMEZONE('UTC','America/Los_Angeles', sysdate()::TIMESTAMP) as LOAD_DATE
from {environment}.DQM.DQM_FACT 
where
( resware_product_id = 35  and ifnull(revenue,0) > 0 and statusid != 9 and datediff('hour', disbursement_date,current_timestamp()) > 48 )
);

insert into {environment}.DQM.DQM_SNAPSHOT_HOURLY (ID, DQM_SOURCE_ID, VALUES_CHECKED, RULE_ID, IDENTIFIED_DATE, LOAD_DATE)
(
select
'DQM4.0'||'-'||filenumber as ID
, filenumber as DQM_SOURCE_ID
, OBJECT_CONSTRUCT_KEEP_NULL('file_status', file_status, 'resware_product_name', resware_product_name, 'revenue', ifnull(revenue,0)) as VALUES_CHECKED
, 'DQM4.0' as RULE_ID
--, 'TRUE' as FLAGGED
, CONVERT_TIMEZONE('UTC','America/Los_Angeles', sysdate()::TIMESTAMP) as IDENTIFIED_DATE
--, '1900-01-01' as RESOLVED_DATE
, CONVERT_TIMEZONE('UTC','America/Los_Angeles', sysdate()::TIMESTAMP) as LOAD_DATE
from {environment}.DQM.DQM_FACT 
where
( statusid = 9 and resware_product_id not in (32, 33) and ifnull(revenue,0) = 0 )

  );

insert into {environment}.DQM.DQM_SNAPSHOT_HOURLY (ID, DQM_SOURCE_ID, VALUES_CHECKED, RULE_ID, IDENTIFIED_DATE, LOAD_DATE)
(
select
'DQM5.0'||'-'||filenumber as ID
, filenumber as DQM_SOURCE_ID
, OBJECT_CONSTRUCT_KEEP_NULL('file_status', file_status, 'm7_start_date', m7_start_date) as VALUES_CHECKED
, 'DQM5.0' as RULE_ID
--, 'TRUE' as FLAGGED
, CONVERT_TIMEZONE('UTC','America/Los_Angeles', sysdate()::TIMESTAMP) as IDENTIFIED_DATE
--, '1900-01-01' as RESOLVED_DATE
, CONVERT_TIMEZONE('UTC','America/Los_Angeles', sysdate()::TIMESTAMP) as LOAD_DATE
from {environment}.DQM.DQM_FACT 
where 
  ( m7_start_date is not null and statusid not in (8,9) )

  );

insert into {environment}.DQM.DQM_SNAPSHOT_HOURLY (ID, DQM_SOURCE_ID, VALUES_CHECKED, RULE_ID, IDENTIFIED_DATE, LOAD_DATE)
(
select
'DQM6.0'||'-'||filenumber as ID
, filenumber as DQM_SOURCE_ID
, OBJECT_CONSTRUCT_KEEP_NULL('file_status', file_status, 'disbursement_date', disbursement_date) as VALUES_CHECKED
, 'DQM6.0' as RULE_ID
--, 'TRUE' as FLAGGED
, CONVERT_TIMEZONE('UTC','America/Los_Angeles', sysdate()::TIMESTAMP) as IDENTIFIED_DATE
--, '1900-01-01' as RESOLVED_DATE
, CONVERT_TIMEZONE('UTC','America/Los_Angeles', sysdate()::TIMESTAMP) as LOAD_DATE
from {environment}.DQM.DQM_FACT  
where
(statusid = 9 AND (disbursement_date > date_trunc( day,date_trunc( day,current_timestamp()))) )
--  order by disbursement_date desc

  );

---DQM7.0
insert into {environment}.DQM.DQM_SNAPSHOT_HOURLY (ID, DQM_SOURCE_ID, VALUES_CHECKED, RULE_ID, IDENTIFIED_DATE, LOAD_DATE)
(
select
'DQM7.0'||'-'||filenumber as ID
, filenumber as DQM_SOURCE_ID
, OBJECT_CONSTRUCT_KEEP_NULL('file_status', file_status, 'm7_end_date', m7_end_date,'resware_product_name',resware_product_name,'m8_end_date',m8_end_date ) as VALUES_CHECKED
, 'DQM7.0' as RULE_ID
--, 'TRUE' as FLAGGED
, CONVERT_TIMEZONE('UTC','America/Los_Angeles', sysdate()::TIMESTAMP) as IDENTIFIED_DATE
--, '1900-01-01' as RESOLVED_DATE
, CONVERT_TIMEZONE('UTC','America/Los_Angeles', sysdate()::TIMESTAMP) as LOAD_DATE
from {environment}.DQM.DQM_FACT
where
((m8_end_date is null and m7_end_date is null) and statusid = 9 and resware_product_id not in (32, 33, 35, 55) )

  );
  
insert into {environment}.DQM.DQM_SNAPSHOT_HOURLY (ID, DQM_SOURCE_ID, VALUES_CHECKED, RULE_ID, IDENTIFIED_DATE, LOAD_DATE)
(
select
'DQM8.0'||'-'||filenumber as ID
, filenumber as DQM_SOURCE_ID
, OBJECT_CONSTRUCT_KEEP_NULL('resware_product_name',resware_product_name,'file_type',file_type ) as VALUES_CHECKED
, 'DQM8.0' as RULE_ID
--, 'TRUE' as FLAGGED
, CONVERT_TIMEZONE('UTC','America/Los_Angeles', sysdate()::TIMESTAMP) as IDENTIFIED_DATE
--, '1900-01-01' as RESOLVED_DATE
, CONVERT_TIMEZONE('UTC','America/Los_Angeles', sysdate()::TIMESTAMP) as LOAD_DATE
from {environment}.DQM.DQM_FACT 
where
(resware_product_id not in (35) and file_type not in ('Full Title and Escrow', 'Title Only', 'Escrow Only')  )
  );

insert into {environment}.DQM.DQM_SNAPSHOT_HOURLY (ID, DQM_SOURCE_ID, VALUES_CHECKED, RULE_ID, IDENTIFIED_DATE, LOAD_DATE)
(
select
'DQM9.0'||'-'||filenumber as ID
, filenumber as DQM_SOURCE_ID
, OBJECT_CONSTRUCT_KEEP_NULL('resware_product_name',resware_product_name,'file_type',file_type ) as VALUES_CHECKED
, 'DQM9.0' as RULE_ID
--, 'TRUE' as FLAGGED
, CONVERT_TIMEZONE('UTC','America/Los_Angeles', sysdate()::TIMESTAMP) as IDENTIFIED_DATE
--, '1900-01-01' as RESOLVED_DATE
, CONVERT_TIMEZONE('UTC','America/Los_Angeles', sysdate()::TIMESTAMP) as LOAD_DATE
from {environment}.DQM.DQM_FACT 
where
(resware_product_id in (35) and file_type not in ('Full Title and Escrow', 'Title Only', 'Escrow Only') )

  );
  
 insert into {environment}.DQM.DQM_SNAPSHOT_HOURLY (ID, DQM_SOURCE_ID, VALUES_CHECKED, RULE_ID, IDENTIFIED_DATE, LOAD_DATE)
(
select
'DQM10.0'||'-'||filenumber as ID
, filenumber as DQM_SOURCE_ID
, OBJECT_CONSTRUCT_KEEP_NULL('primarypropertyaddress',primarypropertyaddress,'file_status',file_status ) as VALUES_CHECKED
, 'DQM10.0' as RULE_ID
--, 'TRUE' as FLAGGED
, CONVERT_TIMEZONE('UTC','America/Los_Angeles', sysdate()::TIMESTAMP) as IDENTIFIED_DATE
--, '1900-01-01' as RESOLVED_DATE
, CONVERT_TIMEZONE('UTC','America/Los_Angeles', sysdate()::TIMESTAMP) as LOAD_DATE
from {environment}.DQM.DQM_FACT
where
( contains(lower(PRIMARYPROPERTYADDRESS), 'test') and lower(file_status) != 'test' )

  );

insert into {environment}.DQM.DQM_SNAPSHOT_HOURLY (ID, DQM_SOURCE_ID, VALUES_CHECKED, RULE_ID, IDENTIFIED_DATE, LOAD_DATE)
(
select
'DQM11.0'||'-'||filenumber as ID
, filenumber as DQM_SOURCE_ID
, OBJECT_CONSTRUCT_KEEP_NULL('resware_product_name',resware_product_name,'escrow_date',escrow_date ) as VALUES_CHECKED
, 'DQM11.0' as RULE_ID
--, 'TRUE' as FLAGGED
, CONVERT_TIMEZONE('UTC','America/Los_Angeles', sysdate()::TIMESTAMP) as IDENTIFIED_DATE
--, '1900-01-01' as RESOLVED_DATE
, CONVERT_TIMEZONE('UTC','America/Los_Angeles', sysdate()::TIMESTAMP) as LOAD_DATE
from {environment}.DQM.DQM_FACT
where
(product != 'Prelim' and escrow_date is null )

  );

insert into {environment}.DQM.DQM_SNAPSHOT_HOURLY (ID, DQM_SOURCE_ID, VALUES_CHECKED, RULE_ID, IDENTIFIED_DATE, LOAD_DATE)
(
select
'DQM12.0'||'-'||filenumber as ID
, filenumber as DQM_SOURCE_ID
, OBJECT_CONSTRUCT_KEEP_NULL('resware_product_name',resware_product_name,'escrow_date',escrow_date ) as VALUES_CHECKED
, 'DQM12.0' as RULE_ID
--, 'TRUE' as FLAGGED
, CONVERT_TIMEZONE('UTC','America/Los_Angeles', sysdate()::TIMESTAMP) as IDENTIFIED_DATE
--, '1900-01-01' as RESOLVED_DATE
, CONVERT_TIMEZONE('UTC','America/Los_Angeles', sysdate()::TIMESTAMP) as LOAD_DATE
from {environment}.DQM.DQM_FACT 
where
(product = 'Prelim' and escrow_date is not null )

  );
 
insert into {environment}.DQM.DQM_SNAPSHOT_HOURLY (ID, DQM_SOURCE_ID, VALUES_CHECKED, RULE_ID, IDENTIFIED_DATE, LOAD_DATE)
(
select
'DQM13.0'||'-'||filenumber as ID
, filenumber as DQM_SOURCE_ID
, OBJECT_CONSTRUCT_KEEP_NULL('actual_settlement_date',actual_settlement_date, 'disbursement_date',disbursement_date, 'file_status',file_status, 'product', product  ) as VALUES_CHECKED
, 'DQM13.0' as RULE_ID
--, 'TRUE' as FLAGGED
, CONVERT_TIMEZONE('UTC','America/Los_Angeles', sysdate()::TIMESTAMP) as IDENTIFIED_DATE
--, '1900-01-01' as RESOLVED_DATE
, CONVERT_TIMEZONE('UTC','America/Los_Angeles', sysdate()::TIMESTAMP) as LOAD_DATE
from {environment}.DQM.DQM_FACT
where
--(actual_settlement_date < date_trunc( day,date_trunc( day,current_timestamp()))  and product = 'Purchase' and statusid = 2)
({environment}.DQM.business_days(ACTUAL_SETTLEMENT_DATE,DISBURSEMENT_DATE) > 1 and statusid = 2 and product = 'Purchase' )
  );

insert into {environment}.DQM.DQM_SNAPSHOT_HOURLY (ID, DQM_SOURCE_ID, VALUES_CHECKED, RULE_ID, IDENTIFIED_DATE, LOAD_DATE)
(
select
'DQM14.0'||'-'||filenumber as ID
, filenumber as DQM_SOURCE_ID
, OBJECT_CONSTRUCT_KEEP_NULL('resware_product_name', resware_product_name, 'file_type', file_type ) as VALUES_CHECKED
, 'DQM14.0' as RULE_ID
--, 'TRUE' as FLAGGED
, CONVERT_TIMEZONE('UTC','America/Los_Angeles', sysdate()::TIMESTAMP) as IDENTIFIED_DATE
--, '1900-01-01' as RESOLVED_DATE
, CONVERT_TIMEZONE('UTC','America/Los_Angeles', sysdate()::TIMESTAMP) as LOAD_DATE
from {environment}.DQM.DQM_FACT  
where
( resware_product_id in (32) AND file_type not in ('Title Only')  )

  );

insert into {environment}.DQM.DQM_SNAPSHOT_HOURLY (ID, DQM_SOURCE_ID, VALUES_CHECKED, RULE_ID, IDENTIFIED_DATE, LOAD_DATE)
(
select
'DQM16.0'||'-'||filenumber as ID
, filenumber as DQM_SOURCE_ID
, OBJECT_CONSTRUCT_KEEP_NULL('resware_product_name', resware_product_name, 'open_order_entry_start_date', open_order_entry_start_date, 'file_status', file_status ) as VALUES_CHECKED
, 'DQM16.0' as RULE_ID
--, 'TRUE' as FLAGGED
, CONVERT_TIMEZONE('UTC','America/Los_Angeles', sysdate()::TIMESTAMP) as IDENTIFIED_DATE
--, '1900-01-01' as RESOLVED_DATE
, CONVERT_TIMEZONE('UTC','America/Los_Angeles', sysdate()::TIMESTAMP) as LOAD_DATE
from {environment}.DQM.DQM_FACT 
where
(resware_product_id in (22) and statusid = 2 and open_order_entry_start_date is null  )

  );
 
insert into {environment}.DQM.DQM_SNAPSHOT_HOURLY (ID, DQM_SOURCE_ID, VALUES_CHECKED, RULE_ID, IDENTIFIED_DATE, LOAD_DATE)
(
select
'DQM17.0'||'-'||filenumber as ID
, filenumber as DQM_SOURCE_ID
, OBJECT_CONSTRUCT_KEEP_NULL('resware_client', resware_client, 'file_status', file_status ) as VALUES_CHECKED
, 'DQM17.0' as RULE_ID
--, 'TRUE' as FLAGGED
, CONVERT_TIMEZONE('UTC','America/Los_Angeles', sysdate()::TIMESTAMP) as IDENTIFIED_DATE
--, '1900-01-01' as RESOLVED_DATE
, CONVERT_TIMEZONE('UTC','America/Los_Angeles', sysdate()::TIMESTAMP) as LOAD_DATE
from {environment}.DQM.DQM_FACT  
where
(statusid = 2 and (lower(resware_client) = 'admin' or lower(resware_client) is null ) )

  );
 
insert into {environment}.DQM.DQM_SNAPSHOT_HOURLY (ID, DQM_SOURCE_ID, VALUES_CHECKED, RULE_ID, IDENTIFIED_DATE, LOAD_DATE)
(
select
'DQM18.0'||'-'||filenumber as ID
, filenumber as DQM_SOURCE_ID
, OBJECT_CONSTRUCT_KEEP_NULL('resware_product_name', resware_product_name, 'm1_end_date', m1_end_date, 'buyer_primary_email', buyer_primary_email, 'buyer_secondary_email', buyer_secondary_email ) as VALUES_CHECKED
, 'DQM18.0' as RULE_ID
--, 'TRUE' as FLAGGED
, CONVERT_TIMEZONE('UTC','America/Los_Angeles', sysdate()::TIMESTAMP) as IDENTIFIED_DATE
--, '1900-01-01' as RESOLVED_DATE
, CONVERT_TIMEZONE('UTC','America/Los_Angeles', sysdate()::TIMESTAMP) as LOAD_DATE
from {environment}.DQM.DQM_FACT
where
(resware_product_id in (3, 5, 32, 39, 40) and m1_end_date is not null and  (buyer_primary_email is null or buyer_secondary_email is null) )

  );
 
insert into {environment}.DQM.DQM_SNAPSHOT_HOURLY (ID, DQM_SOURCE_ID, VALUES_CHECKED, RULE_ID, IDENTIFIED_DATE, LOAD_DATE)
(
select
'DQM19.0'||'-'||filenumber as ID
, filenumber as DQM_SOURCE_ID
, OBJECT_CONSTRUCT_KEEP_NULL('resware_product_name', resware_product_name, 'escrow_date', escrow_date ) as VALUES_CHECKED
, 'DQM19.0' as RULE_ID
--, 'TRUE' as FLAGGED
, CONVERT_TIMEZONE('UTC','America/Los_Angeles', sysdate()::TIMESTAMP) as IDENTIFIED_DATE
--, '1900-01-01' as RESOLVED_DATE
, CONVERT_TIMEZONE('UTC','America/Los_Angeles', sysdate()::TIMESTAMP) as LOAD_DATE
from {environment}.DQM.DQM_FACT 
where
(resware_product_id in (32) and escrow_date is null )

  );
 
insert into {environment}.DQM.DQM_SNAPSHOT_HOURLY (ID, DQM_SOURCE_ID, VALUES_CHECKED, RULE_ID, IDENTIFIED_DATE, LOAD_DATE)
(
select
'DQM20.0'||'-'||filenumber as ID
, filenumber as DQM_SOURCE_ID
, OBJECT_CONSTRUCT_KEEP_NULL('resware_product_name', resware_product_name, 'm7_start_date', m7_start_date, 'file_status', file_status ) as VALUES_CHECKED
, 'DQM20.0' as RULE_ID
--, 'TRUE' as FLAGGED
, CONVERT_TIMEZONE('UTC','America/Los_Angeles', sysdate()::TIMESTAMP) as IDENTIFIED_DATE
--, '1900-01-01' as RESOLVED_DATE
, CONVERT_TIMEZONE('UTC','America/Los_Angeles', sysdate()::TIMESTAMP) as LOAD_DATE
from {environment}.DQM.DQM_FACT 
where
(resware_product_id in (32) and m7_start_date is not null and statusid != 9  )

  );

insert into {environment}.DQM.DQM_SNAPSHOT_HOURLY (ID, DQM_SOURCE_ID, VALUES_CHECKED, RULE_ID, IDENTIFIED_DATE, LOAD_DATE)
(
select
'DQM21.0'||'-'||filenumber as ID
, filenumber as DQM_SOURCE_ID
, OBJECT_CONSTRUCT_KEEP_NULL('curative_count', curative_count,'curatives_values', curatives_values ) as VALUES_CHECKED
, 'DQM21.0' as RULE_ID
--, 'TRUE' as FLAGGED
, CONVERT_TIMEZONE('UTC','America/Los_Angeles', sysdate()::TIMESTAMP) as IDENTIFIED_DATE
--, '1900-01-01' as RESOLVED_DATE
, CONVERT_TIMEZONE('UTC','America/Los_Angeles', sysdate()::TIMESTAMP) as LOAD_DATE
from {environment}.DQM.DQM_FACT 
where
( curative_count > 1  )

  );  

insert into {environment}.DQM.DQM_SNAPSHOT_HOURLY (ID, DQM_SOURCE_ID, VALUES_CHECKED, RULE_ID, IDENTIFIED_DATE, LOAD_DATE)
(
select
'DQM22.0'||'-'||filenumber as ID
, filenumber as DQM_SOURCE_ID
, OBJECT_CONSTRUCT_KEEP_NULL('escrow_id', escrow_id, 'resware_product_name', resware_product_name ) as VALUES_CHECKED
, 'DQM22.0' as RULE_ID
--, 'TRUE' as FLAGGED
, CONVERT_TIMEZONE('UTC','America/Los_Angeles', sysdate()::TIMESTAMP) as IDENTIFIED_DATE
--, '1900-01-01' as RESOLVED_DATE
, CONVERT_TIMEZONE('UTC','America/Los_Angeles', sysdate()::TIMESTAMP) as LOAD_DATE
from {environment}.DQM.DQM_FACT 
where
( escrow_id is null and resware_product_id != 32 )

  ); 

insert into {environment}.DQM.DQM_SNAPSHOT_HOURLY (ID, DQM_SOURCE_ID, VALUES_CHECKED, RULE_ID, IDENTIFIED_DATE, LOAD_DATE)
(
select
'DQM23.0'||'-'||filenumber as ID
, filenumber as DQM_SOURCE_ID
, OBJECT_CONSTRUCT_KEEP_NULL('escrow_date', escrow_date, 'closed_date', closed_date ) as VALUES_CHECKED
, 'DQM23.0' as RULE_ID
--, 'TRUE' as FLAGGED
, CONVERT_TIMEZONE('UTC','America/Los_Angeles', sysdate()::TIMESTAMP) as IDENTIFIED_DATE
--, '1900-01-01' as RESOLVED_DATE
, CONVERT_TIMEZONE('UTC','America/Los_Angeles', sysdate()::TIMESTAMP) as LOAD_DATE
from {environment}.DQM.DQM_FACT 
where
( (escrow_date > date_trunc( day,date_trunc( day,current_timestamp())) or escrow_date > closed_date) )

  ); 

insert into {environment}.DQM.DQM_SNAPSHOT_HOURLY (ID, DQM_SOURCE_ID, VALUES_CHECKED, RULE_ID, IDENTIFIED_DATE, LOAD_DATE)
(
select
'DQM24.0'||'-'||filenumber as ID
, filenumber as DQM_SOURCE_ID
, OBJECT_CONSTRUCT_KEEP_NULL('market_operational', market_operational, 'market', market, 'county', county, 'state', state ) as VALUES_CHECKED
, 'DQM24.0' as RULE_ID
--, 'TRUE' as FLAGGED
, CONVERT_TIMEZONE('UTC','America/Los_Angeles', sysdate()::TIMESTAMP) as IDENTIFIED_DATE
--, '1900-01-01' as RESOLVED_DATE
, CONVERT_TIMEZONE('UTC','America/Los_Angeles', sysdate()::TIMESTAMP) as LOAD_DATE
from {environment}.DQM.DQM_FACT 
where
( market_operational is null )

  ); 

insert into {environment}.DQM.DQM_SNAPSHOT_HOURLY (ID, DQM_SOURCE_ID, VALUES_CHECKED, RULE_ID, IDENTIFIED_DATE, LOAD_DATE)
(
select
'DQM25.0'||'-'||filenumber as ID
, filenumber as DQM_SOURCE_ID
, OBJECT_CONSTRUCT_KEEP_NULL('m1_end_date', m1_end_date, 'escrow_date', escrow_date ) as VALUES_CHECKED
, 'DQM25.0' as RULE_ID
--, 'TRUE' as FLAGGED
, CONVERT_TIMEZONE('UTC','America/Los_Angeles', sysdate()::TIMESTAMP) as IDENTIFIED_DATE
--, '1900-01-01' as RESOLVED_DATE
, CONVERT_TIMEZONE('UTC','America/Los_Angeles', sysdate()::TIMESTAMP) as LOAD_DATE
from {environment}.DQM.DQM_FACT
where
( m1_end_date < escrow_date )

  ); 

insert into {environment}.DQM.DQM_SNAPSHOT_HOURLY (ID, DQM_SOURCE_ID, VALUES_CHECKED, RULE_ID, IDENTIFIED_DATE, LOAD_DATE)
(
select
'DQM26.0'||'-'||filenumber as ID
, filenumber as DQM_SOURCE_ID
, OBJECT_CONSTRUCT_KEEP_NULL('client_company', client_company, 'buyers_entity', buyers_entity, 'sellers_entity', sellers_entity, 'enterprise_channel', enterprise_channel ) as VALUES_CHECKED
, 'DQM26.0' as RULE_ID
--, 'TRUE' as FLAGGED
, CONVERT_TIMEZONE('UTC','America/Los_Angeles', sysdate()::TIMESTAMP) as IDENTIFIED_DATE
--, '1900-01-01' as RESOLVED_DATE
, CONVERT_TIMEZONE('UTC','America/Los_Angeles', sysdate()::TIMESTAMP) as LOAD_DATE
from {environment}.DQM.DQM_FACT 
where
(((client_company ilike '%Knock%' or client_company ilike '%RedfinNow%' or client_company ilike '%Offerpad%' or client_company ilike '%Flyhomes%' or client_company ilike '%Easyknock%' ) or (BUYERS_ENTITY ilike '%Knock%' or BUYERS_ENTITY ilike '%RedfinNow%' or BUYERS_ENTITY ilike '%Offerpad%' or BUYERS_ENTITY ilike '%Flyhomes%' or BUYERS_ENTITY ilike '%Easyknock%' ) or (SELLERS_ENTITY ilike '%Knock%' or SELLERS_ENTITY ilike '%RedfinNow%' or SELLERS_ENTITY ilike '%Offerpad%' or SELLERS_ENTITY ilike '%Flyhomes%' or SELLERS_ENTITY ilike '%Easyknock%' )) and ENTERPRISE_CHANNEL = 'LOCAL' )

  ); 

insert into {environment}.DQM.DQM_SNAPSHOT_HOURLY (ID, DQM_SOURCE_ID, VALUES_CHECKED, RULE_ID, IDENTIFIED_DATE, LOAD_DATE)
(
select
'DQM27.0'||'-'||filenumber as ID
, filenumber as DQM_SOURCE_ID
, OBJECT_CONSTRUCT_KEEP_NULL('actionlisttransactiontypeid', actionlisttransactiontypeid ) as VALUES_CHECKED
, 'DQM27.0' as RULE_ID
--, 'TRUE' as FLAGGED
, CONVERT_TIMEZONE('UTC','America/Los_Angeles', sysdate()::TIMESTAMP) as IDENTIFIED_DATE
--, '1900-01-01' as RESOLVED_DATE
, CONVERT_TIMEZONE('UTC','America/Los_Angeles', sysdate()::TIMESTAMP) as LOAD_DATE
from {environment}.DQM.DQM_FACT A
left join {environment}.DQM.DQM_UNRESOLVED_VW B on ('DQM27.0'||'-'||A.filenumber) = B.ID
where
(actionlisttransactiontypeid != 3 )

  ); 

insert into {environment}.DQM.DQM_SNAPSHOT_HOURLY (ID, DQM_SOURCE_ID, VALUES_CHECKED, RULE_ID, IDENTIFIED_DATE, LOAD_DATE)
(
select
'DQM28.0'||'-'||filenumber as ID
, filenumber as DQM_SOURCE_ID
, OBJECT_CONSTRUCT_KEEP_NULL('m8_end_date', m8_end_date, 'm7_end_date', m7_end_date, 'resware_product_name', resware_product_name  ) as VALUES_CHECKED
, 'DQM28.0' as RULE_ID
--, 'TRUE' as FLAGGED
, CONVERT_TIMEZONE('UTC','America/Los_Angeles', sysdate()::TIMESTAMP) as IDENTIFIED_DATE
--, '1900-01-01' as RESOLVED_DATE
, CONVERT_TIMEZONE('UTC','America/Los_Angeles', sysdate()::TIMESTAMP) as LOAD_DATE
from {environment}.DQM.DQM_FACT 
where
( m8_end_date is not null and m7_end_date is null and {environment}.DQM.business_days(m8_end_date,current_timestamp()) > 1 )

  ); 

insert into {environment}.DQM.DQM_SNAPSHOT_HOURLY (ID, DQM_SOURCE_ID, VALUES_CHECKED, RULE_ID, IDENTIFIED_DATE, LOAD_DATE)
(
select
'DQM29.0'||'-'||filenumber as ID
, filenumber as DQM_SOURCE_ID
, OBJECT_CONSTRUCT_KEEP_NULL('m8_end_date', m8_end_date, 'm7_end_date', m7_end_date, 'resware_product_name', resware_product_name ) as VALUES_CHECKED
, 'DQM29.0' as RULE_ID
--, 'TRUE' as FLAGGED
, CONVERT_TIMEZONE('UTC','America/Los_Angeles', sysdate()::TIMESTAMP) as IDENTIFIED_DATE
--, '1900-01-01' as RESOLVED_DATE
, CONVERT_TIMEZONE('UTC','America/Los_Angeles', sysdate()::TIMESTAMP) as LOAD_DATE
from {environment}.DQM.DQM_FACT A
left join {environment}.DQM.DQM_UNRESOLVED_VW B on ('DQM29.0'||'-'||A.filenumber) = B.ID
where
( m7_end_date is not null and m8_end_date is null and {environment}.DQM.business_days(m7_end_date,current_timestamp()) > 1)

  ); 

insert into {environment}.DQM.DQM_SNAPSHOT_HOURLY (ID, DQM_SOURCE_ID, VALUES_CHECKED, RULE_ID, IDENTIFIED_DATE, LOAD_DATE)
(
select
'DQM30.0'||'-'||filenumber as ID
, filenumber as DQM_SOURCE_ID
, OBJECT_CONSTRUCT_KEEP_NULL('actual_settlement_date', actual_settlement_date, 'disbursement_date', disbursement_date, 'escrow_date', escrow_date ) as VALUES_CHECKED
, 'DQM30.0' as RULE_ID
--, 'TRUE' as FLAGGED
, CONVERT_TIMEZONE('UTC','America/Los_Angeles', sysdate()::TIMESTAMP) as IDENTIFIED_DATE
--, '1900-01-01' as RESOLVED_DATE
, CONVERT_TIMEZONE('UTC','America/Los_Angeles', sysdate()::TIMESTAMP) as LOAD_DATE
from {environment}.DQM.DQM_FACT 
where
( actual_settlement_date < escrow_date or disbursement_date < escrow_date )

  ); 
 
insert into {environment}.DQM.DQM_SNAPSHOT_HOURLY (ID, DQM_SOURCE_ID, VALUES_CHECKED, RULE_ID, IDENTIFIED_DATE, LOAD_DATE)
(
select
'DQM31.0'||'-'||filenumber as ID
, filenumber as DQM_SOURCE_ID
, OBJECT_CONSTRUCT_KEEP_NULL('actual_settlement_date', actual_settlement_date, 'disbursement_date', disbursement_date, 'product', product ) as VALUES_CHECKED
, 'DQM31.0' as RULE_ID
--, 'TRUE' as FLAGGED
, CONVERT_TIMEZONE('UTC','America/Los_Angeles', sysdate()::TIMESTAMP) as IDENTIFIED_DATE
--, '1900-01-01' as RESOLVED_DATE
, CONVERT_TIMEZONE('UTC','America/Los_Angeles', sysdate()::TIMESTAMP) as LOAD_DATE
from {environment}.DQM.DQM_FACT 
where
( {environment}.DQM.business_days(actual_settlement_date, disbursement_date) > 1 and product = 'Purchase' )

  );   
 
insert into {environment}.DQM.DQM_SNAPSHOT_HOURLY (ID, DQM_SOURCE_ID, VALUES_CHECKED, RULE_ID, IDENTIFIED_DATE, LOAD_DATE)
(
select
'DQM34.0'||'-'||filenumber as ID
, filenumber as DQM_SOURCE_ID
, OBJECT_CONSTRUCT_KEEP_NULL('actual_settlement_date', actual_settlement_date, 'disbursement_date', disbursement_date, 'product', product, 'file_status', file_status ) as VALUES_CHECKED
, 'DQM34.0' as RULE_ID
--, 'TRUE' as FLAGGED
, CONVERT_TIMEZONE('UTC','America/Los_Angeles', sysdate()::TIMESTAMP) as IDENTIFIED_DATE
--, '1900-01-01' as RESOLVED_DATE
, CONVERT_TIMEZONE('UTC','America/Los_Angeles', sysdate()::TIMESTAMP) as LOAD_DATE
from {environment}.DQM.DQM_FACT A
where
( {environment}.DQM.business_days(actual_settlement_date, disbursement_date) > 4 and product = 'Refinance'  and statusid = 9)

  );  


insert into {environment}.DQM.DQM_SNAPSHOT_HOURLY (ID, DQM_SOURCE_ID, VALUES_CHECKED, RULE_ID, IDENTIFIED_DATE, LOAD_DATE)
(
select
'DQM35.0'||'-'||filenumber as ID
, filenumber as DQM_SOURCE_ID
, OBJECT_CONSTRUCT_KEEP_NULL('actual_settlement_date', actual_settlement_date, 'disbursement_date', disbursement_date, 'escrow_date', escrow_date ) as VALUES_CHECKED
, 'DQM35.0' as RULE_ID
--, 'TRUE' as FLAGGED
, CONVERT_TIMEZONE('UTC','America/Los_Angeles', sysdate()::TIMESTAMP) as IDENTIFIED_DATE
--, '1900-01-01' as RESOLVED_DATE
, CONVERT_TIMEZONE('UTC','America/Los_Angeles', sysdate()::TIMESTAMP) as LOAD_DATE
from {environment}.DQM.DQM_FACT 
where
( actual_settlement_date > disbursement_date and statusid = 9)

  ); 


insert into {environment}.DQM.DQM_SNAPSHOT_HOURLY (ID, DQM_SOURCE_ID, VALUES_CHECKED, RULE_ID, IDENTIFIED_DATE, LOAD_DATE)
(
select
'DQM36.0'||'-'||filenumber as ID
, filenumber as DQM_SOURCE_ID
, OBJECT_CONSTRUCT_KEEP_NULL('county', county, 'file_status', file_status, 'state', state) as VALUES_CHECKED
, 'DQM36.0' as RULE_ID
--, 'TRUE' as FLAGGED
, CONVERT_TIMEZONE('UTC','America/Los_Angeles', sysdate()::TIMESTAMP) as IDENTIFIED_DATE
--, '1900-01-01' as RESOLVED_DATE
, CONVERT_TIMEZONE('UTC','America/Los_Angeles', sysdate()::TIMESTAMP) as LOAD_DATE
from {environment}.DQM.DQM_FACT 
where
( (county = 'Fremont' ) and state = 'CO' and statusid = 2 )

  ); 

  insert into {environment}.DQM.DQM_SNAPSHOT_HOURLY (ID, DQM_SOURCE_ID, VALUES_CHECKED, RULE_ID, IDENTIFIED_DATE, LOAD_DATE)
(
select
'DQM37.0'||'-'||A.id as ID
, A.id as DQM_SOURCE_ID
, OBJECT_CONSTRUCT_KEEP_NULL('id', A.id, 'invocation_id', invocation_id, 'audit_type', audit_type, 'model_name', model_name, 'target_max_time', target_max_time ) as VALUES_CHECKED
, 'DQM37.0' as RULE_ID
--, 'TRUE' as FLAGGED
, CONVERT_TIMEZONE('UTC','America/Los_Angeles', sysdate()::TIMESTAMP) as IDENTIFIED_DATE
--, '1900-01-01' as RESOLVED_DATE
, CONVERT_TIMEZONE('UTC','America/Los_Angeles', sysdate()::TIMESTAMP) as LOAD_DATE
from {environment}.DBT_AUDIT.TARGET_WATERMARK A
where
( target_max_time > sysdate() )
  );

insert into {environment}.DQM.DQM_SNAPSHOT_HOURLY (ID, DQM_SOURCE_ID, VALUES_CHECKED, RULE_ID, IDENTIFIED_DATE, LOAD_DATE)
(
select
'DQM38.0'||'-'||E.filenumber as ID
, E.filenumber as DQM_SOURCE_ID
, OBJECT_CONSTRUCT_KEEP_NULL('fileid', E.fileid, 'filenumber', E.filenumber, 'file_status', E.file_status, 'county', E.county, 'state', E.state, 'actiondefid', E.actiondefid ) as VALUES_CHECKED
, 'DQM38.0' as RULE_ID
--, 'TRUE' as FLAGGED
, CONVERT_TIMEZONE('UTC','America/Los_Angeles', sysdate()::TIMESTAMP) as IDENTIFIED_DATE
--, '1900-01-01' as RESOLVED_DATE
, CONVERT_TIMEZONE('UTC','America/Los_Angeles', sysdate()::TIMESTAMP) as LOAD_DATE
from (  select A.fileid, A.filenumber, A.file_status, A.state, A.county, B.actiondefid  
        from prod.infomart.orders_vw A 
        join lake.resware.fileactions_current B on A.fileid = B.fileid
        where A.file_status = 'Closed' and B.actiondefid = 597) E
left join ( select distinct D.fileid
            from (  select A.fileid
                    from prod.infomart.orders_vw A 
                    join lake.resware.fileactions_current B on A.fileid = B.fileid
                    where A.file_status = 'Closed' and B.actiondefid = 597) D
            join (  select B.fileid, A.documenttypeid, A.name  
                    from lake.resware.documenttype_current A
                    join lake.resware.document_current B on A.documenttypeid = B.documenttypeid) C on D.fileid = C.fileid
            where (C.documenttypeid = 1243 or C.documenttypeid = 1245)) F on E.fileid = F.fileid
where 
( F.fileid is null )
   );
   
insert into {environment}.DQM.DQM_SNAPSHOT_HOURLY (ID, DQM_SOURCE_ID, VALUES_CHECKED, RULE_ID, IDENTIFIED_DATE, LOAD_DATE)
(
select
'DQM39.0'||'-'||E.filenumber as ID
, E.filenumber as DQM_SOURCE_ID
, OBJECT_CONSTRUCT_KEEP_NULL('fileid', E.fileid, 'filenumber', E.filenumber, 'file_status', E.file_status, 'county', E.county, 'state', E.state) as VALUES_CHECKED
, 'DQM39.0' as RULE_ID
--, 'TRUE' as FLAGGED
, CONVERT_TIMEZONE('UTC','America/Los_Angeles', sysdate()::TIMESTAMP) as IDENTIFIED_DATE
--, '1900-01-01' as RESOLVED_DATE
, CONVERT_TIMEZONE('UTC','America/Los_Angeles', sysdate()::TIMESTAMP) as LOAD_DATE
from
(select A.* from prod.infomart.orders_vw A
 join
         lake.resware.fileactions_current B
         on A.fileid = B.fileid
 where
	  		(
                (state = 'WA' AND county = 'King')
	     OR (state = 'CA' AND county = 'Los Angeles')
	     OR (state = 'CA' AND county = 'San Diego')
	     OR (state = 'CA' AND county = 'San Francisco')
	     OR (state = 'CA' AND county = 'San Mateo')
	     OR (state = 'CA' AND county = 'Santa Clara')
	     OR (state = 'TX' AND county = 'Bexar')
	     OR (state = 'TX' AND county = 'Tarrant')
	     OR (state = 'TX' AND county = 'Dallas')
	     OR (state = 'FL' AND county = 'Miami-Dade')
	     OR (state = 'FL' AND county = 'Broward')
	     OR (state = 'FL' AND county = 'Palm Beach')
	     OR (state = 'IL' AND county = 'Cook')
	     OR (state = 'MA' AND county = 'Suffolk')
	     OR (state = 'MA' AND county = 'Middlesex')
	     OR (state = 'NV' AND county = 'Clark')
	     OR (state = 'HI' AND county = 'Honolulu')
            )
         and A.file_status = 'Closed'
and B.actiondefid = 322) E
         left join
         (
         select distinct D.fileid    from
         (select A.fileid from prod.infomart.orders_vw A

      join 
     lake.resware.fileactions_current B 
     on A.fileid = B.fileid where 
  		(
            (state = 'WA' AND county = 'King')
     OR (state = 'CA' AND county = 'Los Angeles')
     OR (state = 'CA' AND county = 'San Diego')
     OR (state = 'CA' AND county = 'San Francisco')
     OR (state = 'CA' AND county = 'San Mateo')
     OR (state = 'CA' AND county = 'Santa Clara')
     OR (state = 'TX' AND county = 'Bexar')
     OR (state = 'TX' AND county = 'Tarrant')
     OR (state = 'TX' AND county = 'Dallas')
     OR (state = 'FL' AND county = 'Miami-Dade')
     OR (state = 'FL' AND county = 'Broward')
     OR (state = 'FL' AND county = 'Palm Beach')
     OR (state = 'IL' AND county = 'Cook')
     OR (state = 'MA' AND county = 'Suffolk')
     OR (state = 'MA' AND county = 'Middlesex')
     OR (state = 'NV' AND county = 'Clark')
     OR (state = 'HI' AND county = 'Honolulu')
        )
     and A.file_status = 'Closed'
     and B.actiondefid = 322) D
     join
     (select B.fileid, A.documenttypeid, A.name  from lake.resware.documenttype_current A 
     join 
     lake.resware.document_current B
     on A.documenttypeid = B.documenttypeid
      ) C
     on D.fileid = C.fileid
     where C.documenttypeid = 1367
     ) F
     on E.fileid = F.fileid
where 
( F.fileid is null )
   );
   