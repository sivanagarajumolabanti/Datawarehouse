insert into bi_backups.d_sfdc_opportunity_line_item_deleted
select *,now() from bi_prod.d_sfdc_opportunity_line_item a
where 1=1
and opport_line_item_key>0
and not exists (select 1 from bi_stage.stg_sfdc_opportunity_line_item b where a.opport_line_item_id=b.id );

delete from bi_prod.d_sfdc_opportunity_line_item a
where 1=1
and opport_line_item_key>0
and not exists (select 1 from bi_stage.stg_sfdc_opportunity_line_item b where a.opport_line_item_id=b.id );

update bi_prod.d_sfdc_opportunity_line_item tgt
set
	opportunity_key 	= 	coalesce(opp.opportunity_key,-999),
	opportunity_id 		= 	opportunityid,
	product_code 		= 	productcode,
	currency_iso_code 	= 	currencyisocode,
	net_suite_item_id 	=	netsuite_itemid__c,
	use_case 			= 	use_case__c,
	revenue_type 		= 	revenue_type__c,
	start_date 			= 	start_date__c,
	end_date 			= 	end_date__c,
	dw_update_timestamp	= 	now()
from
bi_stage.stg_sfdc_opportunity_line_item stg 
left join bi_prod.d_sfdc_opportunity opp on opp.opportunity_id=stg.opportunityid
where 1=1
and stg.id=tgt.opport_line_item_id
and
(
	tgt.opportunity_key <> coalesce(opp.opportunity_key,-999) or
	tgt.opportunity_id <> opportunityid or 
	tgt.product_code <> productcode or
	tgt.currency_iso_code <> currencyisocode or
	tgt.net_suite_item_id <> netsuite_itemid__c or
	tgt.use_case <> use_case__c or
	tgt.revenue_type <> revenue_type__c or
	tgt.start_date <> start_date__c or
	tgt.end_date <> end_date__c	
);

insert into bi_prod.d_sfdc_opportunity_line_item
(
	opport_line_item_id,
	opportunity_key,
	opportunity_id,
	product_code,
	currency_iso_code,
	net_suite_item_id,
	use_case,
	revenue_type,
	start_date,
	end_date,
	created_date,
	last_modified_date,
	dw_insert_timestamp,
	dw_update_timestamp
)
select
	id,
	coalesce(opp.opportunity_key,-999),
	opportunityid,
	productcode,
	currencyisocode,
	netsuite_itemid__c,
	use_case__c,
	revenue_type__c,
	start_date__c,
	end_date__c,
	createddate,
	lastmodifieddate,
	now(),
	now()
from
bi_stage.stg_sfdc_opportunity_line_item stg 
left join bi_prod.d_sfdc_opportunity_line_item tgt on tgt.opport_line_item_id=stg.id
left join bi_prod.d_sfdc_opportunity opp on opp.opportunity_id=stg.opportunityid
where tgt.opport_line_item_id is null;

-- insert into bi_prod.d_sfdc_opportunity_line_item
-- (
-- 	opport_line_item_id,
-- 	opportunity_key,
-- 	opportunity_id,
-- 	product_code,
-- 	currency_iso_code,
-- 	net_suite_item_id,
-- 	use_case,
-- 	revenue_type,
-- 	start_date,
-- 	end_date,
-- 	created_date,
-- 	last_modified_date,
-- 	dw_insert_timestamp,
-- 	dw_update_timestamp
-- )
-- select
-- 	id,
-- 	coalesce(opp.opportunity_key,-999),
-- 	opportunityid,
-- 	productcode,
-- 	currencyisocode,
-- 	netsuite_itemid__c,
-- 	use_case__c,
-- 	revenue_type__c,
-- 	start_date__c,
-- 	end_date__c,
-- 	createddate,
-- 	lastmodifieddate,
-- 	now(),
-- 	now()
-- from
-- bi_stage.stg_sfdc_opportunity_line_item stg 
-- left join bi_prod.d_sfdc_opportunity_line_item tgt on tgt.opport_line_item_id=stg.id
-- left join bi_prod.d_sfdc_opportunity opp on opp.opportunity_id=stg.opportunityid
-- on conflict(opport_line_item_id)
-- do update 
-- set
-- 	opport_line_item_id = excluded.opport_line_item_id,
-- 	opportunity_key = excluded.opportunity_key,
-- 	opportunity_id = excluded.opportunity_id,
-- 	product_code = excluded.product_code,
-- 	currency_iso_code = excluded.currency_iso_code,
-- 	net_suite_item_id = excluded.net_suite_item_id,
-- 	use_case = excluded.use_case,
-- 	revenue_type = excluded.revenue_type,
-- 	start_date = excluded.start_date,
-- 	end_date = excluded.end_date,
-- 	created_date = excluded.created_date,
-- 	last_modified_date = excluded.last_modified_date,
-- 	dw_update_timestamp=now();

--truncate table bi_stage.stg_sfdc_opportunity_line_item;
