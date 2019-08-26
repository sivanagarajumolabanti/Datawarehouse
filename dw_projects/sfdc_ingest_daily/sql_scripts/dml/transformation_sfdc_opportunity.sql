insert into bi_backups.d_sfdc_opportunity_deleted
select *,now() from bi_prod.d_sfdc_opportunity a
where 1=1
and opportunity_key>0
and not exists (select 1 from bi_stage.stg_sfdc_opportunity b where a.opportunity_id=b.id );

delete from bi_prod.d_sfdc_opportunity a
where 1=1
and opportunity_key>0
and not exists (select 1 from bi_stage.stg_sfdc_opportunity b where a.opportunity_id=b.id );

update bi_prod.d_sfdc_opportunity tgt
set
	account_key 					= 	coalesce(acct.account_key,-999),
	account_id 						= 	accountid,
	record_type_id					= 	recordtypeid,
	opportunity_name 				= 	stg.name,
	stage_name 						= 	stagename,
	amount 							= 	stg.amount,
	probability 					= 	stg.probability,
	close_date 						= 	closedate,
	lead_source 					= 	leadsource,
	currency_iso_code 				= 	currencyisocode,
	campaign_id 					= 	campaignid,
	user_key 						= 	coalesce(usr.user_key,-999),
	owner_id 						= 	ownerid,
	lost_reason 					= 	lost_reason__c,
	net_total_value 				= 	net_total_value__c,
	monthly_recurring_revenue 		= 	monthly_recurring_revenue__c,
	annual_contract_value 			= 	annual_contract_value__c,
	contract_start_date 			= 	contract_end_date__c,
	contract_end_date 				= 	contract_start_date__c,
	net_suite_contract_id 			= 	netsuite_contractid__c,
	net_suite_id 					= 	netsuite_id__c,
	auto_renew_days 				= 	autorenew_days__c,
	auto_renew_date 				= 	autorenew_date__c,
	order_type 						= 	order_type__c,
	contract_term 					= 	contract_term__c,
	ad_tech_partners 				= 	adtech_partners__c,
	campaign_start_date 			= 	campaign_start_date__c,
	campaign_end_date 				= 	campaign_end_date__c,
	lost_reason_picklist 			= 	lost_reason_picklist__c,
	direct_to_brand 				= 	direct_to_brand__c,
	opportunity_revenue_type 		= 	opportunity_revenue_type__c,
	region 							= 	region__c,
	opportunity_won_by 				= 	opportunity_won_by__c,
	upsell_cross_device 			= 	upsell_cross_device__c,
	created_at 						= 	createddate,
	last_modified_at 				= 	lastmodifieddate,
	dw_update_timestamp				=	now()
from bi_stage.stg_sfdc_opportunity stg 
left join bi_prod.d_sfdc_account acct on acct.account_id=stg.accountid
left join bi_prod.d_sfdc_user usr on usr.user_id=stg.ownerid
where 1=1
and stg.id=tgt.opportunity_id
and
(
	tgt.account_key <> coalesce(acct.account_key,-999) or
	tgt.account_id <> stg.accountid or
	record_type_id <> recordtypeid or
	opportunity_name <> stg.name or
	stage_name <> stagename or
	tgt.amount <> stg.amount or
	tgt.probability <> stg.probability or 
	close_date <> closedate or
	lead_source <> leadsource or
	currency_iso_code <> currencyisocode or
	campaign_id <> campaignid or
	tgt.user_key <> coalesce(usr.user_key,-999) or
	tgt.owner_id <> ownerid or
	lost_reason <> lost_reason__c or
	net_total_value <> net_total_value__c or
	monthly_recurring_revenue <> monthly_recurring_revenue__c or
	annual_contract_value <> annual_contract_value__c or
	contract_start_date <> contract_end_date__c or
	contract_end_date <> contract_start_date__c or
	net_suite_contract_id <> netsuite_contractid__c or
	net_suite_id <>	netsuite_id__c or
	auto_renew_days <> autorenew_days__c or
	auto_renew_date <> autorenew_date__c or
	order_type <> order_type__c or
	contract_term <> contract_term__c or
	ad_tech_partners <> adtech_partners__c or
	campaign_start_date <> campaign_start_date__c or
	campaign_end_date <> campaign_end_date__c or
	lost_reason_picklist <> lost_reason_picklist__c or
	direct_to_brand <> direct_to_brand__c or
	opportunity_revenue_type <> opportunity_revenue_type__c or
	tgt.region <> region__c or
	opportunity_won_by <> opportunity_won_by__c or
	upsell_cross_device <> upsell_cross_device__c
);

insert into bi_prod.d_sfdc_opportunity
(
	opportunity_id,
	account_key,
	account_id,
	record_type_id,
	opportunity_name,
	stage_name,
	amount,
	probability,
	close_date,
	lead_source,
	currency_iso_code,
	campaign_id,
	user_key,
	owner_id,
	lost_reason,
	net_total_value,
	monthly_recurring_revenue,
	annual_contract_value,
	contract_start_date,
	contract_end_date,
	net_suite_contract_id,
	net_suite_id,
	auto_renew_days,
	auto_renew_date,
	order_type,
	contract_term,
	ad_tech_partners,
	campaign_start_date,
	campaign_end_date,
	lost_reason_picklist,
	direct_to_brand,
	opportunity_revenue_type,
	region,
	opportunity_won_by,
	upsell_cross_device,
	created_at,
	last_modified_at,
	dw_insert_timestamp,
	dw_update_timestamp
) 
select
	id,
	coalesce(acct.account_key,-999),
	accountid,
	recordtypeid,
	stg.name,
	stagename,
	stg.amount,
	stg.probability,
	closedate,
	leadsource,
	currencyisocode,
	campaignid,
	coalesce(usr.user_key,-999),
	ownerid,
	lost_reason__c,
	net_total_value__c,
	monthly_recurring_revenue__c,
	annual_contract_value__c,
	contract_end_date__c,
	contract_start_date__c,
	netsuite_contractid__c,
	netsuite_id__c,
	autorenew_days__c,
	autorenew_date__c,
	order_type__c,
	contract_term__c,
	adtech_partners__c,
	campaign_start_date__c,
	campaign_end_date__c,
	lost_reason_picklist__c,
	direct_to_brand__c,
	opportunity_revenue_type__c,
	region__c,
	opportunity_won_by__c,
	upsell_cross_device__c,
	createddate,
	lastmodifieddate,	
	now(),
	now()
from bi_stage.stg_sfdc_opportunity stg 
left join bi_prod.d_sfdc_opportunity tgt on tgt.opportunity_id=stg.id
left join bi_prod.d_sfdc_account acct on acct.account_id=stg.accountid
left join bi_prod.d_sfdc_user usr on usr.user_id=stg.ownerid
where tgt.opportunity_id is null;


-- insert into bi_prod.d_sfdc_opportunity
-- (
-- 	opportunity_id,
-- 	account_key,
-- 	account_id,
-- 	record_type_id,
-- 	opportunity_name,
-- 	stage_name,
-- 	amount,
-- 	probability,
-- 	close_date,
-- 	lead_source,
-- 	currency_iso_code,
-- 	campaign_id,
-- 	user_key,
-- 	owner_id,
-- 	lost_reason,
-- 	net_total_value,
-- 	monthly_recurring_revenue,
-- 	annual_contract_value,
-- 	contract_start_date,
-- 	contract_end_date,
-- 	net_suite_contract_id,
-- 	net_suite_id,
-- 	auto_renew_days,
-- 	auto_renew_date,
-- 	order_type,
-- 	contract_term,
-- 	ad_tech_partners,
-- 	campaign_start_date,
-- 	campaign_end_date,
-- 	lost_reason_picklist,
-- 	direct_to_brand,
-- 	opportunity_revenue_type,
-- 	region,
-- 	opportunity_won_by,
-- 	upsell_cross_device,
-- 	created_at,
-- 	last_modified_at,
-- 	dw_insert_timestamp,
-- 	dw_update_timestamp
-- ) 
-- select
-- 	id,
-- 	coalesce(acct.account_key,-999),
-- 	accountid,
-- 	recordtypeid,
-- 	stg.name,
-- 	stagename,
-- 	stg.amount,
-- 	stg.probability,
-- 	closedate,
-- 	leadsource,
-- 	currencyisocode,
-- 	campaignid,
-- 	coalesce(usr.user_key,-999),
-- 	ownerid,
-- 	lost_reason__c,
-- 	net_total_value__c,
-- 	monthly_recurring_revenue__c,
-- 	annual_contract_value__c,
-- 	contract_end_date__c,
-- 	contract_start_date__c,
-- 	netsuite_contractid__c,
-- 	netsuite_id__c,
-- 	autorenew_days__c,
-- 	autorenew_date__c,
-- 	order_type__c,
-- 	contract_term__c,
-- 	adtech_partners__c,
-- 	campaign_start_date__c,
-- 	campaign_end_date__c,
-- 	lost_reason_picklist__c,
-- 	direct_to_brand__c,
-- 	opportunity_revenue_type__c,
-- 	region__c,
-- 	opportunity_won_by__c,
-- 	upsell_cross_device__c,
-- 	createddate,
-- 	lastmodifieddate,	
-- 	now(),
-- 	now()
-- from bi_stage.stg_sfdc_opportunity stg 
-- left join bi_prod.d_sfdc_opportunity tgt on tgt.opportunity_id=stg.id
-- left join bi_prod.d_sfdc_account acct on acct.account_id=stg.accountid
-- left join bi_prod.d_sfdc_user usr on usr.user_id=stg.ownerid
-- on conflict(opportunity_id)
-- do update 
-- set 
-- 	account_key=excluded.account_key,
-- 	account_id = excluded.account_id,
-- 	record_type_id = excluded.record_type_id,
-- 	opportunity_name = excluded.opportunity_name,
-- 	stage_name = excluded.stage_name,
-- 	amount = excluded.amount,
-- 	probability = excluded.probability,
-- 	close_date = excluded.close_date,
-- 	lead_source = excluded.lead_source,
-- 	currency_iso_code = excluded.currency_iso_code,
-- 	campaign_id = excluded.campaign_id,
-- 	user_key=excluded.user_key,
-- 	owner_id = excluded.owner_id,
-- 	lost_reason = excluded.lost_reason,
-- 	net_total_value = excluded.net_total_value,
-- 	monthly_recurring_revenue = excluded.monthly_recurring_revenue,
-- 	annual_contract_value = excluded.annual_contract_value,
-- 	contract_start_date = excluded.contract_start_date,
-- 	contract_end_date = excluded.contract_end_date,
-- 	net_suite_contract_id = excluded.net_suite_contract_id,
-- 	net_suite_id = excluded.net_suite_id,
-- 	auto_renew_days = excluded.auto_renew_days,
-- 	auto_renew_date = excluded.auto_renew_date,
-- 	order_type = excluded.order_type,
-- 	contract_term = excluded.contract_term,
-- 	ad_tech_partners = excluded.ad_tech_partners,
-- 	campaign_start_date = excluded.campaign_start_date,
-- 	campaign_end_date = excluded.campaign_end_date,
-- 	lost_reason_picklist = excluded.lost_reason_picklist,
-- 	direct_to_brand = excluded.direct_to_brand,
-- 	opportunity_revenue_type = excluded.opportunity_revenue_type,
-- 	region = excluded.region,
-- 	opportunity_won_by = excluded.opportunity_won_by,
-- 	upsell_cross_device = excluded.upsell_cross_device,
-- 	created_at = excluded.created_at,
-- 	last_modified_at = excluded.last_modified_at,
-- 	dw_update_timestamp=now();

--truncate table bi_stage.stg_sfdc_opportunity;
