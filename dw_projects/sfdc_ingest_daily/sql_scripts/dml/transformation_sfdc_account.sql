insert into bi_backups.d_sfdc_account_deleted
select *,now() from bi_prod.d_sfdc_account a
where 1=1
and account_key>0
and not exists (select 1 from bi_stage.stg_sfdc_account b where a.account_id=b.id );

delete from bi_prod.d_sfdc_account a
where 1=1
and account_key>0
and not exists (select 1 from bi_stage.stg_sfdc_account b where a.account_id=b.id );

update bi_prod.d_sfdc_account tgt
	set 
		name 					= 	stg.name,
		billing_city 			= 	billingcity,
		billing_state 			= 	billingstate,
		billing_country 		= 	billingcountry,
		user_key 				= 	coalesce(usr.user_key,-999),
		owner_id 				= 	ownerid,
		size 					= 	size__c,
		account_manager 		= 	account_manager__c,
		brand_account 			= 	brand_account__c,
		account_balance 		= 	account_balance__c,
		account_overdue_balance = 	account_overdue_balance__c,
		days_overdue 			= 	days_overdue__c,
		netsuite_id 			= 	netsuite_id__c,
		unbilled_orders 		= 	unbilled_orders__c,
		ad_tech_partner 		= 	adtech_partner__c,
		region 					= 	region__c,
		ad_tech_spm 			= 	adtech_spm__c,
		industry				= 	stg.industry,
		created_date 			= 	createddate,
		last_modified_date 		= 	lastmodifieddate,
		dw_update_timestamp 	= 	now()
from
bi_stage.stg_sfdc_account stg 
left join bi_prod.d_sfdc_user usr on usr.user_id = stg.ownerid
where 1=1
and stg.id=tgt.account_id
and 
	(
		tgt.name <>stg.name or
		billing_city <> billingcity or
		billing_state <> billingstate or
		billing_country <> billingcountry or
		tgt.user_key <> coalesce(usr.user_key) or
		owner_id <> ownerid or
		size <> size__c or
		account_manager <> account_manager__c or
		brand_account <> brand_account__c or
		account_balance <> account_balance__c or 
		account_overdue_balance <> account_overdue_balance__c or
		days_overdue <> days_overdue__c or
		netsuite_id <> netsuite_id__c or
		unbilled_orders <> unbilled_orders__c or
		ad_tech_partner <> adtech_partner__c or
		region <> region__c or
		ad_tech_spm <> adtech_spm__c or
		tgt.industry <> stg.industry

	);


insert into bi_prod.d_sfdc_account
(
	account_id,
    name,                    
    billing_city,            
    billing_state,           
    billing_country,         
    user_key,
    owner_id,                
    size,                    
    account_manager,         
    brand_account,           
    account_balance,
    account_overdue_balance,
    days_overdue,
    netsuite_id,
    unbilled_orders,
    ad_tech_partner,
    region,                  
    ad_tech_spm,
    industry,             
    created_date,
    last_modified_date,
    dw_insert_timestamp,
    dw_update_timestamp
)
select
	id,
	stg.name,
	billingcity,
	billingstate,
	billingcountry,
	coalesce(usr.user_key,-999),
	ownerid,
	size__c,
	account_manager__c,
	brand_account__c,
	account_balance__c,
	account_overdue_balance__c,
	days_overdue__c,
	netsuite_id__c,
	unbilled_orders__c,
	adtech_partner__c,
	region__c,
	adtech_spm__c,
	stg.industry,
	createddate,
	lastmodifieddate,
	now(),
	now()
from
bi_stage.stg_sfdc_account stg
left join bi_prod.d_sfdc_account tgt on tgt.account_id=stg.id
left join bi_prod.d_sfdc_user usr on usr.user_id=stg.ownerid
where tgt.account_id is null;

-- insert into bi_prod.d_sfdc_account
-- (
-- 	account_id,
--     name,                    
--     billing_city,            
--     billing_state,           
--     billing_country,         
--     user_key,
--     owner_id,                
--     size,                    
--     account_manager,         
--     brand_account,           
--     account_balance,
--     account_overdue_balance,
--     days_overdue,
--     netsuite_id,
--     unbilled_orders,
--     ad_tech_partner,
--     region,                  
--     ad_tech_spm,             
--     created_date,
--     last_modified_date,
--     dw_insert_timestamp,
--     dw_update_timestamp
-- )
-- select
-- 	id,
-- 	stg.name,
-- 	billingcity,
-- 	billingstate,
-- 	billingcountry,
-- 	coalesce(usr.user_key),
-- 	ownerid,
-- 	size__c,
-- 	account_manager__c,
-- 	brand_account__c,
-- 	account_balance__c,
-- 	account_overdue_balance__c,
-- 	days_overdue__c,
-- 	netsuite_id__c,
-- 	unbilled_orders__c,
-- 	adtech_partner__c,
-- 	region__c,
-- 	adtech_spm__c,
-- 	createddate,
-- 	lastmodifieddate,
-- 	now(),
-- 	now()
-- from
-- bi_stage.stg_sfdc_account stg
-- left join bi_prod.d_sfdc_account tgt on tgt.account_id=stg.id
-- left join bi_prod.d_sfdc_user usr on usr.user_id=stg.ownerid
-- on conflict(account_id)  
-- do update
-- set
-- 	name=excluded.name,
-- 	billing_city=excluded.billing_city,
-- 	billing_state=excluded.billing_state,
-- 	billing_country=excluded.billing_country,
-- 	user_key=excluded.user_key,
-- 	owner_id=excluded.owner_id,
-- 	size=excluded.size,
-- 	account_manager=excluded.account_manager,
-- 	brand_account=excluded.brand_account,
-- 	account_balance=excluded.account_balance,
-- 	account_overdue_balance=excluded.account_overdue_balance,
-- 	days_overdue=excluded.days_overdue,
-- 	netsuite_id=excluded.netsuite_id,
-- 	unbilled_orders=excluded.unbilled_orders,
-- 	ad_tech_partner=excluded.ad_tech_partner,
-- 	region=excluded.region,
-- 	ad_tech_spm=excluded.ad_tech_spm,
-- 	created_date=excluded.created_date,
-- 	last_modified_date=excluded.last_modified_date,
-- 	dw_update_timestamp=now();

--truncate table bi_stage.stg_sfdc_account;	
