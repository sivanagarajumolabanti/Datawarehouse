select
	id,
	name,
	billingcity,
	billingstate,
	billingcountry,
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
	createddate,
	lastmodifieddate,
	industry
from account 
where createddate = LAST_N_DAYS:3 or lastmodifieddate = LAST_N_DAYS:3