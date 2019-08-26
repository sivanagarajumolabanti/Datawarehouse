drop table if exists bi_stage.stg_sfdc_account;

create table if not exists bi_stage.stg_sfdc_account
(
	id varchar not null,
	name varchar,
	billingcity varchar,
	billingstate varchar,
	billingcountry varchar,
	ownerid varchar,
	createddate timestamp,
	lastmodifieddate timestamp,
	size__c varchar,
	account_manager__c varchar,
	brand_account__c varchar, 
	account_balance__c numeric(10,2),
	account_overdue_balance__c numeric(10,2),
	days_overdue__c int,
	netsuite_id__c varchar,
	unbilled_orders__c int,
	adtech_partner__c bool,
	region__c varchar,
	adtech_spm__c  varchar,
	industry varchar
);

alter table if exists bi_stage.stg_sfdc_account add constraint pk_stg_sfdc_account_id primary key (id);

commit;

