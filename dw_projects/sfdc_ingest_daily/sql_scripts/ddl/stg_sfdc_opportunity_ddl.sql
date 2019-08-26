drop table if exists bi_stage.stg_sfdc_opportunity;

create table if not exists bi_stage.stg_sfdc_opportunity
(
	id 								varchar(36) not null,
	accountid 						varchar(36) not null,
	recordtypeid 					varchar null ,
	name 							varchar null ,
	stagename 						varchar null ,
	amount 							numeric(10,2) default 0,
	probability 					varchar null ,
	closedate 						date null ,
	leadsource 						varchar null ,
	currencyisocode 				varchar null ,
	campaignid 						varchar null ,
	ownerid 						varchar null ,
	lost_reason__c 					varchar null ,
	net_total_value__c 				numeric(10,2) default 0 ,
	monthly_recurring_revenue__c    numeric(10,2) default 0 ,
	annual_contract_value__c 		numeric(10,2) default 0 ,
	contract_end_date__c 			timestamp null ,
	contract_start_date__c 			timestamp null ,
	netsuite_contractid__c 			varchar null ,
	netsuite_id__c 					varchar null ,
	autorenew_days__c 				int null ,
	autorenew_date__c 				timestamp null ,
	order_type__c 					varchar null ,
	contract_term__c 				int null ,
	adtech_partners__c 				varchar null ,
	campaign_start_date__c 			timestamp null ,
	campaign_end_date__c 			timestamp null ,
	lost_reason_picklist__c 		varchar null ,
	direct_to_brand__c 				varchar null ,
	opportunity_revenue_type__c 	varchar null ,
	region__c 						varchar null ,
	opportunity_won_by__c 			varchar null ,
	upsell_cross_device__c 			varchar null ,
	createddate 					timestamp not null,
	lastmodifieddate 				timestamp not null
);

alter table bi_stage.stg_sfdc_opportunity add constraint pk_stg_sfdc_opportunity_owner_id primary key (id)

commit;
