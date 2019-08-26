drop table if exists bi_stage.stg_sfdc_opportunity_line_item  cascade;

create table if not exists bi_stage.stg_sfdc_opportunity_line_item  
( 
	id 					varchar not null,
	opportunityid		varchar not null,
	productcode			varchar,
	currencyisocode		varchar,
	netsuite_itemid__c  varchar,
	use_case__c			varchar,
	revenue_type__c		varchar,
	start_date__c		timestamp,
	end_date__c			timestamp,
	createddate			timestamp	not null,
	lastmodifieddate	timestamp	not null
);

alter table bi_stage.stg_sfdc_opportunity_line_item add constraint stg_sfdc_opportunity_line_item_id primary key (id);

commit;