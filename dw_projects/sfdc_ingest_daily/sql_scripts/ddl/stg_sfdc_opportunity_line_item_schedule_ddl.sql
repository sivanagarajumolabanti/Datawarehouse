drop table if exists bi_stage.stg_sfdc_opportunity_line_item_schedule  cascade;

create table if not exists bi_stage.stg_sfdc_opportunity_line_item_schedule  
( 
	id varchar not null, 	 						
	opportunitylineitemid varchar not null,
	revenue numeric(10,2),
	scheduledate timestamp,
	currencyisocode varchar,
	createddate timestamp,
	lastmodifieddate timestamp
);

alter table bi_stage.stg_sfdc_opportunity_line_item_schedule add constraint stg_sfdc_opportunity_line_item_schedule_id primary key (id);

commit;

	