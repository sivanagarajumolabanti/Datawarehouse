drop table if exists bi_prod.d_sfdc_opportunity cascade; 

create table if not exists bi_prod.d_sfdc_opportunity
(
	opportunity_key          	serial not null,
    opportunity_id           	varchar(36) not null,
    account_key              	int not null,
    account_id               	varchar(36) not null,
    record_type_id           	varchar null,
    opportunity_name         	varchar null,
    stage_name               	varchar null,
    amount                   	numeric(10,2) null,
    probability              	varchar null,
    close_date               	date null,
    lead_source              	varchar null,
    currency_iso_code        	varchar null,
    campaign_id              	varchar null,
    user_key                 	int null,
    owner_id                 	varchar null,
    lost_reason              	varchar null,
    net_total_value          	numeric(10,2) null default 0,
    monthly_recurring_revenue	numeric(10,2) null default 0,
    annual_contract_value    	numeric(10,2) null default 0,
    contract_start_date      	timestamp null,
    contract_end_date        	timestamp null,
    net_suite_contract_id    	varchar null,
    net_suite_id             	varchar null,
    auto_renew_days          	int null,
    auto_renew_date          	timestamp null,
    order_type               	varchar null,
    contract_term            	int null,
    ad_tech_partners         	varchar null,
    campaign_start_date      	timestamp null,
    campaign_end_date        	timestamp null,
    lost_reason_picklist     	varchar null,
    direct_to_brand          	varchar null,
    opportunity_revenue_type 	varchar null,
    region                   	varchar null,
    opportunity_won_by       	varchar null,
    upsell_cross_device      	varchar null,
    created_at               	timestamp not null,
    last_modified_at         	timestamp not null,
    dw_insert_timestamp      	timestamp not null,
    dw_update_timestamp      	timestamp not null 
);

alter table if exists bi_prod.d_sfdc_opportunity add constraint pk_d_sfdc_opportunity_opportunity_key primary key (opportunity_key);

alter table if exists  bi_prod.d_sfdc_opportunity add constraint unique_sfdc_opportunity_id unique (opportunity_id);

--alter table if exists  bi_prod.d_sfdc_opportunity add constraint fk_user_key foreign key(user_key) references bi_prod.d_sfdc_user(user_key);

--alter table if exists  bi_prod.d_sfdc_opportunity add constraint fk_account_key foreign key(account_key) references bi_prod.d_sfdc_account(account_key);

commit;

insert into bi_prod.d_sfdc_opportunity
values
(
    -999,
    'unknown',
    -999,
    'unknown',
    'unknown',
    'unknown',
    'unknown',
    0.0,
    'unknown',
    '1980-01-01',
    'unknown',
    'unknown',
    'unknown',
    -999,
    'unknown',
    'unknown',
    0.0,
    0.0,
    0.0,
    '1980-01-01',
    '1980-01-01',
    'unknown',
    'unknown',
    0,
    '1980-01-01',
    'unknown',
    0,
    'unknown',
    '1980-01-01',
    '1980-01-01',
    'unknown',
    'unknown',
    'unknown',
    'unknown',
    'unknown',
    'unknown',    
    '1980-01-01',
    '1980-01-01',
    now(),
    now()
);

alter sequence bi_prod.d_sfdc_opportunity_opportunity_key_seq restart with 1;





