drop table if exists bi_prod.d_sfdc_account cascade;

create table if not exists bi_prod.d_sfdc_account  
( 
    account_key             serial not null,
    account_id              varchar not null,
    name                    varchar not null,
    billing_city            varchar null,
    billing_state           varchar null,
    billing_country         varchar null,
    user_key                int null,
    owner_id                varchar null,
    size                    varchar null,
    account_manager         varchar null,
    brand_account           varchar null,
    account_balance         numeric(10,2) null default 0,
    account_overdue_balance numeric(10,2) null default 0,
    days_overdue            int null,
    netsuite_id             varchar null,
    unbilled_orders         int null,
    ad_tech_partner         bool null,
    region                  varchar null,
    ad_tech_spm             varchar null,
    industry                varchar,
    created_date            timestamp not null,
    last_modified_date      timestamp not null,
    dw_insert_timestamp     timestamp with time zone  not null,
    dw_update_timestamp     timestamp with time zone  not null 
 );

alter table bi_prod.d_sfdc_account add constraint pk_d_sfdc_account_account_key primary key (account_key);

alter table bi_prod.d_sfdc_account add constraint unique_d_sfdc_account_account_id unique (account_id);

--alter table bi_prod.d_sfdc_account add constraint fk_user_key foreign key(user_key) references bi_prod.d_sfdc_user(user_key);

commit;

-- Insert unknown record and reset sequesnce to 1

insert into bi_prod.d_sfdc_account
values
(
    -999,
    'unknown',
    'unknown',
    'unknown',
    'unknown',
    'unknown',
    -999,
    'unknown',
    'unknown',
    'unknown',
    'unknown',
    0.0,
    0.0,
    -999,
    'unknown',
    -999,
    false,
    'unknown',
    'unknown',
    'unknown',
    now(),
    now(),
    now(),
    now()   
);

alter sequence bi_prod.d_sfdc_account_account_key_seq restart with 1;

