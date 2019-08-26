drop table if exists bi_prod.d_sfdc_opportunity_line_item  cascade;

create table if not exists bi_prod.d_sfdc_opportunity_line_item  
( 
    opport_line_item_key    serial  not null,
    opport_line_item_id     varchar not null,
    opportunity_key         int     not null,
    opportunity_id          varchar not null,
    product_code            varchar null,
    currency_iso_code       varchar null,
    net_suite_item_id       varchar null,
    use_case                varchar null,
    revenue_type            varchar null,
    start_date              timestamp null,
    end_date                timestamp null,
    created_date            timestamp not null,
    last_modified_date      timestamp not null,
    dw_insert_timestamp     timestamp not null,
    dw_update_timestamp     timestamp not null 
);

alter table if exists bi_prod.d_sfdc_opportunity_line_item add constraint pk_d_sfdc_opportunity_line_item_opport_line_item_key primary key (opport_line_item_key);

alter table if exists bi_prod.d_sfdc_opportunity_line_item add constraint unique_d_sfdc_opportunity_line_item_oppor_line_item_id unique (opport_line_item_id);
 
--alter table if exists bi_prod.d_sfdc_opportunity_line_item add constraint fk_opportunity_key foreign key(opportunity_key) references bi_prod.d_sfdc_opportunity(opportunity_key);

commit;

insert into bi_prod.d_sfdc_opportunity_line_item values
(
    -999,
    'unknown',
    -999,
    'unknown',
    'unknown',
    'unknown',
    'unknown',
    'unknown',
    'unknown',
    '1980-01-01',
    '1980-01-01',
    '1980-01-01',
    '1980-01-01',
    now(),
    now()
);
alter sequence bi_prod.d_sfdc_opportunity_line_item_opport_line_item_key_seq restart with 1;
