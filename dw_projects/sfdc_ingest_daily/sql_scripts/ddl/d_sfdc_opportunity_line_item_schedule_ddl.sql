drop table if exists bi_prod.d_sfdc_opportunity_line_item_schedule  cascade;

create table bi_prod.d_sfdc_opportunity_line_item_schedule  
( 
    opport_line_item_sched_key serial not null,
    opport_line_item_sched_id  varchar not null,
    opport_line_item_key          int not null,
    opport_line_item_id           varchar not null,
    revenue                       numeric(10,2) not null default 0,
    schedule_date                 date null,
    currency_iso_code             varchar null,
    created_date                  timestamp not null,
    last_modified_date            timestamp not null,
    dw_insert_timestamp           timestamp not null,
    dw_update_timestamp           timestamp not null
);

alter table bi_prod.d_sfdc_opportunity_line_item_schedule add constraint pk_d_sfdc_opportunity_line_item_schedule_opport_line_item_sched_key primary key (opport_line_item_sched_key);
alter table bi_prod.d_sfdc_opportunity_line_item_schedule add constraint unique_d_sfdc_opportunity_line_item_schedule_opport_line_item_sched_id unique (opport_line_item_sched_id);
--alter table if exists  bi_prod.d_sfdc_opportunity_line_item_schedule add constraint fk_opport_line_item_key foreign key(opport_line_item_key) references bi_prod.d_sfdc_opportunity_line_item(opport_line_item_key);

commit;

insert into bi_prod.d_sfdc_opportunity_line_item_schedule values
(
    -999,
    'unknown',
    -999,
    'unknown',
    0.0,
    '1980-01-01',
    'unknown',
    '1980-01-01',
    '1980-01-01',
    now(),
    now()
);
alter sequence bi_prod.d_sfdc_opportunity_line_item_sch_opport_line_item_sched_key_seq restart with 1;
