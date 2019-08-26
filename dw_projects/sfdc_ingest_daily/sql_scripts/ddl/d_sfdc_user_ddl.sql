drop table if exists bi_prod.d_sfdc_user cascade;

create table if not exists bi_prod.d_sfdc_user  
( 
    user_key           	serial not null,
    user_id            	varchar not null,
    first_name         	varchar not null,
    last_name          	varchar not null,
    created_date       	timestamp not null,
    last_modified_date 	timestamp not null,
    dw_insert_timestamp	timestamp not null,
    dw_update_timestamp	timestamp not null 
);

alter table if exists bi_prod.d_sfdc_user add constraint pk_d_sfdc_user_user_key primary key (user_key);

alter table if exists bi_prod.d_sfdc_user add constraint unique_d_sfdc_user_user_id unique (user_id);

commit;

-- Insert unknown record and reset sequesnce to 1

insert into bi_prod.d_sfdc_user
values
(
  -999,  
  'unknown',
  'unknown',
  'unknown',
  now(),
  now(),
  now(),
  now()
);

alter sequence bi_prod.d_sfdc_user_user_key_seq restart with 1;