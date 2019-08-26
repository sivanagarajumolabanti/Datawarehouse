drop table if exists bi_stage.stg_sfdc_user cascade;


create table if not exists bi_stage.stg_sfdc_user  
( 
    id            		varchar not null,
    firstname         	varchar not null,
    lastname          	varchar not null,
    createddate       	timestamp not null,
    lastmodifieddate 	timestamp not null 
);

alter table if exists bi_stage.stg_sfdc_user add constraint pk_stg_sfdc_user_id primary key (id);

commit;
