insert into bi_backups.d_sfdc_user_deleted
select *,now() from bi_prod.d_sfdc_user a
where 1=1
and user_key>0
and not exists (select 1 from bi_stage.stg_sfdc_user b where a.user_id=b.id );

delete from bi_prod.d_sfdc_user a
where 1=1
and user_key>0
and not exists (select 1 from bi_stage.stg_sfdc_user b where a.user_id=b.id );

update bi_prod.d_sfdc_user tgt
	set 
	    first_name 			= stg.firstname,
	    last_name 			= stg.lastname,
	    created_date 		= stg.createddate,
	    last_modified_date 	= stg.lastmodifieddate,
	    dw_update_timestamp = now()
from
bi_stage.stg_sfdc_user stg
where 1=1
and stg.id=tgt.user_id
and (stg.firstname <> tgt.first_name or stg.lastname <> tgt.last_name);

insert into bi_prod.d_sfdc_user
(
	user_id,
    first_name,
    last_name,
    created_date,
    last_modified_date,
    dw_insert_timestamp,
    dw_update_timestamp
)
select
	id,
	firstname,
	lastname,
	createddate,
	lastmodifieddate,
	now(),
	now()
from
bi_stage.stg_sfdc_user stg
left join bi_prod.d_sfdc_user tgt on stg.id=tgt.user_id
where tgt.user_id is null;


-- insert into bi_prod.d_sfdc_user
-- (
-- 	user_id,
--     first_name,
--     last_name,
--     created_date,
--     last_modified_date,
--     dw_insert_timestamp,
--     dw_update_timestamp
-- )
-- select
-- 	id,
-- 	firstname,
-- 	lastname,
-- 	createddate,
-- 	lastmodifieddate,
-- 	now(),
-- 	now()
-- from
-- bi_stage.stg_sfdc_user stg
-- left join bi_prod.d_sfdc_user prd on stg.id=prd.user_id
-- on conflict(user_id)
-- do update
-- set
-- 	user_id=excluded.user_id,
-- 	first_name=excluded.first_name,
-- 	last_name=excluded.last_name,
-- 	created_date=excluded.created_date,
-- 	last_modified_date=excluded.last_modified_date,
-- 	dw_update_timestamp=now();

--truncate table bi_stage.stg_sfdc_user;	
