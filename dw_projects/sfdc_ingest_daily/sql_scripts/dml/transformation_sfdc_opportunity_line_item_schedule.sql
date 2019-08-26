insert into bi_backups.d_sfdc_opportunity_line_item_schedule_deleted
select *,now() from bi_prod.d_sfdc_opportunity_line_item_schedule a
where 1=1
and opport_line_item_sched_key>0
and not exists (select 1 from bi_stage.stg_sfdc_opportunity_line_item_schedule b where a.opport_line_item_sched_id=b.id );

delete from bi_prod.d_sfdc_opportunity_line_item_schedule a
where 1=1
and opport_line_item_sched_key>0
and not exists (select 1 from bi_stage.stg_sfdc_opportunity_line_item_schedule b where a.opport_line_item_sched_id=b.id );

update bi_prod.d_sfdc_opportunity_line_item_schedule tgt
set
    opport_line_item_key = coalesce(itm.opport_line_item_key,-999),
    opport_line_item_id  = opportunitylineitemid,
    revenue              = stg.revenue,
    schedule_date        = scheduledate,
    currency_iso_code    = currencyisocode,
    created_date         = createddate,
    last_modified_date   = lastmodifieddate,
    dw_update_timestamp  = now()   
from
bi_stage.stg_sfdc_opportunity_line_item_schedule stg
left join bi_prod.d_sfdc_opportunity_line_item itm on itm.opport_line_item_id=stg.opportunitylineitemid
where 1=1
and stg.id=tgt.opport_line_item_id
and
(
    tgt.opport_line_item_key <> coalesce(itm.opport_line_item_key,-999) or
    tgt.opport_line_item_id <> opportunitylineitemid or
    tgt.revenue <> stg.revenue or
    tgt.schedule_date <> scheduledate or
    tgt.currency_iso_code <> currencyisocode 
);

insert into bi_prod.d_sfdc_opportunity_line_item_schedule
(
    opport_line_item_sched_id,
    opport_line_item_key,
    opport_line_item_id,
    revenue,
    schedule_date,
    currency_iso_code,
    created_date,
    last_modified_date,
    dw_insert_timestamp,
    dw_update_timestamp
)
select
    id,
    coalesce(itm.opport_line_item_key,-999),
    opportunitylineitemid,
    stg.revenue,
    scheduledate,
    currencyisocode,
    createddate,
    lastmodifieddate,
    now(),
    now()
from
bi_stage.stg_sfdc_opportunity_line_item_schedule stg
left join bi_prod.d_sfdc_opportunity_line_item_schedule tgt on stg.id=tgt.opport_line_item_sched_id
left join bi_prod.d_sfdc_opportunity_line_item itm on itm.opport_line_item_id=stg.opportunitylineitemid
where tgt.opport_line_item_id is null;


-- insert into bi_prod.d_sfdc_opportunity_line_item_schedule
-- (
--     opport_line_item_sched_id,
--     opport_line_item_key,
--     opport_line_item_id,
--     revenue,
--     schedule_date,
--     currency_iso_code,
--     created_date,
--     last_modified_date,
--     dw_insert_timestamp,
--     dw_update_timestamp
-- )
-- select
-- 	id,
-- 	coalesce(itm.opport_line_item_key,-999),
-- 	opportunitylineitemid,
-- 	revenue,
-- 	scheduledate,
-- 	currencyisocode,
-- 	createddate,
-- 	lastmodifieddate,
-- 	now(),
-- 	now()
-- from
-- bi_stage.stg_sfdc_opportunity_line_item_schedule stg
-- left join bi_prod.d_sfdc_opportunity_line_item itm on itm.opport_line_item_id=stg.opportunitylineitemid
-- on conflict (opport_line_item_sched_id)
-- do update
-- set 
--     opport_line_item_key = excluded.opport_line_item_key,
--     opport_line_item_id = excluded.opport_line_item_id,
--     revenue = excluded.revenue,
--     schedule_date = excluded.schedule_date,
--     currency_iso_code = excluded.currency_iso_code,
--     created_date = excluded.created_date,
--     last_modified_date = excluded.last_modified_date,
--     dw_update_timestamp	= now();

--truncate table bi_prod.d_sfdc_opportunity_line_item_schedule;