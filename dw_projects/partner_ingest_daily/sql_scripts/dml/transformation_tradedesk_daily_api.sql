--> Version 1


-- d_agency

insert into bi_prod.d_agency
	(
		agency_name_raw,
		dw_insert_timestamp,
		dw_update_timestamp
	)
	select	
	    distinct
		lower(stg.agency_name),
		now(),
		now()
	from
	bi_stage.stg_tradedesk_daily_api stg
	left join bi_prod.d_agency tgt on lower(agency_name_raw)=lower(stg.agency_name)
	where tgt.agency_name_raw is null;

-- d_brand

insert into bi_prod.d_brand
	(
		brand_name_raw,
		dw_insert_timestamp,
		dw_update_timestamp
	)
	select	
	    distinct
		lower(stg.advertiser_name),
		now(),
		now()
	from
	bi_stage.stg_tradedesk_daily_api stg
	left join bi_prod.d_brand tgt on lower(brand_name_raw)=lower(stg.advertiser_name)
	where tgt.brand_name_raw is null;

-- d_partner_campaign

insert into bi_prod.d_partner_campaign
	(
		partner_campaign_name,
		dw_insert_timestamp
	)
	select	
	    distinct
		lower(stg.campaign_name),
		now()
	from
	bi_stage.stg_tradedesk_daily_api stg
	left join bi_prod.d_partner_campaign tgt on lower(partner_campaign_name)=lower(campaign_name)
	where tgt.partner_campaign_name is null;

drop table if exists tmp;

-- Derive design_key based on multiple columns for non standard segment
-- Derive standard_segment_key for standard segments
-- Derive columns for joining to the lk tables

create local temporary table tmp as
(
	select 
		tmp.*,
		'tradedesk' 	partner_name,
		case 
			when standard_segment_key > 0 then standard_segment_name
			when price_cpm = .5 then 'demo'	
		 	else  'non demo'
		end 			rate_card_segment_name,
		'tradedesk'		platform,
		'audience' 		product_name,
		case
			when standard_segment_key > 0 then 'standard'
			else 'custom'
		end 			discount_category,
		case
			when standard_segment_key > 0 then standard_inventory_source
			else coalesce(lower(array_to_string(des.data_sources,',','*')),'unknown')
		end inventory_source
	from
	(
		select
			stg.*,	
			case
				when std.standard_segment_key is not null then -999
				else coalesce(tgt.design_key,unq.design_key,des.design_key,-999)
			end design_key,
			coalesce(std.standard_segment_key,-999) standard_segment_key,
			coalesce(std.segment_name,'non demo') standard_segment_name,
			coalesce(lower(std.inventory_source),'unknown') standard_inventory_source
		from bi_stage.stg_tradedesk_daily_api stg
		left join bi_prod.lk_unique_designs unq on lower(stg.data_segment_name) = unq.design_name
		left join 
		(
			select 
			    targeting_code,
			    design_key 
			from bi_prod.d_front_design_targeting_codes
			where targeting_code in (select targeting_code from bi_prod.d_front_design_targeting_codes group by targeting_code having count(1)=1)
		) tgt on tgt.targeting_code = stg.segment_id
		left join bi_prod.d_front_designs des on des.design_id = stg.segment_id
		left join bi_prod.lk_standard_segments std on std.targeting_code = stg.segment_id and std.partner_name='tradedesk' and product_name='audience' and stg.date between std.start_date and std.end_date
	) tmp join 	bi_prod.d_front_designs des on des.design_key = tmp.design_key
); 

-- Derive revenue metrics 
drop table if exists stg;

create local temporary table stg as
(
	select
		tmp1.*,
		case 												-- brand discount superseeds agency discount
			when discount_brand > 0 then discount_brand
			else discount_agency
		end discount,
		(													-- rev_share (gross_revenue-discount) * rev_share %
			gross_revenue -		
			case
				when discount_brand > 0 then discount_brand
				else discount_agency
			end
		) * (revenue_share * .01) rev_share
	from
	(
		select
			tmp.*,
			case 											
				when brand_name ilike '%Data Alliance%' then data_cost_in_usd
				else (rc.price/1000) * tmp.impressions_count
			end::decimal(10,2) gross_revenue, 
			coalesce
			(
		        case
		            when brand_name ilike '%Data Alliance%' then data_cost_in_usd
		            else (rc.price/1000) * tmp.impressions_count
		        end::decimal(10,2) * (dis.discount * .01)
		        ,0
		    ) discount_agency, 
		    coalesce
			(
		        case
		            when brand_name ilike '%Data Alliance%' then data_cost_in_usd
		            else (rc.price/1000) * tmp.impressions_count
		        end::decimal(10,2) * (dis1.discount * .01)
		        ,0
		    ) discount_brand,
		    rs.revenue_share
		from
		tmp
		left join bi_prod.lk_rate_card rc on rc.partner_name = tmp.partner_name and rc.rate_card_segment_name = tmp.rate_card_segment_name and tmp.date between rc.start_date and rc.end_date
		left join bi_prod.lk_discounts dis on dis.product_name = tmp.product_name and dis.discount_category = tmp.discount_category and dis.partner_type = 'agency' and tmp.agency_name ilike '%'||dis.partner_name ||'%' and tmp.date between dis.start_date and dis.end_date 
		left join bi_prod.lk_discounts dis1 on dis1.product_name = tmp.product_name and dis1.discount_category = tmp.discount_category and dis1.partner_type = 'brand' and tmp.advertiser_name ilike '%'||dis1.partner_name ||'%' and tmp.date between dis1.start_date and dis1.end_date
	    left join bi_prod.lk_rev_share rs on rs.partner_name = tmp.partner_name and rs.platform = tmp.platform and rs.product_name = tmp.product_name and tmp.date between rs.start_date and rs.end_date
	) tmp1
);

-- d_inventory_source (Derived from lk_standard_segments)

insert into bi_prod.d_inventory_source
	(
		inventory_source_raw,
		inventory_source_actual,
		inventory_source_type,
		dw_insert_timestamp,
		dw_update_timestamp
	)
	select	
	    distinct
		inventory_source,
		inventory_source,
		case 
			when position(',' in inventory_source) > 0 then 'multi'
			else 'single'
		end inventory_source_type,
		now(),
		now()
	from
	stg
	left join bi_prod.d_inventory_source tgt on inventory_source_raw = inventory_source
	where 1=1
	and tgt.inventory_source_raw is null;

insert into bi_prod.f_tf_revenue_daily_base
(
	date_key,
	partner_source_key,
	brand_key,
	agency_key,
	dsp_key,
	dmp_key,
	publisher_key,
	country_key,
	design_key,
	product_key,
	revenue_type_key,
	partner_campaign_key,
	exchange_key,
	inventory_source_key,
	sfdc_opportunity_key,
	ad_type_key,
	ad_format_key,
	integration_type_key,
	standard_segment_key,
	is_branded,
	is_private,
	impressions,
	rev_from_partner,
	gross_revenue,
	discount,
	rev_share,
	net_revenue,
	dw_insert_timestamp,
	dw_update_timestamp
)
select
	to_char(date,'yyyymmdd')::int date_key,
	2 partner_source_key,
	coalesce(brd.brand_key,-999) brand_key,
	coalesce(agc.agency_key,-999) agency_key,
	1 dsp_key,
	-999 dmp_key,
	-999 publisher_key,
	-999 country_key,
	stg.design_key design_key,
	1 product_key, 												-- audience product
	case 
		when stg.brand_name ilike '%Data Alliance%' then 2
		else 1
	end revenue_type_key, 										-- cpm
	coalesce(partner_campaign_key,-999) partner_campaign_key,
	-999 exchange_key,
	coalesce(invsrc.inventory_source_key,-999) inventory_source_key,
	coalesce(hist.opportunity_key,-999) sfdc_opportunity_key, 									-- column will be removed
	-999 ad_type_key,
	-999 ad_format_key,
	1 integration_type_key,
	standard_segment_key, 									-- on prem
	case 
		when stg.brand_name ilike '%Data Alliance%' then 0
		else 1
	end is_branded, 											-- confirm the value
	-999 is_private, 											-- confirm the value
	sum(stg.impressions_count) impressions,
	sum(data_cost_in_usd) rev_from_partner,
	sum(gross_revenue), 
	sum(discount), 
	sum(rev_share),
	sum((gross_revenue-discount-rev_share)) net_revenue,
	now() ,
	now()
from
stg
left join bi_prod.d_brand brd on lower(stg.advertiser_name) = brd.brand_name_raw
left join bi_prod.d_agency agc on lower(stg.agency_name) = agc.agency_name_raw
left join bi_prod.d_partner_campaign pc on lower(stg.campaign_name) = pc.partner_campaign_name
left join bi_prod.d_inventory_source invsrc on invsrc.inventory_source_raw = stg.inventory_source
left join bi_prod.d_front_design_opportunity_hist hist on hist.design_key = stg.design_key and stg.date between start_date and end_date
group  by 
	to_char(date,'yyyymmdd')::int ,
	coalesce(brd.brand_key,-999) ,
	coalesce(agc.agency_key,-999) ,
	stg.design_key ,
	case 
		when stg.brand_name ilike '%Data Alliance%' then 2
		else 1
	end,
	coalesce(partner_campaign_key,-999) ,
	coalesce(invsrc.inventory_source_key,-999),
	coalesce(hist.opportunity_key,-999) , 									-- column will be removed
	standard_segment_key,
	case 
		when stg.brand_name ilike '%Data Alliance%' then 0
		else 1
	end;  											-- confirm the value
