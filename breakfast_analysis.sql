
---------------------------------------------------------------------
--2/27更新：
--Section1:按八种情况来给每个TC打上标签
drop table if exists tmp.ny2_bigbkf_trans_detail;
create table tmp.ny2_bigbkf_trans_detail as
with big_bkf as (
	select distinct transaction_guid,
			case when p_biz_date between 20190114 and 20190203 then '非Cny'
				when p_biz_date between 20190204 and 20190210 then 'Cny'
				when p_biz_date between 20190211 and 20190217 then '非Cny' end as week_tag
	from dw_kfc.flat_tld_detail a
	where a.item_code in ("4D3151","4D3152","650122","650123","650124","650136","650144","650145","650146","650159","650239","650240")
		and p_biz_date between 20190114 and 20190217	
	)
select a.transaction_guid, a.item_code,a.is_combo_flag,a.unit_sold,a.sell_name,a.product_category_name,b.week_tag
from dw_kfc.flat_tld_detail a left join big_bkf b on a.transaction_guid=b.transaction_guid
where b.transaction_guid is not Null and a.p_biz_date between 20190114 and 20190217
 and a.item_code not in ("AAAAAA","0", "FFFFFA") 
 --and a.product_category_name not in ('Others','Null','Non-food') and a.product_category_name is not Null
-- and a.is_combo_flag<2

--计算每笔交易在全餐、套餐、单点的unitsold

create table tmp.ny2_bigbkf_trans_combo_us as
select transaction_guid, week_tag, sum(quancan) as quancan, sum(other_combo) as other_combo
		, sum(dandian) as dandian
from (
	select transaction_guid, sell_name,product_category_name, is_combo_flag, unit_sold, week_tag,
			case when is_combo_flag=1 and sell_name like '%全餐%'  then unit_sold else 0 end as quancan,
			case when is_combo_flag=1 and sell_name not like '%全餐%' then unit_sold else 0 end as other_combo,
			case when is_combo_flag=0  then unit_sold else 0 end as dandian
		from tmp.ny2_bigbkf_trans_detail
		where product_category_name not in ('Others','Null','Non-food')
	group by transaction_guid, sell_name,product_category_name, is_combo_flag, unit_sold, week_tag
	) a
group by transaction_guid, week_tag

--每笔交易都打上了cross_type 
drop table if exists tmp.ny2_bigbkf_combo_cross;
create table tmp.ny2_bigbkf_combo_cross as
select transaction_guid, week_tag,quancan,other_combo,dandian, concat(tag_quancan,tag_other_combo,tag_dandian, tag_no_cross) as cross_type
from (select transaction_guid, week_tag,quancan,other_combo,dandian
		, case when quancan>=2 then 'cross全餐' else '' end as tag_quancan
		,case when other_combo>=1 then 'cross其他套餐' else '' end as tag_other_combo
		,case when dandian>=1 then 'cross单点' else '' end as tag_dandian
		,case when quancan =1 and other_combo=0 and dandian=0 then 'no_cross' else '' end as tag_no_cross
	from tmp.ny2_bigbkf_trans_combo_us) a

--查看各类cross type的比例
select cross_type, count(distinct transaction_guid)
from tmp.ny2_bigbkf_combo_cross
group by cross_type

--2.计算各种cross type的产品组合

select week_tag,class_cnt,cross_type, bkf_class_pattern, count(distinct transaction_guid) as tc_num
from(
	select transaction_guid, cross_type,week_tag,
	count(distinct item_name) as class_cnt,
	sort_array(collect_list(item_name))  as bkf_class_pattern
	from 
		(select a.transaction_guid,a.item_name,b.cross_type,a.week_tag
			from tmp.ny2_bigbkf_trans_detail a 
			left join tmp.ny2_bigbkf_combo_cross b on a.transaction_guid=b.transaction_guid) b
	group by transaction_guid, cross_type,week_tag
) a
group by class_cnt, bkf_class_pattern, cross_type,week_tag,cross_type


--0305补充正餐（非早餐时段）的partysize
select week_tag, occasion, sum(case when derived_party_size>0 then derived_party_size else 0 end) as total_ps, count(distinct transaction_guid) as total_tc,
		sum(case when derived_party_size>0 then derived_party_size else 0 end)/count(distinct transaction_guid) as partysize
from (select p_biz_date,derived_party_size, transaction_guid,daypart_id,transaction_amount,
		case when p_biz_date between 20190114 and 20190120 then 'CNY-3'
			when p_biz_date between 20190121 and 20190127 then 'CNY-2'
			when p_biz_date between 20190128 and 20190203 then 'CNY-1'
			when p_biz_date between 20190204 and 20190210 then 'CNY+0'
			when p_biz_date between 20190211 and 20190217 then 'CNY+1' end as week_tag,
		case when occasion_name in ("Drive through","Pre-order","Carry out","Selforder-out","Selforder-in","Dine in","Kiosk") then "线上"
			when occasion_name in ("Pro delivery CSC") then "线下" else "others" end as occasion
		from dw_kfc.flat_tld_header
		where p_biz_date between 20190114 and 20190217
		and  daypart_id !=2
		union all
		select p_biz_date,derived_party_size, transaction_guid,daypart_id,transaction_amount,
		case when p_biz_date between 20190114 and 20190120 then 'CNY-3'
			when p_biz_date between 20190121 and 20190127 then 'CNY-2'
			when p_biz_date between 20190128 and 20190203 then 'CNY-1'
			when p_biz_date between 20190204 and 20190210 then 'CNY+0'
			when p_biz_date between 20190211 and 20190217 then 'CNY+1' end as week_tag,
			"Total" as occasion
		from dw_kfc.flat_tld_header
		where p_biz_date between 20190114 and 20190217
		and  daypart_id != 2
		) a
group by week_tag, occasion





