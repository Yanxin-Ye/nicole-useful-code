# hello-world

# As a data analyst, I used Hive SQL everyday and there are some useful functions that I found interesting.

# 1. sort_array(collect_set(sell_name)) returns a set of sell_name of each transaction_id

select class_cnt,week_tag, bkf_class_pattern, count(distinct transaction_guid) as tc_num
from(
select transaction_guid, 
week_tag,
count(distinct sell_name) as class_cnt,
sort_array(collect_set(sell_name))  as bkf_class_pattern
from tmp.ny_bigbkf_trans_detail_sellname
group by transaction_guid,week_tag
) a
group by class_cnt, bkf_class_pattern,week_tag

# sort_array(collect_list(sell_name)) will return a collection, but duplicated sellnames will not be removed.

# 2. sort updated_time by usercode
select usercode,point,row_number() over(partition by usercode order by updatetime_format desc) as r

