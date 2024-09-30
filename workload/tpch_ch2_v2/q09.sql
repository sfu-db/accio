select
	nation,
	o_year,
	sum(amount) as sum_profit
from ( select n_name as nation,
			toYear(o_orderdate) AS o_year,
			l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
		from db1.part,
			db2.supplier,
			db1.lineitem,
			db1.partsupp,
			db1.orders,
			db2.nation
		where s_suppkey = l_suppkey
			and ps_suppkey = l_suppkey
			and ps_partkey = l_partkey
			and p_partkey = l_partkey
			and o_orderkey = l_orderkey
			and s_nationkey = n_nationkey
			and p_name like '%green%'
	) as profit
group by nation, o_year
order by nation, o_year desc
