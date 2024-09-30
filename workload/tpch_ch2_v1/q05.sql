select
	n_name,
	sum(l_extendedprice * (1 - l_discount)) as revenue
from
	db1.customer,
	db2.orders,
	db2.lineitem,
	db1.supplier,
	db1.nation,
	db1.region
where
	c_custkey = o_custkey
	and l_orderkey = o_orderkey
	and l_suppkey = s_suppkey
	and c_nationkey = s_nationkey
	and s_nationkey = n_nationkey
	and n_regionkey = r_regionkey
	and r_name = 'ASIA'
	and o_orderdate >= toDate('1994-01-01')
	and o_orderdate < toDate('1994-01-01') + interval 1 year
group by
	n_name
order by
	revenue desc
