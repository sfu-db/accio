select c_custkey,
	c_name,
	sum(l_extendedprice * (1 - l_discount)) as revenue,
	c_acctbal,
	n_name,
	c_address,
	c_phone,
	c_comment
from db2.customer,
	db1.orders,
	db1.lineitem,
	db2.nation
where c_custkey = o_custkey
	and l_orderkey = o_orderkey
	and o_orderdate >= toDate('1993-10-01')
	and o_orderdate < toDate('1993-10-01') + INTERVAL 3 MONTH
	and l_returnflag = 'R'
	and c_nationkey = n_nationkey
group by c_custkey, c_name,
	c_acctbal, c_phone, n_name,
	c_address, c_comment
order by revenue desc
