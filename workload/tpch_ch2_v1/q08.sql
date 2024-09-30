select
	o_year,
	sum(case
		when nation = 'BRAZIL' then volume
		else 0
	end) / sum(volume) as mkt_share
from ( select extract(year from o_orderdate) as o_year,
			l_extendedprice * (1 - l_discount) as volume,
			n2.n_name as nation
		from db1.part,
			db1.supplier,
			db2.lineitem,
			db2.orders,
			db1.customer,
			db1.nation n1,
			db1.nation n2,
			db1.region
		where p_partkey = l_partkey
			and s_suppkey = l_suppkey
			and l_orderkey = o_orderkey
			and o_custkey = c_custkey
			and c_nationkey = n1.n_nationkey
			and n1.n_regionkey = r_regionkey
			and r_name = 'AMERICA'
			and s_nationkey = n2.n_nationkey
			and o_orderdate BETWEEN toDate('1995-01-01') AND toDate('1996-12-31')
			and p_type = 'ECONOMY ANODIZED STEEL'
	) as all_nations
group by o_year
order by o_year
