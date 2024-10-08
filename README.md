# Accio: Bolt-on Query Federation

Rewrite a federated query into a list of queries with dependencies, each either run in a single data source or run locally. [[Technical Report](accio_technical_report.pdf)]

The following query:
```SQL
select
	l_orderkey,
	sum(l_extendedprice * (1 - l_discount)) as revenue,
	o_orderdate,
	o_shippriority
from
	db1.customer,
	db1.orders,
	db2.lineitem
where
	c_mktsegment = 'AUTOMOBILE'
	and c_custkey = o_custkey
	and l_orderkey = o_orderkey
	and o_orderdate < date '1995-03-10'
	and l_shipdate > date '1995-03-10'
group by
	l_orderkey,
	o_orderdate,
	o_shippriority
order by
  revenue desc,
  o_orderdate
```

will be rewritten into three queries: `q1`, `q2` and `q_local`:

```SQL
-- q1 (issue to db1)
SELECT "t"."o_orderkey",
       "t"."o_orderdate",
       "t"."o_shippriority"
FROM   (SELECT *
        FROM   "orders"
        WHERE  "o_orderdate" < DATE '1995-03-10') AS "t"
       inner join (SELECT *
                   FROM   "customer"
                   WHERE  "c_mktsegment" = 'AUTOMOBILE') AS "t0"
               ON "t"."o_custkey" = "t0"."c_custkey"

-- q2 (issue to db2)
SELECT "l_orderkey",
       "l_extendedprice" * ( 1 - "l_discount" ) AS "$f3"
FROM   "lineitem"
WHERE  "l_shipdate" > DATE '1995-03-10'

-- q_local (run locally, join results from db1 and db2)
SELECT "db2"."l_orderkey"     AS "L_ORDERKEY",
       Sum("db2"."$f3")       AS "REVENUE",
       "db1"."o_orderdate"    AS "O_ORDERDATE",
       "db1"."o_shippriority" AS "O_SHIPPRIORITY"
FROM   "db1"
       INNER JOIN "db2"
               ON "db1"."o_orderkey" = "db2"."l_orderkey"
GROUP  BY "db1"."o_orderdate",
          "db1"."o_shippriority",
          "db2"."l_orderkey"
ORDER  BY Sum("db2"."$f3") DESC,
          "o_orderdate" 
```

## Build Accio

* Install poetry: `pip install poetry`
* Install python dependencies: `poetry install`
* Install [just](https://github.com/casey/just)
* Build accio: `just python-setup` (JDK >=1.8 is required)

## Experiment Setup

* [Install docker](https://docs.docker.com/engine/install/)
* Network setup using `netem`:
  * clear: `tc qdisc del dev eth0 root netem`
  * show: `tc qdisc show dev eth0 | grep netem`
  * set to 10gbit/s: `sudo tc qdisc add dev eth0 root netem rate 10gbit`
  * set to 1gbit/s: `sudo tc qdisc add dev eth0 root netem rate 1gbit`
* Setup Postgres data source:
  * pull: `docker pull postgres`
  * start: `docker run --rm --name postgres --shm-size=128g -e POSTGRES_USER=postgres -e POSTGRES_DB=tpch -e POSTGRES_PASSWORD=postgres -d -p 5432:5432 -v ${POSTGRES_DATA_PATH}:/var/lib/postgresql/data postgres -c shared_buffers=128GB -c max_connections=500 -c max_worker_processes=16 -c max_parallel_workers_per_gather=4 -c max_parallel_workers=16 -c from_collapse_limit=16 -c join_collapse_limit=16`
  * initialize statistics: `just postgres_setup --url postgresql://${DB_USERNAME}:${DB_PASSWORD}@${DB_IP}:${DB_PORT}/${DB_NAME} --stats 100 --seed 42`

### Dataset

#### TPC-H

* Download TPC-H toolkit and compile:
```
git clone git@github.com:gregrahn/tpch-kit.git
cd tpch-kit/dbgen && make MACHINE=LINUX DATABASE=POSTGRESQL
```
* Generate tables with scale factor 10 (1, 50)
```
./dbgen -s 10 (1, 50)
```
* Create tables on each database
* Load data into each database

#### JOB

* Download IMDB csv files: `git clone git@github.com:danolivo/jo-bench.git`
* Create tables on each database
* Load data into each database

### Workload
The workload used can be found [here](./workload). `v0, v1, v2` represents three table distributions.

### DS engines and systems setup (optional)

#### DuckDB

We made some minor modification to duckdb's postgresscanner to enable query partitioning for arbitrary queries. To reproduce:
* Download: `git clone: git@github.com:wangxiaoying/postgresscanner.git`.
* Switch to `parallel_query` branch and prepare submodule:
```
git submodule init
git pull --recurse-submodules
```
* Build: `make`
* Add `duckdb = { path = "${YOUR_PATH}/postgresscanner/duckdb/tools/pythonpkg", develop = true }` to `pyproject.toml` and install the local version by `poetry lock && poetry install`.
* Add `DUCK_PG_EXTENSION=${YOUR_PATH}/postgresscanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension` to `.env` for benchmark.

#### Polars

We made a minor update to polars so that it can support more quries in our evaluation. To reproduce:
* Download: `git clone git@github.com:wangxiaoying/polars.git`
* Switch to `no_jk_check` branch and prepare dependencies follow [doc](https://docs.pola.rs/development/contributing/#installing-dependencies).
* Build: `cd py-polars && maturin build --release`
* Add `polars = { path = "${YOUR_PATH}/polars/target/wheels/polars-1.3.0-cp38-abi3-manylinux_2_35_x86_64.whl"}` to `pyproject.toml` and install the local version by `poetry lock && poetry install`.

#### chDB

Since chDB cannot pushdown arbitrary queries, we use connectorx to fetch data into arrow and then ingest into chDB. To reproduce:
* Download and build connectorx dependency:
   * Download: `git clone git@github.com:sfu-db/connector-x.git && cd connectorx && git checkout accio`
   * Follow the [documentation](https://sfu-db.github.io/connector-x/install.html#build-from-source-code) to build connectorx from source code
   * Build dynamic library: `just build-cpp-release`
     * Generated so file: `./target/release/libconnectorx_cpp.so`
* Download and build chDB:
  * Download: `git clone git@github.com:wangxiaoying/chdb.git && cd chdb && git checkout v1.3.0_cx`
  * Build wheel for chDB: `make wheel` (need llvm, some reference can be found [here](https://github.com/wangxiaoying/chdb/blob/19bf2f5084b3a754d81c4da49e760762f9d2c4b4/.github/workflows/build_wheels.yml#L116) and [here](https://clickhouse.com/docs/en/development/developer-instruction#cloning-a-repository-to-your-development-machine))
     * The wheel file can be found under `./dist/`
* Add `chdb = {path = "${YOUR_PATH}/chdb/dist/chdb-1.3.0-cp311-cp311-linux_x86_64.whl"}` to `pyproject.toml` and install the local version by `poetry lock && poetry install`.

#### Trino
* Go to trino path: `cd benchmark/trino`
* Pull image: `docker pull trinodb/trino`
* Update the `discovery.uri` under `./etc*/config.properties`
* Update the `connection-url` under `./etc*/catalog/*.properties` to corresponding addresses of each service
* Run trino: `just start-trino`

#### Apache Wayang

* Download a fork with benchmark code added: 
    * `git clone git@github.com:wangxiaoying/incubator-wayang.git`
    * `cd incubator-wayang`
    * `git switch 240806`
* Build wayang following the [instruction](https://github.com/apache/incubator-wayang?tab=readme-ov-file#building):
    * Setup JAVA environment (Java 11)
    * Setup `SPARK_HOME` and `HADOOP_HOME`, as well as `spark.version` and `hadoop.version` from `pom.xml`
    * Build: `./mvnw clean install -pl :wayang-assembly -Pdistribution -DskipTests `
    * Unpack: `mkdir install && cp wayang-assembly/target/apache-wayang-assembly-0.7.1-SNAPSHOT-incubating-dist.tar.gz install && cd install && tar -xvf wayang-assembly-0.7.1-SNAPSHOT-dist.tar.gz`
* Run: `./install/wayang-0.7.1/bin/wayang-submit -Xms128g -Xmx128g org.apache.wayang.apps.tpch.TpcH exp\(123\) spark,postgres,java file://${CONFIG_FILE_PATH}.properties ${QUERY}`
* Confiuration example
```
wayang.postgres.jdbc.url = jdbc:postgresql://${HOST}:${PORT}/${DB}
wayang.postgres.jdbc.user = ${USER}
wayang.postgres.jdbc.password = ${PASSWORD}

spark.master = local[32]
spark.driver.memory = 128G
spark.executor.memory = 128G
spark.executor.cores = 32
wayang.giraph.hdfs.tempdir = file:///tmp/wayang/

spark.rdd.compress = true
spark.log.level = INFO
```

## Running Experiments

Commands needed can be found in the [Justfile](./Justfile). 

Here are some examples of running accio with spark on JOB(v0) benchmark under 10Gbps bandwidth:

```bash

### spark native (without accio)
just bench-s spark native -c -c ./benchmark/config/job_spark/10gbit db1 db2 -w workload/job2_v0

### with accio
just bench-s spark accio -c ./benchmark/config/job_spark/10gbit -s benefit db1 db2 -w workload/job2_v0

### pushdown evaluation (with partition disabled)
# test iterative approach
just bench-s spark accio -c ./benchmark/config/job_spark/10gbit_nopart -s benefit db1 db2 -w workload/job2_v0

# test twophase approach 
just bench-s spark accio -c ./benchmark/config/job_spark/10gbit_nopart -s goo db1 db2 -w workload/job2_v0

# test exhaustive approach
just bench-s spark accio -c ./benchmark/config/job_spark/10gbit_nopart -s dpsize db1 db2 -w workload/job2_v0

# test pushall baseline
just bench-s spark accio -c ./benchmark/config/job_spark/10gbit_nopart -s pushdown db1 db2 -w workload/job2_v0

# test nopush baseline
just bench-s spark accio -c ./benchmark/config/job_spark/10gbit_nopush_nopart -s benefit db1 db2 -w workload/job2_v0

### partition evaluation (with pushdown disabled)

# enable partition
just bench-s spark accio -c ./benchmark/config/job_spark/10gbit_nopush -s benefit db1 db2 -w workload/job2_v0

# disable partition
just bench-s spark accio -c ./benchmark/config/job_spark/10gbit_nopush_nopart -s benefit db1 db2 -w workload/job2_v0

```

Use command `just bench-s spark --help` for more detailed parameter explanations.

Note that for the join pushdown evaluation, please checkout to `ablation` branch first to use a single postgres configured in `**/ce.json` for cardinality estimation.

Other engines, workloads and network condition are similar by changing the corresponding parameters in the command. 
More details can be found from the benchmark scripts located [here](./benchmark). 

#### Configuration
Specifically, we create a json file for each database as configuration, here is an example:
```json
{
  "driver": "org.postgresql.Driver",
  "url": "jdbc:postgresql://${DB_IP}:${DB_PORT}/${DB_NAME}",
  "username": "postgres",
  "password": "postgres",
  "costParams": {
    "join": 2.0,
    "trans": 10.0
  },
  "cardEstType": "postgres",
  "partitionType": "postgres",
  "partition": {
        "max_partition_per_table": 16
  },
  "dialect": "postgres",
  "disableOps": []
}
```
It contains the following information:
* Authentication and connection info: driver, ulr, username, password
* Customized modules for the data source:
  * costParams: the parameters used in the default cost estimator, by default, all parameters are 1.0
  * cardEstType: the cardinality estimator used for the data source, interface and existing implementations can be found [here](./rewriter/src/main/java/ai/dataprep/accio/stats/)
  * partitionType: the query partitioner used for the data source, interface and existing implementations can be found [here](./rewriter/src/main/java/ai/dataprep/accio/partition/)
  * partition: the parameters used in the partitioner
  * dialect: the dialect used to generate queries to be issued to the data source, we implement a few dialects based on calcite's dialect, which can be cound [here](./rewriter/src/main/java/ai/dataprep/accio/sql/)
  * disableOps: the operators that the data source cannot support. For example, if its value is `["join"]`, we won't push down join to the source

The configurations we use for benchmark can be found [here](./benchmark/config).
