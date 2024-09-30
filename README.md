# Accio: Bolt-on Query Federation

[Technical Report](accio_technical_report.pdf)

### Apache Wayang

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


### DuckDB

With query partitioning:
* Download: `git clone: git@github.com:wangxiaoying/postgresscanner.git`.
* Switch to `parallel_query` branch and prepare submodule:
```
git submodule init
git pull --recurse-submodules
```
* Build: `make`
* Add `duckdb = { path = "${YOUR_PATH}/postgresscanner/duckdb/tools/pythonpkg", develop = true }` to `pyproject.toml` and install the local version by `poetry lock && poetry install`.
* Add `DUCK_PG_EXTENSION=${YOUR_PATH}/postgresscanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension` to `.env` for benchmark.

### Polars

* Download: `git clone git@github.com:wangxiaoying/polars.git`
* Switch to `no_jk_check` branch and prepare dependencies follow [doc](https://docs.pola.rs/development/contributing/#installing-dependencies).
* Build: `cd py-polars && maturin build --release`
* Add `polars = { path = "${YOUR_PATH}/polars/target/wheels/polars-1.3.0-cp38-abi3-manylinux_2_35_x86_64.whl"}` to `pyproject.toml` and install the local version by `poetry lock && poetry install`.

### chDB / ClickHouse

* Download and build connectorx dependency:
* Download and build chDB
* Download and build ClickHouse