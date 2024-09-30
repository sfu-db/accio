set dotenv-load := true

build:
    cd rewriter && mvn package -Dmaven.test.skip=true

python-setup: build
    cp rewriter/target/accio-rewriter-1.0-SNAPSHOT-jar-with-dependencies.jar accio/

python-test +opts="": python-setup 
    poetry run pytest accio/tests -v -s {{opts}}

python-test-s +opts="":
    poetry run pytest accio/tests -v -s {{opts}}

postgres-setup +opts="":
    poetry run python workload/prepare/postgres.py {{opts}}

postgres-time +opts="":
    poetry run python benchmark/time_postgres.py {{opts}}

wkld-gen wkld:
    cd workload && bash gen_{{wkld}}.sh

cmd:
    poetry run python

bench-time +opts="": python-setup
    #!/bin/bash
    export PYTHONPATH=$PWD
    poetry run python benchmark/time_rewrite.py {{opts}}

bench-time-s +opts="":
    #!/bin/bash
    export PYTHONPATH=$PWD
    poetry run python benchmark/time_rewrite.py {{opts}}

bench engine method +opts="": python-setup
    #!/bin/bash
    export PYTHONPATH=$PWD
    poetry run python benchmark/bench_{{engine}}.py {{method}} {{opts}}

bench-s engine method +opts="":
    #!/bin/bash
    export PYTHONPATH=$PWD
    poetry run python benchmark/bench_{{engine}}.py {{method}} {{opts}}

start-trino etc="etc":
    sudo docker run --rm -p 8080:8080 -v ./benchmark/trino/{{etc}}:/etc/trino --name trino -d trinodb/trino
