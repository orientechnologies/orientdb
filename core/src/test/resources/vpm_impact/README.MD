Testing the impact of VPM with a single and a distributed (but single instance) setup.

**Step-by-step**

1) Go to the ODB repository, clean and start the distribution
```
mvn clean install -pl distribution -DskipTests
```
for single instance
```
./distribution/target/orientdb-community-3.2.0-SNAPSHOT.dir/orientdb-community-3.2.0-SNAPSHOT/bin/server.sh
```
OR distributed
```
./distribution/target/orientdb-community-3.2.0-SNAPSHOT.dir/orientdb-community-3.2.0-SNAPSHOT/bin/dserver.sh
```

2) Get the YCSB benchmark runner (incl. the new ODB driver for v3.1.x)
```
git clone https://github.com/brianfrankcooper/YCSB.git
cd YCSB
```

3) Create an output directory for the benchmark results
```
mkdir -p orientdb/outputs/orientdb
```
and run workloads for single:
```
./bin/ycsb load orientdb -s -P workloads/workloada -p recordcount=1000000
./bin/ycsb run orientdb -s -P workloads/workloada -p operationcount=500000 -p batchsize=1000 > orientdb/outputs/orientdb/outputRun_workloada.txt
```
```
./bin/ycsb load orientdb -s -P workloads/workloadb -p recordcount=1000000
./bin/ycsb run orientdb -s -P workloads/workloadb -p operationcount=500000 -p batchsize=1000 > orientdb/outputs/orientdb/outputRun_workloadb.txt
```
```
./bin/ycsb load orientdb -s -P workloads/workloadc -p recordcount=1000000
./bin/ycsb run orientdb -s -P workloads/workloadc -p operationcount=500000 -p batchsize=1000 > orientdb/outputs/orientdb/outputRun_workloadc.txt
```
```
./bin/ycsb load orientdb -s -P workloads/workloadd -p recordcount=1000000
./bin/ycsb run orientdb -s -P workloads/workloadd -p operationcount=500000 -p batchsize=1000 > orientdb/outputs/orientdb/outputRun_workloadd.txt
```
```
./bin/ycsb load orientdb -s -P workloads/workloade -p recordcount=1000000
./bin/ycsb run orientdb -s -P workloads/workloade -p operationcount=500000 -p batchsize=1000 > orientdb/outputs/orientdb/outputRun_workloade.txt
```
```
./bin/ycsb load orientdb -s -P workloads/workloadf -p recordcount=1000000
./bin/ycsb run orientdb -s -P workloads/workloadf -p operationcount=500000 -p batchsize=1000 > orientdb/outputs/orientdb/outputRun_workloadf.txt
```
OR distributed
```
./bin/ycsb load orientdb -s -P workloads/workloada -p recordcount=1000000
./bin/ycsb run orientdb -s -P workloads/workloada -p operationcount=500000 -p batchsize=1000 > orientdb/outputs/orientdb-dist/outputRun_workloada.txt
```
```
./bin/ycsb load orientdb -s -P workloads/workloadb -p recordcount=1000000
./bin/ycsb run orientdb -s -P workloads/workloadb -p operationcount=500000 -p batchsize=1000 > orientdb/outputs/orientdb-dist/outputRun_workloadb.txt
```
```
./bin/ycsb load orientdb -s -P workloads/workloadc -p recordcount=1000000
./bin/ycsb run orientdb -s -P workloads/workloadc -p operationcount=500000 -p batchsize=1000 > orientdb/outputs/orientdb-dist/outputRun_workloadc.txt
```
```
./bin/ycsb load orientdb -s -P workloads/workloadd -p recordcount=1000000
./bin/ycsb run orientdb -s -P workloads/workloadd -p operationcount=500000 -p batchsize=1000 > orientdb/outputs/orientdb-dist/outputRun_workloadd.txt
```
```
./bin/ycsb load orientdb -s -P workloads/workloade -p recordcount=1000000
./bin/ycsb run orientdb -s -P workloads/workloade -p operationcount=500000 -p batchsize=1000 > orientdb/outputs/orientdb-dist/outputRun_workloade.txt
```
```
./bin/ycsb load orientdb -s -P workloads/workloadf -p recordcount=1000000
./bin/ycsb run orientdb -s -P workloads/workloadf -p operationcount=500000 -p batchsize=1000 > orientdb/outputs/orientdb-dist/outputRun_workloadf.txt
```
> the database might need cleaning between the runs.

4) Generate the plots and save them. The `dat` files will be created in `plots/data` and PNG images in `plots/images`
```
cd orientdb/plots
./tool.sh "plot" "save" ../outputs/orientdb/
```

5) Optionally, comparisions can be plotted using the `.plt` files in this directory.