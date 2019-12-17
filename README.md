## Getting Started
benchmark tool in Go for KV database, support leveldb, rocksdb, tikv

### build

 - os: centos7.3 
 - go version: 1.13 

```
git clone https://github.com/mdsf22/kvctl
cd kvctl
make
```

Notice:
+ To use RocksDB, you must follow [INSTALL](https://github.com/facebook/rocksdb/blob/master/INSTALL.md) to install RocksDB at first.

## Usage
```
Usage:
  kvctl [command]

Available Commands:
  del         del data of key
  get         get value of key from db
  getn        get num of data from db, default key is start with test_(test_0, test_1 ...)
  help        Help about any command
  put         put data to db
  putn        put data to db, default key is start with test_(test_0, test_1 ...)
  test        1: init total(MB) data; 2: insert num(MB) data; 3: query data in step2; 4: query data not in story; 5: update data in step2; 6: delete data in step2

Flags:
      --dbname string               name of db (default "tikv")
  -h, --help                        help for kvctl
      --initData int                0: do not init data, 1: init data
      --keyPre string               start char of key (default "test")
  -p, --prop stringArray            Specify a property value with name=value
  -c, --property_file stringArray   Spefify a property file
      --threads int                 thread num (default 10)
      --valLen int                  length of value (default 1024)
      --valType int                 0: repeat data, 1: random data
```

## example

 - insert
```
./kvctl put name dzg --dbname leveldb
```

 - get
```
./kvctl get name --dbname leveldb
``` 

- insert 100000 record

```
./kvctl putn 100000 --dbname leveldb --threads 1
```
```
Elapsed(s): 1.1Avg(us): 9, Qps: 89048, Count: 100000, Min(us): 2, Max(us): 67871, kB/s: 89048, 
sum:  Elapsed(s): 1.1Avg(us): 9, Qps: 89048, Count: 100000, Min(us): 2, Max(us): 67871, kB/s: 89048,
```

- insert 100000 record with 10 threads
```
./kvctl putn 100000 --dbname rocksdb --threads 10
```
```
Elapsed(s): 1.0Avg(us): 84, Qps: 10332, Count: 10000, Min(us): 2, Max(us): 44315, kB/s: 10332, 
Elapsed(s): 1.4Avg(us): 125, Qps: 7079, Count: 10000, Min(us): 2, Max(us): 57811, kB/s: 7079, 
Elapsed(s): 1.5Avg(us): 122, Qps: 6768, Count: 10000, Min(us): 2, Max(us): 44534, kB/s: 6768, 
Elapsed(s): 1.5Avg(us): 130, Qps: 6702, Count: 10000, Min(us): 2, Max(us): 104945, kB/s: 6702, 
Elapsed(s): 1.5Avg(us): 126, Qps: 6699, Count: 10000, Min(us): 2, Max(us): 50341, kB/s: 6699, 
Elapsed(s): 1.5Avg(us): 124, Qps: 6879, Count: 10000, Min(us): 2, Max(us): 44446, kB/s: 6879, 
Elapsed(s): 1.5Avg(us): 131, Qps: 6712, Count: 10000, Min(us): 2, Max(us): 60112, kB/s: 6712, 
Elapsed(s): 1.5Avg(us): 134, Qps: 6676, Count: 10000, Min(us): 2, Max(us): 57645, kB/s: 6676, 
Elapsed(s): 1.5Avg(us): 133, Qps: 6828, Count: 10000, Min(us): 2, Max(us): 63596, kB/s: 6828, 
Elapsed(s): 1.5Avg(us): 135, Qps: 6804, Count: 10000, Min(us): 2, Max(us): 60190, kB/s: 6804, 
sum:  Elapsed(s): 1.5Avg(us): 124, Qps: 71479, Count: 100000, Min(us): 2, Max(us): 104945, kB/s: 71479,
```

- insert 100000 record with spefify a property file
```
./kvctl putn 100000 --dbname rocksdb -c conf/rocksdb.conf --threads 10
```

## Supported Database

- LevelDB
- TiKV
- RocksDB 