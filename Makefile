ROCKSDB_CHECK := $(shell echo "int main() { return 0; }" | gcc -lrocksdb -x c++ -o /dev/null - 2>/dev/null; echo $$?)

ifeq ($(ROCKSDB_CHECK), 0)
	TAGS += rocksdb
	ROCKSDB_INCLUDE = "-I/usr/local/include"
	CGO_CFLAGS += CGO_CFLAGS=$(ROCKSDB_INCLUDE)
	ROCKSDB_LIB = "-L/usr/local/lib -lrocksdb -lstdc++ -lm -lz -lbz2 -lsnappy -llz4 /usr/local/lib/libzstd.a"
	CGO_LDFLAGS += CGO_LDFLAGS=$(ROCKSDB_LIB)
endif

default: build
build:
ifeq ($(TAGS),)
	go build -o kvctl client.go
else
	$(CGO_CFLAGS) $(CGO_LDFLAGS) go build -o kvctl client.go rocksdb.go
endif