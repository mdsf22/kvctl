package rocksdb

import (
	"fmt"
	"github.com/mdsf22/kvctl/db"

	"github.com/magiconair/properties"
)

const (
	rocksdbDir = "rocksdb.dir"
	// DBOptions
	rocksdbAllowConcurrentMemtableWrites   = "rocksdb.allow_concurrent_memtable_writes"
	rocsdbAllowMmapReads                   = "rocksdb.allow_mmap_reads"
	rocksdbAllowMmapWrites                 = "rocksdb.allow_mmap_writes"
	rocksdbArenaBlockSize                  = "rocksdb.arena_block_size"
	rocksdbDBWriteBufferSize               = "rocksdb.db_write_buffer_size"
	rocksdbHardPendingCompactionBytesLimit = "rocksdb.hard_pending_compaction_bytes_limit"
	rocksdbLevel0FileNumCompactionTrigger  = "rocksdb.level0_file_num_compaction_trigger"
	rocksdbLevel0SlowdownWritesTrigger     = "rocksdb.level0_slowdown_writes_trigger"
	rocksdbLevel0StopWritesTrigger         = "rocksdb.level0_stop_writes_trigger"
	rocksdbMaxBytesForLevelBase            = "rocksdb.max_bytes_for_level_base"
	rocksdbMaxBytesForLevelMultiplier      = "rocksdb.max_bytes_for_level_multiplier"
	rocksdbMaxTotalWalSize                 = "rocksdb.max_total_wal_size"
	rocksdbMemtableHugePageSize            = "rocksdb.memtable_huge_page_size"
	rocksdbNumLevels                       = "rocksdb.num_levels"
	rocksdbUseDirectReads                  = "rocksdb.use_direct_reads"
	rocksdbUseFsync                        = "rocksdb.use_fsync"
	rocksdbWriteBufferSize                 = "rocksdb.write_buffer_size"
	rocksdbMaxWriteBufferNumber            = "rocksdb.max_write_buffer_number"
	rocksdbMaxBackgroundFlushes            = "rocksdb.max_background_flushes"
	rocksdbCompression                     = "rocksdb.compression"
	rocksdbThreadsCompaction               = "rocksdb.threads_flush_compaction"
	// TableOptions/BlockBasedTable
	rocksdbBlockSize                        = "rocksdb.block_size"
	rocksdbBlockSizeDeviation               = "rocksdb.block_size_deviation"
	rocksdbCacheIndexAndFilterBlocks        = "rocksdb.cache_index_and_filter_blocks"
	rocksdbNoBlockCache                     = "rocksdb.no_block_cache"
	rocksdbPinL0FilterAndIndexBlocksInCache = "rocksdb.pin_l0_filter_and_index_blocks_in_cache"
	rocksdbWholeKeyFiltering                = "rocksdb.whole_key_filtering"
	rocksdbBlockRestartInterval             = "rocksdb.block_restart_interval"
	rocksdbFilterPolicy                     = "rocksdb.filter_policy"
	rocksdbIndexType                        = "rocksdb.index_type"
)

type rocksdbCreator struct {
}

func (c rocksdbCreator) Create(p *properties.Properties) db.KVDB {
	dataDir := p.GetString(rocksdbDir, "path/")
	keypre := p.GetString("keypre", "test")
	threads := p.GetInt("threads", 10)
	vallen := p.GetInt("vallen", 1024)

	fmt.Println("===============================")
	fmt.Println("db: level")
	fmt.Println("dataDir: ", dataDir)
	fmt.Println("threads: ", threads)
	fmt.Println("keypre: ", keypre)
	fmt.Println("===============================")
	return NewRocksDb(p, dataDir, threads, keypre, vallen)
}

func init() {
	db.RegisterDBCreator("rocksdb", rocksdbCreator{})
}
