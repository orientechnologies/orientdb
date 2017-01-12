/*
*
*  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
*  *
*  *  Licensed under the Apache License, Version 2.0 (the "License");
*  *  you may not use this file except in compliance with the License.
*  *  You may obtain a copy of the License at
*  *
*  *       http://www.apache.org/licenses/LICENSE-2.0
*  *
*  *  Unless required by applicable law or agreed to in writing, software
*  *  distributed under the License is distributed on an "AS IS" BASIS,
*  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
*  *  See the License for the specific language governing permissions and
*  *  limitations under the License.
*  *
*  * For more information: http://www.orientechnologies.com
*
*/
package com.orientechnologies.orient.core.config;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.common.util.OApi;
import com.orientechnologies.orient.core.OConstants;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.cache.ORecordCacheWeakRefs;
import com.orientechnologies.orient.core.engine.local.OEngineLocalPaginated;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.metadata.OMetadataDefault;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerBinary;

import java.io.PrintStream;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.ConsoleHandler;
import java.util.logging.FileHandler;
import java.util.logging.Level;

/**
 * Keeps all configuration settings. At startup assigns the configuration values by reading system properties.
 *
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 */
public enum OGlobalConfiguration {
  // ENVIRONMENT
  ENVIRONMENT_DUMP_CFG_AT_STARTUP("environment.dumpCfgAtStartup", "Dumps the configuration during application startup",
      Boolean.class, Boolean.FALSE),

  ENVIRONMENT_CONCURRENT("environment.concurrent",
      "Specifies if running in multi-thread environment. Setting this to false turns off the internal lock management",
      Boolean.class, Boolean.TRUE),

  ENVIRONMENT_LOCK_MANAGER_CONCURRENCY_LEVEL("environment.lockManager.concurrency.level", "Concurrency level of lock manager",
      Integer.class, Runtime.getRuntime().availableProcessors() << 3, false),

  ENVIRONMENT_ALLOW_JVM_SHUTDOWN("environment.allowJVMShutdown", "Allows the shutdown of the JVM, if needed/requested",
      Boolean.class, true, true),

  // SCRIPT
  SCRIPT_POOL("script.pool.maxSize", "Maximum number of instances in the pool of script engines", Integer.class, 20),

  // MEMORY
  MEMORY_USE_UNSAFE("memory.useUnsafe", "Indicates whether Unsafe will be used, if it is present", Boolean.class, true),

  MEMORY_CHUNK_SIZE("memory.chunk.size", "Size of single memory chunk (in bytes) which will be preallocated by OrientDB",
      Integer.class, Integer.MAX_VALUE),

  DIRECT_MEMORY_SAFE_MODE("memory.directMemory.safeMode",
      "Indicates whether to perform a range check before each direct memory update. It is true by default, "
          + "but usually it can be safely set to false. It should only be to true after dramatic changes have been made in the storage structures",
      Boolean.class, true),

  DIRECT_MEMORY_TRACK_MODE("memory.directMemory.trackMode",
      "Activates the direct memory pool [leak detector](Leak-Detector.md). This detector causes a large overhead and should be used for debugging "
          + "purposes only. It's also a good idea to pass the "
          + "-Djava.util.logging.manager=com.orientechnologies.common.log.OLogManager$DebugLogManager switch to the JVM, "
          + "if you use this mode, this will enable the logging from JVM shutdown hooks.", Boolean.class, false),

  DIRECT_MEMORY_ONLY_ALIGNED_ACCESS("memory.directMemory.onlyAlignedMemoryAccess",
      "Some architectures do not allow unaligned memory access or may suffer from speed degradation. For such platforms, this flag should be set to true",
      Boolean.class, true),

  JVM_GC_DELAY_FOR_OPTIMIZE("jvm.gc.delayForOptimize",
      "Minimal amount of time (in seconds), since the last System.gc(), when called after tree optimization", Long.class, 600),

  // STORAGE
  /**
   * Limit of amount of files which may be open simultaneously
   */
  OPEN_FILES_LIMIT("storage.openFiles.limit", "Limit of amount of files which may be open simultaneously", Integer.class, 512),

  /**
   * Amount of cached locks is used for component lock in atomic operation to avoid constant creation of new lock instances, default
   * value is 10000.
   */
  COMPONENTS_LOCK_CACHE("storage.componentsLock.cache",
      "Amount of cached locks is used for component lock to avoid constant creation of new lock instances", Integer.class, 10000),

  DISK_CACHE_PINNED_PAGES("storage.diskCache.pinnedPages", "Maximum amount of pinned pages which may be contained in cache,"
      + " if this percent is reached next pages will be left in unpinned state. You can not set value more than 50", Integer.class,
      20, false),

  DISK_CACHE_SIZE("storage.diskCache.bufferSize", "Size of disk buffer in megabytes, disk size may be changed at runtime, "
      + "but if does not enough to contain all pinned pages exception will be thrown", Integer.class, 4 * 1024,
      new OConfigurationChangeCallback() {

        @Override
        public void change(Object currentValue, Object newValue) {
          final OEngineLocalPaginated engineLocalPaginated = (OEngineLocalPaginated) Orient.instance()
              .getEngineIfRunning(OEngineLocalPaginated.NAME);
          if (engineLocalPaginated != null)
            engineLocalPaginated.changeCacheSize(((Integer) (newValue)) * 1024L * 1024L);
        }
      }),

  DISK_WRITE_CACHE_PART("storage.diskCache.writeCachePart", "Percentage of disk cache, which is used as write cache", Integer.class,
      15),

  DISK_WRITE_CACHE_PAGE_TTL("storage.diskCache.writeCachePageTTL",
      "Max time until a page will be flushed from write cache (in seconds)", Long.class, 24 * 60 * 60),

  DISK_WRITE_CACHE_PAGE_FLUSH_INTERVAL("storage.diskCache.writeCachePageFlushInterval",
      "Interval between flushing of pages from write cache (in ms)", Integer.class, 25),

  DISK_WRITE_CACHE_FLUSH_WRITE_INACTIVITY_INTERVAL("storage.diskCache.writeCacheFlushInactivityInterval",
      "Interval between 2 writes to the disk cache,"
          + " if writes are done with an interval more than provided, all files will be fsynced before the next write,"
          + " which allows a data restore after a server crash (in ms)", Long.class, 60 * 1000),

  DISK_WRITE_CACHE_FLUSH_LOCK_TIMEOUT("storage.diskCache.writeCacheFlushLockTimeout",
      "Maximum amount of time the write cache will wait before a page flushes (in ms, -1 to disable)", Integer.class, -1),

  @Deprecated DISC_CACHE_FREE_SPACE_CHECK_INTERVAL("storage.diskCache.diskFreeSpaceCheckInterval",
      "The interval (in seconds), after which the storage periodically "
          + "checks whether the amount of free disk space is enough to work in write mode", Integer.class, 5),

  /**
   * The interval (how many new pages should be added before free space will be checked), after which the storage periodically
   * checks whether the amount of free disk space is enough to work in write mode.
   */
  DISC_CACHE_FREE_SPACE_CHECK_INTERVAL_IN_PAGES("storage.diskCache.diskFreeSpaceCheckIntervalInPages",
      "The interval (how many new pages should be added before free space will be checked), after which the storage periodically "
          + "checks whether the amount of free disk space is enough to work in write mode", Integer.class, 2048),

  /**
   * Keep disk cache state between moment when storage is closed and moment when it is opened again. <code>true</code> by default.
   */
  STORAGE_KEEP_DISK_CACHE_STATE("storage.diskCache.keepState",
      "Keep disk cache state between moment when storage is closed and moment when it is opened again. true by default",
      Boolean.class, true),

  STORAGE_CONFIGURATION_SYNC_ON_UPDATE("storage.configuration.syncOnUpdate",
      "Indicates a force sync should be performed for each update on the storage configuration", Boolean.class, true),

  STORAGE_COMPRESSION_METHOD("storage.compressionMethod", "Record compression method used in storage"
      + " Possible values : gzip, nothing, snappy, snappy-native. Default is 'nothing' that means no compression", String.class,
      "nothing"),

  STORAGE_ENCRYPTION_METHOD("storage.encryptionMethod",
      "Record encryption method used in storage" + " Possible values : 'aes' and 'des'. Default is 'nothing' for no encryption",
      String.class, "nothing"),

  STORAGE_ENCRYPTION_KEY("storage.encryptionKey", "Contains the storage encryption key. This setting is hidden", String.class, null,
      false, true),

  STORAGE_MAKE_FULL_CHECKPOINT_AFTER_CREATE("storage.makeFullCheckpointAfterCreate",
      "Indicates whether a full checkpoint should be performed, if storage was created", Boolean.class, false),

  STORAGE_MAKE_FULL_CHECKPOINT_AFTER_OPEN("storage.makeFullCheckpointAfterOpen",
      "Indicates whether a full checkpoint should be performed, if storage was opened. It is needed so fuzzy checkpoints can work properly",
      Boolean.class, true),

  STORAGE_MAKE_FULL_CHECKPOINT_AFTER_CLUSTER_CREATE("storage.makeFullCheckpointAfterClusterCreate",
      "Indicates whether a full checkpoint should be performed, if storage was opened", Boolean.class, true),

  STORAGE_TRACK_CHANGED_RECORDS_IN_WAL("storage.trackChangedRecordsInWAL",
      "If this flag is set metadata which contains rids of changed records is added at the end of each atomic operation",
      Boolean.class, false),

  USE_WAL("storage.useWAL", "Whether WAL should be used in paginated storage", Boolean.class, true),

  WAL_SYNC_ON_PAGE_FLUSH("storage.wal.syncOnPageFlush", "Indicates whether a force sync should be performed during WAL page flush",
      Boolean.class, true),

  WAL_CACHE_SIZE("storage.wal.cacheSize",
      "Maximum size of WAL cache (in amount of WAL pages, each page is 64k) If set to 0, caching will be disabled", Integer.class,
      3000),

  WAL_FILE_AUTOCLOSE_INTERVAL("storage.wal.fileAutoCloseInterval",
      "Interval in seconds after which WAL file will be closed if there is no "
          + "any IO operations on this file (in seconds), default value is 10", Integer.class, 10, false),

  WAL_MAX_SEGMENT_SIZE("storage.wal.maxSegmentSize", "Maximum size of single WAL segment (in megabytes)", Integer.class, 128),

  WAL_MAX_SIZE("storage.wal.maxSize", "Maximum size of WAL on disk (in megabytes)", Integer.class, 4096),

  WAL_COMMIT_TIMEOUT("storage.wal.commitTimeout", "Maximum interval between WAL commits (in ms.)", Integer.class, 1000),

  WAL_SHUTDOWN_TIMEOUT("storage.wal.shutdownTimeout", "Maximum wait interval between events, when the background flush thread"
      + "receives a shutdown command and when the background flush will be stopped (in ms.)", Integer.class, 10000),

  WAL_FUZZY_CHECKPOINT_INTERVAL("storage.wal.fuzzyCheckpointInterval", "Interval between fuzzy checkpoints (in seconds)",
      Integer.class, 300),

  WAL_REPORT_AFTER_OPERATIONS_DURING_RESTORE("storage.wal.reportAfterOperationsDuringRestore",
      "Amount of processed log operations, after which status of data restore procedure will be printed (0 or a negative value, disables the logging)",
      Integer.class, 10000),

  WAL_RESTORE_BATCH_SIZE("storage.wal.restore.batchSize",
      "Amount of WAL records, which are read at once in a single batch during a restore procedure", Integer.class, 1000),

  @Deprecated WAL_READ_CACHE_SIZE("storage.wal.readCacheSize", "Size of WAL read cache in amount of pages", Integer.class, 1000),

  WAL_FUZZY_CHECKPOINT_SHUTDOWN_TIMEOUT("storage.wal.fuzzyCheckpointShutdownWait",
      "The amount of time the DB should wait until it shuts down (in seconds)", Integer.class, 60 * 10),

  WAL_FULL_CHECKPOINT_SHUTDOWN_TIMEOUT("storage.wal.fullCheckpointShutdownTimeout",
      "The amount of time the DB will wait, until a checkpoint is finished, during a DB shutdown (in seconds)", Integer.class,
      60 * 10),

  WAL_LOCATION("storage.wal.path", "Path to the WAL file on the disk. By default, it is placed in the DB directory, but"
      + " it is highly recommended to use a separate disk to store log operations", String.class, null),

  DISK_CACHE_PAGE_SIZE("storage.diskCache.pageSize", "Size of page of disk buffer (in kilobytes). !!! NEVER CHANGE THIS VALUE !!!",
      Integer.class, 64),

  DISK_CACHE_FREE_SPACE_LIMIT("storage.diskCache.diskFreeSpaceLimit", "Minimum amount of space on disk, which, when exceeded, "
      + "will cause the database to switch to read-only mode (in megabytes)", Long.class,
      2 * WAL_MAX_SEGMENT_SIZE.getValueAsLong()),

  PAGINATED_STORAGE_LOWEST_FREELIST_BOUNDARY("storage.lowestFreeListBound",
      "The least amount of free space (in kb) in a page, which is tracked in paginated storage", Integer.class, 16),

  STORAGE_LOCK_TIMEOUT("storage.lockTimeout", "Maximum amount of time (in ms) to lock the storage", Integer.class, 0),

  STORAGE_RECORD_LOCK_TIMEOUT("storage.record.lockTimeout", "Maximum of time (in ms) to lock a shared record", Integer.class, 2000),

  STORAGE_USE_TOMBSTONES("storage.useTombstones",
      "When a record is deleted, the space in the cluster will not be freed, but rather tombstoned", Boolean.class, false),

  // RECORDS
  RECORD_DOWNSIZING_ENABLED("record.downsizing.enabled",
      "On updates, if the record size is lower than before, this reduces the space taken accordingly. "
          + "If enabled this could increase defragmentation, but it reduces the used disk space", Boolean.class, true),

  // DATABASE
  OBJECT_SAVE_ONLY_DIRTY("object.saveOnlyDirty", "Object Database only! It saves objects bound to dirty records", Boolean.class,
      false, true),

  // DATABASE
  DB_POOL_MIN("db.pool.min", "Default database pool minimum size", Integer.class, 1),

  DB_POOL_MAX("db.pool.max", "Default database pool maximum size", Integer.class, 100),

  DB_POOL_IDLE_TIMEOUT("db.pool.idleTimeout", "Timeout for checking for free databases in the pool", Integer.class, 0),

  DB_POOL_IDLE_CHECK_DELAY("db.pool.idleCheckDelay", "Delay time on checking for idle databases", Integer.class, 0),

  DB_MVCC_THROWFAST("db.mvcc.throwfast",
      "Use fast-thrown exceptions for MVCC OConcurrentModificationExceptions. No context information will be available. "
          + "Set to true, when these exceptions are thrown, but the details are not necessary", Boolean.class, false, true),

  DB_VALIDATION("db.validation", "Enables or disables validation of records", Boolean.class, true, true),

  // SETTINGS OF NON-TRANSACTIONAL MODE
  NON_TX_RECORD_UPDATE_SYNCH("nonTX.recordUpdate.synch",
      "Executes a sync against the file-system for every record operation. This slows down record updates, "
          + "but guarantees reliability on unreliable drives", Boolean.class, Boolean.FALSE),

  NON_TX_CLUSTERS_SYNC_IMMEDIATELY("nonTX.clusters.sync.immediately",
      "List of clusters to sync immediately after update (separated by commas). Can be useful for a manual index", String.class,
      OMetadataDefault.CLUSTER_MANUAL_INDEX_NAME),

  // TRANSACTIONS

  TX_TRACK_ATOMIC_OPERATIONS("tx.trackAtomicOperations",
      "This setting is used only for debug purposes. It creates a stack trace of methods, when an atomic operation is started",
      Boolean.class, false),

  TX_PAGE_CACHE_SIZE("tx.pageCacheSize",
      "The size of a per-transaction page cache in pages, 12 by default, 0 to disable the cache.", Integer.class, 12),

  // INDEX
  INDEX_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD("index.embeddedToSbtreeBonsaiThreshold",
      "Amount of values, after which the index implementation will use an sbtree as a values container. Set to -1, to disable and force using an sbtree",
      Integer.class, 40, true),

  INDEX_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD("index.sbtreeBonsaiToEmbeddedThreshold",
      "Amount of values, after which index implementation will use an embedded values container (disabled by default)",
      Integer.class, -1, true),

  HASH_TABLE_SPLIT_BUCKETS_BUFFER_LENGTH("hashTable.slitBucketsBuffer.length", "Length of buffer (in pages), where buckets "
      + "that were split, but not flushed to the disk, are kept. This buffer is used to minimize random IO overhead", Integer.class,
      1500),

  INDEX_SYNCHRONOUS_AUTO_REBUILD("index.auto.synchronousAutoRebuild",
      "Synchronous execution of auto rebuilding of indexes, in case of a DB crash", Boolean.class, Boolean.TRUE),

  INDEX_AUTO_LAZY_UPDATES("index.auto.lazyUpdates",
      "Configure the TreeMaps for automatic indexes, as buffered or not. -1 means buffered until tx.commit() or db.close() are called",
      Integer.class, 10000),

  INDEX_FLUSH_AFTER_CREATE("index.flushAfterCreate", "Flush storage buffer after index creation", Boolean.class, true),

  INDEX_MANUAL_LAZY_UPDATES("index.manual.lazyUpdates",
      "Configure the TreeMaps for manual indexes as buffered or not. -1 means buffered until tx.commit() or db.close() are called",
      Integer.class, 1),

  INDEX_DURABLE_IN_NON_TX_MODE("index.durableInNonTxMode",
      "Indicates whether index implementation for plocal storage will be durable in non-Tx mode (true by default)", Boolean.class,
      true),

  /**
   * @see OIndexDefinition#isNullValuesIgnored()
   * @since 2.2
   */
  INDEX_IGNORE_NULL_VALUES_DEFAULT("index.ignoreNullValuesDefault",
      "Controls whether null values will be ignored by default " + "by newly created indexes or not (false by default)",
      Boolean.class, false),

  INDEX_TX_MODE("index.txMode",
      "Indicates the index durability level in TX mode. Can be ROLLBACK_ONLY or FULL (ROLLBACK_ONLY by default)", String.class,
      "FULL"),

  INDEX_CURSOR_PREFETCH_SIZE("index.cursor.prefetchSize", "Default prefetch size of index cursor", Integer.class, 500000),

  // SBTREE
  SBTREE_MAX_DEPTH("sbtree.maxDepth",
      "Maximum depth of sbtree, which will be traversed during key look up until it will be treated as broken (64 by default)",
      Integer.class, 64),

  SBTREE_MAX_KEY_SIZE("sbtree.maxKeySize", "Maximum size of a key, which can be put in the SBTree in bytes (10240 by default)",
      Integer.class, 10240),

  SBTREE_MAX_EMBEDDED_VALUE_SIZE("sbtree.maxEmbeddedValueSize",
      "Maximum size of value which can be put in an SBTree without creation link to a standalone page in bytes (40960 by default)",
      Integer.class, 40960),

  SBTREEBONSAI_BUCKET_SIZE("sbtreebonsai.bucketSize",
      "Size of bucket in OSBTreeBonsai (in kB). Contract: bucketSize < storagePageSize, storagePageSize % bucketSize == 0",
      Integer.class, 2),

  SBTREEBONSAI_LINKBAG_CACHE_SIZE("sbtreebonsai.linkBagCache.size",
      "Amount of LINKBAG collections to be cached, to avoid constant reloading of data", Integer.class, 100000),

  SBTREEBONSAI_LINKBAG_CACHE_EVICTION_SIZE("sbtreebonsai.linkBagCache.evictionSize",
      "The number of cached LINKBAG collections, which will be removed, when the cache limit is reached", Integer.class, 1000),

  SBTREEBOSAI_FREE_SPACE_REUSE_TRIGGER("sbtreebonsai.freeSpaceReuseTrigger",
      "How much free space should be in an sbtreebonsai file, before it will be reused during the next allocation", Float.class,
      0.5),

  // RIDBAG
  RID_BAG_EMBEDDED_DEFAULT_SIZE("ridBag.embeddedDefaultSize", "Size of embedded RidBag array, when created (empty)", Integer.class,
      4),

  RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD("ridBag.embeddedToSbtreeBonsaiThreshold",
      "Amount of values after which a LINKBAG implementation will use sbtree as values container. Set to -1 to always use an sbtree",
      Integer.class, 40, true),

  RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD("ridBag.sbtreeBonsaiToEmbeddedToThreshold",
      "Amount of values, after which a LINKBAG implementation will use an embedded values container (disabled by default)",
      Integer.class, -1, true),

  // COLLECTIONS
  PREFER_SBTREE_SET("collections.preferSBTreeSet", "This configuration setting is experimental", Boolean.class, false),

  // FILE
  @Deprecated
  TRACK_FILE_CLOSE("file.trackFileClose",
      "Log all the cases when files are closed. This is needed only for internal debugging purposes", Boolean.class, false),

  FILE_LOCK("file.lock", "Locks files when used. Default is true", boolean.class, true),

  FILE_DELETE_DELAY("file.deleteDelay", "Delay time (in ms) to wait for another attempt to delete a locked file", Integer.class,
      10),

  FILE_DELETE_RETRY("file.deleteRetry", "Number of retries to delete a locked file", Integer.class, 50),

  // SECURITY
  SECURITY_USER_PASSWORD_SALT_ITERATIONS("security.userPasswordSaltIterations",
      "Number of iterations to generate the salt or user password. Changing this setting does not affect stored passwords",
      Integer.class, 65536),

  SECURITY_USER_PASSWORD_SALT_CACHE_SIZE("security.userPasswordSaltCacheSize",
      "Cache size of hashed salt passwords. The cache works as LRU. Use 0 to disable the cache", Integer.class, 500),

  SECURITY_USER_PASSWORD_DEFAULT_ALGORITHM("security.userPasswordDefaultAlgorithm",
      "Default encryption algorithm used for passwords hashing", String.class, "PBKDF2WithHmacSHA256"),

  // NETWORK
  NETWORK_MAX_CONCURRENT_SESSIONS("network.maxConcurrentSessions", "Maximum number of concurrent sessions", Integer.class, 1000,
      true),

  NETWORK_SOCKET_BUFFER_SIZE("network.socketBufferSize", "TCP/IP Socket buffer size, if 0 use the OS default", Integer.class, 0,
      true),

  NETWORK_LOCK_TIMEOUT("network.lockTimeout", "Timeout (in ms) to acquire a lock against a channel", Integer.class, 15000, true),

  NETWORK_SOCKET_TIMEOUT("network.socketTimeout", "TCP/IP Socket timeout (in ms)", Integer.class, 15000, true),

  NETWORK_REQUEST_TIMEOUT("network.requestTimeout", "Request completion timeout (in ms)", Integer.class, 3600000 /* one hour */,
      true),

  NETWORK_SOCKET_RETRY("network.retry", "Number of attempts to connect to the server on failure", Integer.class, 5, true),

  NETWORK_SOCKET_RETRY_DELAY("network.retryDelay",
      "The time (in ms) the client must wait, before reconnecting to the server on failure", Integer.class, 500, true),

  NETWORK_BINARY_DNS_LOADBALANCING_ENABLED("network.binary.loadBalancing.enabled",
      "Asks for DNS TXT record, to determine if load balancing is supported", Boolean.class, Boolean.FALSE, true),

  NETWORK_BINARY_DNS_LOADBALANCING_TIMEOUT("network.binary.loadBalancing.timeout",
      "Maximum time (in ms) to wait for the answer from DNS about the TXT record for load balancing", Integer.class, 2000, true),

  NETWORK_BINARY_MAX_CONTENT_LENGTH("network.binary.maxLength", "TCP/IP max content length (in KB) of BINARY requests",
      Integer.class, 16384, true),

  NETWORK_BINARY_READ_RESPONSE_MAX_TIMES("network.binary.readResponse.maxTimes",
      "Maximum attempts, until a response can be read. Otherwise, the response will be dropped from the channel", Integer.class, 20,
      true),

  NETWORK_BINARY_DEBUG("network.binary.debug", "Debug mode: print all data incoming on the binary channel", Boolean.class, false,
      true),

  // HTTP

  /**
   * Since v2.2.8
   */
  NETWORK_HTTP_INSTALL_DEFAULT_COMMANDS("network.http.installDefaultCommands", "Installs the default HTTP commands", Boolean.class,
      Boolean.TRUE, true),

  NETWORK_HTTP_SERVER_INFO("network.http.serverInfo",
      "Server info to send in HTTP responses. Change the default if you want to hide it is a OrientDB Server", String.class,
      "OrientDB Server v." + OConstants.getVersion(), true),

  NETWORK_HTTP_MAX_CONTENT_LENGTH("network.http.maxLength", "TCP/IP max content length (in bytes) for HTTP requests", Integer.class,
      1000000, true),

  NETWORK_HTTP_STREAMING("network.http.streaming", "Enable Http chunked streaming for json responses", Boolean.class, false, true),

  NETWORK_HTTP_CONTENT_CHARSET("network.http.charset", "Http response charset", String.class, "utf-8", true),

  NETWORK_HTTP_JSON_RESPONSE_ERROR("network.http.jsonResponseError", "Http response error in json", Boolean.class, true, true),

  NETWORK_HTTP_JSONP_ENABLED("network.http.jsonp",
      "Enable the usage of JSONP, if requested by the client. The parameter name to use is 'callback'", Boolean.class, false, true),

  NETWORK_HTTP_SESSION_EXPIRE_TIMEOUT("network.http.sessionExpireTimeout",
      "Timeout, after which an http session is considered to have expired (in seconds)", Integer.class, 300),

  NETWORK_HTTP_USE_TOKEN("network.http.useToken", "Enable Token based sessions for http", Boolean.class, false),

  NETWORK_TOKEN_SECRETKEY("network.token.secretKey", "Network token sercret key", String.class, ""),

  NETWORK_TOKEN_ENCRYPTION_ALGORITHM("network.token.encryptionAlgorithm", "Network token algorithm", String.class, "HmacSHA256"),

  NETWORK_TOKEN_EXPIRE_TIMEOUT("network.token.expireTimeout",
      "Timeout, after which a binary session is considered to have expired (in minutes)", Integer.class, 60),

  // PROFILER

  PROFILER_ENABLED("profiler.enabled", "Enables the recording of statistics and counters", Boolean.class, false,
      new OConfigurationChangeCallback() {
        public void change(final Object iCurrentValue, final Object iNewValue) {
          final OProfiler prof = Orient.instance().getProfiler();
          if (prof != null)
            if ((Boolean) iNewValue)
              prof.startRecording();
            else
              prof.stopRecording();
        }
      }),

  PROFILER_CONFIG("profiler.config", "Configures the profiler as <seconds-for-snapshot>,<archive-snapshot-size>,<summary-size>",
      String.class, null, new OConfigurationChangeCallback() {
    public void change(final Object iCurrentValue, final Object iNewValue) {
      Orient.instance().getProfiler().configure(iNewValue.toString());
    }
  }),

  PROFILER_AUTODUMP_INTERVAL("profiler.autoDump.interval", "Dumps the profiler values at regular intervals (in seconds)",
      Integer.class, 0, new OConfigurationChangeCallback() {
    public void change(final Object iCurrentValue, final Object iNewValue) {
      Orient.instance().getProfiler().setAutoDump((Integer) iNewValue);
    }
  }),

  PROFILER_MAXVALUES("profiler.maxValues", "Maximum values to store. Values are managed in a LRU", Integer.class, 200),

  PROFILER_MEMORYCHECK_INTERVAL("profiler.memoryCheckInterval",
      "Checks the memory usage every configured milliseconds. Use 0 to disable it", Long.class, 120000),

  // SEQUENCES

  SEQUENCE_MAX_RETRY("sequence.maxRetry", "Maximum number of retries between attempt to change a sequence in concurrent mode",
      Integer.class, 100),

  SEQUENCE_RETRY_DELAY("sequence.retryDelay",
      "Maximum number of ms to wait between concurrent modification exceptions. The value is computed as random between 1 and this number",
      Integer.class, 200),

  /**
   * Interval between snapshots of profiler state in milliseconds, default value is 100.
   */
  STORAGE_PROFILER_SNAPSHOT_INTERVAL("storageProfiler.intervalBetweenSnapshots",
      "Interval between snapshots of profiler state in milliseconds", Integer.class, 100),

  STORAGE_PROFILER_CLEANUP_INTERVAL("storageProfiler.cleanUpInterval", "Interval between time series in milliseconds",
      Integer.class, 5000),

  // LOG
  LOG_CONSOLE_LEVEL("log.console.level", "Console logging level", String.class, "info", new OConfigurationChangeCallback() {
    public void change(final Object iCurrentValue, final Object iNewValue) {
      OLogManager.instance().setLevel((String) iNewValue, ConsoleHandler.class);
    }
  }),

  LOG_FILE_LEVEL("log.file.level", "File logging level", String.class, "info", new OConfigurationChangeCallback() {
    public void change(final Object iCurrentValue, final Object iNewValue) {
      OLogManager.instance().setLevel((String) iNewValue, FileHandler.class);
    }
  }),

  // CLASS
  CLASS_MINIMUM_CLUSTERS("class.minimumClusters", "Minimum clusters to create when a new class is created. 0 means Automatic",
      Integer.class, 0),

  // LOG
  LOG_SUPPORTS_ANSI("log.console.ansi",
      "ANSI Console support. 'auto' means automatic check if it is supported, 'true' to force using ANSI, 'false' to avoid using ANSI",
      String.class, "auto"),

  // CACHE
  CACHE_LOCAL_IMPL("cache.local.impl", "Local Record cache implementation", String.class, ORecordCacheWeakRefs.class.getName()),

  // COMMAND
  COMMAND_TIMEOUT("command.timeout", "Default timeout for commands (in ms)", Long.class, 0, true),

  COMMAND_CACHE_ENABLED("command.cache.enabled", "Enable command cache", Boolean.class, false),

  COMMAND_CACHE_EVICT_STRATEGY("command.cache.evictStrategy", "Command cache strategy between: [INVALIDATE_ALL,PER_CLUSTER]",
      String.class, "PER_CLUSTER"),

  COMMAND_CACHE_MIN_EXECUTION_TIME("command.cache.minExecutionTime", "Minimum execution time to consider caching the result set",
      Integer.class, 10),

  COMMAND_CACHE_MAX_RESULSET_SIZE("command.cache.maxResultsetSize", "Maximum resultset time to consider caching result set",
      Integer.class, 500),

  // QUERY
  QUERY_PARALLEL_AUTO("query.parallelAuto", "Auto enable parallel query, if requirements are met", Boolean.class, false),

  QUERY_PARALLEL_MINIMUM_RECORDS("query.parallelMinimumRecords",
      "Minimum number of records to activate parallel query automatically", Long.class, 300000),

  QUERY_PARALLEL_RESULT_QUEUE_SIZE("query.parallelResultQueueSize",
      "Size of the queue that holds results on parallel execution. The queue is blocking, so in case the queue is full, the query threads will be in a wait state",
      Integer.class, 20000),

  QUERY_SCAN_PREFETCH_PAGES("query.scanPrefetchPages",
      "Pages to prefetch during scan. Setting this value higher makes scans faster, because it reduces the number of I/O operations, though it consumes more memory. (Use 0 to disable)",
      Integer.class, 20),

  QUERY_SCAN_BATCH_SIZE("query.scanBatchSize",
      "Scan clusters in blocks of records. This setting reduces the lock time on the cluster during scans. A high value mean a faster execution, but also a lower concurrency level. Set to 0 to disable batch scanning. Disabling batch scanning is suggested for read-only databases only",
      Long.class, 1000),

  QUERY_SCAN_THRESHOLD_TIP("query.scanThresholdTip",
      "If the total number of records scanned in a query exceeds this setting, then a warning is given. (Use 0 to disable)",
      Long.class, 50000),

  QUERY_LIMIT_THRESHOLD_TIP("query.limitThresholdTip",
      "If the total number of returned records exceeds this value, then a warning is given. (Use 0 to disable)", Long.class, 10000),

  QUERY_LIVE_SUPPORT("query.live.support", "Enable/Disable the support of live query. (Use false to disable)", Boolean.class, true),

  STATEMENT_CACHE_SIZE("statement.cacheSize", "Number of parsed SQL statements kept in cache", Integer.class, 100),

  // GRAPH
  SQL_GRAPH_CONSISTENCY_MODE("sql.graphConsistencyMode",
      "Consistency mode for graphs. It can be 'tx' (default), 'notx_sync_repair' and 'notx_async_repair'. "
          + "'tx' uses transactions to maintain consistency. Instead both 'notx_sync_repair' and 'notx_async_repair' do not use transactions, "
          + "and the consistency, in case of JVM crash, is guaranteed by a database repair operation that run at startup. "
          + "With 'notx_sync_repair' the repair is synchronous, so the database comes online after the repair is ended, while "
          + "with 'notx_async_repair' the repair is a background process", String.class, "tx"),

  /**
   * Maximum size of pool of network channels between client and server. A channel is a TCP/IP connection.
   */
  CLIENT_CHANNEL_MAX_POOL("client.channel.maxPool",
      "Maximum size of pool of network channels between client and server. A channel is a TCP/IP connection", Integer.class, 100),

  /**
   * Maximum time, where the client should wait for a connection from the pool, when all connections busy.
   */
  CLIENT_CONNECT_POOL_WAIT_TIMEOUT("client.connectionPool.waitTimeout",
      "Maximum time, where the client should wait for a connection from the pool, when all connections busy", Integer.class, 5000,
      true),

  CLIENT_DB_RELEASE_WAIT_TIMEOUT("client.channel.dbReleaseWaitTimeout",
      "Delay (in ms), after which a data modification command will be resent, if the DB was frozen", Integer.class, 10000, true),

  CLIENT_USE_SSL("client.ssl.enabled", "Use SSL for client connections", Boolean.class, false),

  CLIENT_SSL_KEYSTORE("client.ssl.keyStore", "Use SSL for client connections", String.class, null),

  CLIENT_SSL_KEYSTORE_PASSWORD("client.ssl.keyStorePass", "Use SSL for client connections", String.class, null),

  CLIENT_SSL_TRUSTSTORE("client.ssl.trustStore", "Use SSL for client connections", String.class, null),

  CLIENT_SSL_TRUSTSTORE_PASSWORD("client.ssl.trustStorePass", "Use SSL for client connections", String.class, null),

  // SERVER
  SERVER_OPEN_ALL_DATABASES_AT_STARTUP("server.openAllDatabasesAtStartup",
      "If true, the server opens all the available databases at startup. Available since 2.2", Boolean.class, false),

  SERVER_CHANNEL_CLEAN_DELAY("server.channel.cleanDelay", "Time in ms of delay to check pending closed connections", Integer.class,
      5000),

  SERVER_CACHE_FILE_STATIC("server.cache.staticFile", "Cache static resources upon loading", Boolean.class, false),

  SERVER_LOG_DUMP_CLIENT_EXCEPTION_LEVEL("server.log.dumpClientExceptionLevel",
      "Logs client exceptions. Use any level supported by Java java.util.logging.Level class: OFF, FINE, CONFIG, INFO, WARNING, SEVERE",
      String.class, Level.FINE.getName()),

  SERVER_LOG_DUMP_CLIENT_EXCEPTION_FULLSTACKTRACE("server.log.dumpClientExceptionFullStackTrace",
      "Dumps the full stack trace of the exception sent to the client", Boolean.class, Boolean.FALSE, true),

  // DISTRIBUTED

  DISTRIBUTED_CRUD_TASK_SYNCH_TIMEOUT("distributed.crudTaskTimeout", "Maximum timeout (in ms) to wait for CRUD remote tasks",
      Long.class, 10000l, true),

  DISTRIBUTED_MAX_STARTUP_DELAY("distributed.maxStartupDelay", "Maximum delay time (in ms) to wait for a server to start",
      Long.class, 10000l, true),

  DISTRIBUTED_COMMAND_TASK_SYNCH_TIMEOUT("distributed.commandTaskTimeout",
      "Maximum timeout (in ms) to wait for command distributed tasks", Long.class, 2 * 60 * 1000l, true),

  DISTRIBUTED_COMMAND_QUICK_TASK_SYNCH_TIMEOUT("distributed.commandQuickTaskTimeout",
      "Maximum timeout (in ms) to wait for quick command distributed tasks", Long.class, 5 * 1000l, true),

  DISTRIBUTED_COMMAND_LONG_TASK_SYNCH_TIMEOUT("distributed.commandLongTaskTimeout",
      "Maximum timeout (in ms) to wait for Long-running distributed tasks", Long.class, 24 * 60 * 60 * 1000, true),

  DISTRIBUTED_DEPLOYDB_TASK_SYNCH_TIMEOUT("distributed.deployDbTaskTimeout",
      "Maximum timeout (in ms) to wait for database deployment", Long.class, 1200000l, true),

  DISTRIBUTED_DEPLOYCHUNK_TASK_SYNCH_TIMEOUT("distributed.deployChunkTaskTimeout",
      "Maximum timeout (in ms) to wait for database chunk deployment", Long.class, 15000l, true),

  DISTRIBUTED_DEPLOYDB_TASK_COMPRESSION("distributed.deployDbTaskCompression",
      "Compression level (between 0 and 9) to use in backup for database deployment", Integer.class, 7, true),

  DISTRIBUTED_ASYNCH_QUEUE_SIZE("distributed.asynchQueueSize",
      "Queue size to handle distributed asynchronous operations. The bigger is the queue, the more operation are buffered, but also more memory it's consumed. 0 = dynamic allocation, which means up to 2^31-1 entries",
      Integer.class, 0),

  DISTRIBUTED_ASYNCH_RESPONSES_TIMEOUT("distributed.asynchResponsesTimeout",
      "Maximum timeout (in ms) to collect all the asynchronous responses from replication. After this time the operation is rolled back (through an UNDO)",
      Long.class, 15000l),

  DISTRIBUTED_PURGE_RESPONSES_TIMER_DELAY("distributed.purgeResponsesTimerDelay",
      "Maximum timeout (in ms) to collect all the asynchronous responses from replication. This is the delay the purge thread uses to check asynchronous requests in timeout",
      Long.class, 15000l),

  /**
   * @Since 2.2.7
   */
  DISTRIBUTED_CONFLICT_RESOLVER_REPAIRER_CHAIN("distributed.conflictResolverRepairerChain",
      "Chain of conflict resolver implementation to use", String.class, "majority,content,version", false),

  /**
   * @Since 2.2.7
   */
  DISTRIBUTED_CONFLICT_RESOLVER_REPAIRER_CHECK_EVERY("distributed.conflictResolverRepairerCheckEvery",
      "Time (in ms) when the conflict resolver auto-repairer checks for records/cluster to repair", Long.class, 5000, true),

  /**
   * @Since 2.2.7
   */
  DISTRIBUTED_CONFLICT_RESOLVER_REPAIRER_BATCH("distributed.conflictResolverRepairerBatch", "Number of record to repair in batch",
      Integer.class, 100, true),

  /**
   * @Since 2.2.7
   */
  DISTRIBUTED_TX_EXPIRE_TIMEOUT("distributed.txAliveTimeout",
      "Maximum timeout (in ms) a distributed transaction can be alive. This timeout is to rollback pending transactions after a while",
      Long.class, 30000l, true),

  /**
   * @Since 2.2.6
   */
  DISTRIBUTED_REQUEST_CHANNELS("distributed.requestChannels", "Number of network channels used to send requests", Integer.class, 1),

  /**
   * @Since 2.2.6
   */
  DISTRIBUTED_RESPONSE_CHANNELS("distributed.responseChannels", "Number of network channels used to send responses", Integer.class,
      1),

  /**
   * @Since 2.2.5
   */
  DISTRIBUTED_HEARTBEAT_TIMEOUT("distributed.heartbeatTimeout",
      "Maximum time in ms to wait for the heartbeat. If the server does not respond in time, it is put offline", Long.class,
      10000l),

  /**
   * @Since 2.2.5
   */
  DISTRIBUTED_CHECK_HEALTH_CAN_OFFLINE_SERVER("distributed.checkHealthCanOfflineServer",
      "In case a server does not respond to the heartbeat message, it is set offline", Boolean.class, false),

  /**
   * @Since 2.2.5
   */
  DISTRIBUTED_CHECK_HEALTH_EVERY("distributed.checkHealthEvery", "Time in ms to check the cluster health. Set to 0 to disable it",
      Long.class, 10000l),

  /**
   * Since 2.2.4
   */
  DISTRIBUTED_AUTO_REMOVE_OFFLINE_SERVERS("distributed.autoRemoveOfflineServers",
      "This is the amount of time (in ms) the server has to be OFFLINE, before it is automatically removed from the distributed configuration. -1 = never, 0 = immediately, >0 the actual time to wait",
      Long.class, 0, true),

  /**
   * @Since 2.2.0
   */
  DISTRIBUTED_PUBLISH_NODE_STATUS_EVERY("distributed.publishNodeStatusEvery",
      "Time in ms to publish the node status on distributed map. Set to 0 to disable such refresh of node configuration",
      Long.class, 5000l, true),

  /**
   * @Since 2.2.0
   */
  @OApi(maturity = OApi.MATURITY.NEW)DISTRIBUTED_LOCAL_QUEUESIZE("distributed.localQueueSize",
      "Size of the intra-thread queue for distributed messages", Integer.class, 10000),

  /**
   * @Since 2.2.0
   */
  @OApi(maturity = OApi.MATURITY.NEW)DISTRIBUTED_DB_WORKERTHREADS("distributed.dbWorkerThreads",
      "Number of parallel worker threads per database that process distributed messages", Integer.class, 8),

  /**
   * @Since 2.1.3, Deprecated in 2.2.0
   */
  @Deprecated @OApi(maturity = OApi.MATURITY.NEW)DISTRIBUTED_QUEUE_MAXSIZE("distributed.queueMaxSize",
      "Maximum queue size to mark a node as stalled. If the number of messages in queue are more than this values, the node is restarted with a remote command (0 = no maximum, which means up to 2^31-1 entries)",
      Integer.class, 10000),

  /**
   * @Since 2.1.3
   */
  @OApi(maturity = OApi.MATURITY.NEW)DISTRIBUTED_BACKUP_DIRECTORY("distributed.backupDirectory",
      "Directory where the copy of an existent database is saved, before it is downloaded from the cluster. Leave it empty to avoid the backup.",
      String.class, "../backup/databases"),

  /**
   * @Since 2.2.15
   */
  @OApi(maturity = OApi.MATURITY.NEW)DISTRIBUTED_BACKUP_TRY_INCREMENTAL_FIRST("distributed.backupTryIncrementalFirst",
      "Try to execute an incremental backup first.", Boolean.class, true),

  /**
   * @Since 2.1
   */
  @OApi(maturity = OApi.MATURITY.NEW)DISTRIBUTED_CONCURRENT_TX_MAX_AUTORETRY("distributed.concurrentTxMaxAutoRetry",
      "Maximum attempts the transaction coordinator should execute a transaction automatically, if records are locked. (Minimum is 1 = no attempts)",
      Integer.class, 10, true),

  /**
   * @Since 2.2.7
   */
  @OApi(maturity = OApi.MATURITY.NEW)DISTRIBUTED_ATOMIC_LOCK_TIMEOUT("distributed.atomicLockTimeout",
      "Timeout (in ms) to acquire a distributed lock on a record. (0=infinite)", Integer.class, 300, true),

  /**
   * @Since 2.1
   */
  @OApi(maturity = OApi.MATURITY.NEW)DISTRIBUTED_CONCURRENT_TX_AUTORETRY_DELAY("distributed.concurrentTxAutoRetryDelay",
      "Delay (in ms) between attempts on executing a distributed transaction, which had failed because of locked records. (0=no delay)",
      Integer.class, 100, true),

  DB_DOCUMENT_SERIALIZER("db.document.serializer", "The default record serializer used by the document database", String.class,
      ORecordSerializerBinary.NAME),

  /**
   * @Since 2.2
   */
  @OApi(maturity = OApi.MATURITY.NEW)CLIENT_KRB5_CONFIG("client.krb5.config", "Location of the Kerberos configuration file",
      String.class, null),

  /**
   * @Since 2.2
   */
  @OApi(maturity = OApi.MATURITY.NEW)CLIENT_KRB5_CCNAME("client.krb5.ccname", "Location of the Kerberos client ticketcache",
      String.class, null),

  /**
   * @Since 2.2
   */
  @OApi(maturity = OApi.MATURITY.NEW)CLIENT_KRB5_KTNAME("client.krb5.ktname", "Location of the Kerberos client keytab",
      String.class, null),

  /**
   * @Since 2.2
   */
  @OApi(maturity = OApi.MATURITY.NEW)CLIENT_CREDENTIAL_INTERCEPTOR("client.credentialinterceptor",
      "The name of the CredentialInterceptor class", String.class, null),

  @OApi(maturity = OApi.MATURITY.NEW)CLIENT_CI_KEYALGORITHM("client.ci.keyalgorithm",
      "The key algorithm used by the symmetric key credential interceptor", String.class, "AES"),

  @OApi(maturity = OApi.MATURITY.NEW)CLIENT_CI_CIPHERTRANSFORM("client.ci.ciphertransform",
      "The cipher transformation used by the symmetric key credential interceptor", String.class, "AES/CBC/PKCS5Padding"),

  @OApi(maturity = OApi.MATURITY.NEW)CLIENT_CI_KEYSTORE_FILE("client.ci.keystore.file",
      "The file path of the keystore used by the symmetric key credential interceptor", String.class, null),

  @OApi(maturity = OApi.MATURITY.NEW)CLIENT_CI_KEYSTORE_PASSWORD("client.ci.keystore.password",
      "The password of the keystore used by the symmetric key credential interceptor", String.class, null),

  /**
   * @Since 2.2
   */
  @OApi(maturity = OApi.MATURITY.NEW)CREATE_DEFAULT_USERS("security.createDefaultUsers",
      "Indicates whether default database users should be created", Boolean.class, true),

  /**
   * @Since 2.2
   */
  @OApi(maturity = OApi.MATURITY.NEW)SERVER_SECURITY_FILE("server.security.file",
      "Location of the OrientDB security.json configuration file", String.class, null),

  /**
   * Deprecated in v2.2.0
   */
  @Deprecated JNA_DISABLE_USE_SYSTEM_LIBRARY("jna.disable.system.library",
      "This property disables using JNA, should it be installed on your system. (Default true) To use JNA bundled with database",
      boolean.class, true),

  @Deprecated DISTRIBUTED_QUEUE_TIMEOUT("distributed.queueTimeout",
      "Maximum timeout (in ms) to wait for the response in replication", Long.class, 500000l, true),

  @Deprecated DB_MAKE_FULL_CHECKPOINT_ON_INDEX_CHANGE("db.makeFullCheckpointOnIndexChange",
      "When index metadata is changed, a full checkpoint is performed", Boolean.class, true, true),

  @Deprecated DB_MAKE_FULL_CHECKPOINT_ON_SCHEMA_CHANGE("db.makeFullCheckpointOnSchemaChange",
      "When index schema is changed, a full checkpoint is performed", Boolean.class, true, true),

  @Deprecated CLIENT_SESSION_TOKEN_BASED("client.session.tokenBased", "Request a token based session to the server", Boolean.class,
      true),

  @Deprecated OAUTH2_SECRETKEY("oauth2.secretkey", "Http OAuth2 secret key", String.class, ""),

  @Deprecated STORAGE_USE_CRC32_FOR_EACH_RECORD("storage.cluster.usecrc32",
      "Indicates whether crc32 should be used for each record to check record integrity", Boolean.class, false),

  @Deprecated LAZYSET_WORK_ON_STREAM("lazyset.workOnStream", "Deprecated, now BINARY serialization is used in place of CSV",
      Boolean.class, true),

  @Deprecated DB_MVCC("db.mvcc", "Deprecated, MVCC cannot be disabled anymore", Boolean.class, true),

  @Deprecated DB_USE_DISTRIBUTED_VERSION("db.use.distributedVersion", "Deprecated, distributed version is not used anymore",
      Boolean.class, Boolean.FALSE),

  @Deprecated MVRBTREE_TIMEOUT("mvrbtree.timeout", "Deprecated, MVRBTREE IS NOT USED ANYMORE IN FAVOR OF SBTREE AND HASHINDEX",
      Integer.class, 0),

  @Deprecated MVRBTREE_NODE_PAGE_SIZE("mvrbtree.nodePageSize",
      "Deprecated, MVRBTREE IS NOT USED ANYMORE IN FAVOR OF SBTREE AND HASHINDEX", Integer.class, 256),

  @Deprecated MVRBTREE_LOAD_FACTOR("mvrbtree.loadFactor",
      "Deprecated, MVRBTREE IS NOT USED ANYMORE IN FAVOR OF SBTREE AND HASHINDEX", Float.class, 0.7f),

  @Deprecated MVRBTREE_OPTIMIZE_THRESHOLD("mvrbtree.optimizeThreshold",
      "Deprecated, MVRBTREE IS NOT USED ANYMORE IN FAVOR OF SBTREE AND HASHINDEX", Integer.class, 100000),

  @Deprecated MVRBTREE_ENTRYPOINTS("mvrbtree.entryPoints",
      "Deprecated, MVRBTREE IS NOT USED ANYMORE IN FAVOR OF SBTREE AND HASHINDEX", Integer.class, 64),

  @Deprecated MVRBTREE_OPTIMIZE_ENTRYPOINTS_FACTOR("mvrbtree.optimizeEntryPointsFactor",
      "Deprecated, MVRBTREE IS NOT USED ANYMORE IN FAVOR OF SBTREE AND HASHINDEX", Float.class, 1.0f),

  @Deprecated MVRBTREE_ENTRY_KEYS_IN_MEMORY("mvrbtree.entryKeysInMemory",
      "Deprecated, MVRBTREE IS NOT USED ANYMORE IN FAVOR OF SBTREE AND HASHINDEX", Boolean.class, Boolean.FALSE),

  @Deprecated MVRBTREE_ENTRY_VALUES_IN_MEMORY("mvrbtree.entryValuesInMemory",
      "Deprecated, MVRBTREE IS NOT USED ANYMORE IN FAVOR OF SBTREE AND HASHINDEX", Boolean.class, Boolean.FALSE),

  @Deprecated MVRBTREE_RID_BINARY_THRESHOLD("mvrbtree.ridBinaryThreshold",
      "Deprecated, MVRBTREE IS NOT USED ANYMORE IN FAVOR OF SBTREE AND HASHINDEX", Integer.class, -1),

  @Deprecated MVRBTREE_RID_NODE_PAGE_SIZE("mvrbtree.ridNodePageSize",
      "Deprecated, MVRBTREE IS NOT USED ANYMORE IN FAVOR OF SBTREE AND HASHINDEX", Integer.class, 64),

  @Deprecated MVRBTREE_RID_NODE_SAVE_MEMORY("mvrbtree.ridNodeSaveMemory",
      "Deprecated, MVRBTREE IS NOT USED ANYMORE IN FAVOR OF SBTREE AND HASHINDEX", Boolean.class, Boolean.FALSE),

  @Deprecated TX_COMMIT_SYNCH("tx.commit.synch", "Synchronizes the storage after transaction commit", Boolean.class, false),

  @Deprecated TX_AUTO_RETRY("tx.autoRetry",
      "Maximum number of automatic retry if some resource has been locked in the middle of the transaction (Timeout exception)",
      Integer.class, 1),

  @Deprecated TX_LOG_TYPE("tx.log.fileType", "File type to handle transaction logs: mmap or classic", String.class, "classic"),

  @Deprecated TX_LOG_SYNCH("tx.log.synch",
      "Executes a synch against the file-system at every log entry. This slows down transactions but guarantee transaction reliability on unreliable drives",
      Boolean.class, Boolean.FALSE), @Deprecated TX_USE_LOG("tx.useLog",
      "Transactions use log file to store temporary data to be rolled back in case of crash", Boolean.class, true),

  @Deprecated INDEX_AUTO_REBUILD_AFTER_NOTSOFTCLOSE("index.auto.rebuildAfterNotSoftClose",
      "Auto rebuild all automatic indexes after upon database open when wasn't closed properly", Boolean.class, true),

  @Deprecated CLIENT_CHANNEL_MIN_POOL("client.channel.minPool", "Minimum pool size", Integer.class, 1),

  // DEPRECATED IN 2.0
  @Deprecated STORAGE_KEEP_OPEN("storage.keepOpen", "Deprecated", Boolean.class, Boolean.TRUE),

  // DEPRECATED IN 2.0, LEVEL1 CACHE CANNOT BE DISABLED ANYMORE
  @Deprecated CACHE_LOCAL_ENABLED("cache.local.enabled", "Deprecated, Level1 cache cannot be disabled anymore", Boolean.class,
      true);

  private final String   key;
  private final Object   defValue;
  private final Class<?> type;
  private volatile Object value = null;
  private final String                       description;
  private final OConfigurationChangeCallback changeCallback;
  private final Boolean                      canChangeAtRuntime;
  private final boolean                      hidden;

  static {
    readConfiguration();
  }

  OGlobalConfiguration(final String iKey, final String iDescription, final Class<?> iType, final Object iDefValue,
      final OConfigurationChangeCallback iChangeAction) {
    key = iKey;
    description = iDescription;
    defValue = iDefValue;
    type = iType;
    canChangeAtRuntime = true;
    hidden = false;
    changeCallback = iChangeAction;
  }

  OGlobalConfiguration(final String iKey, final String iDescription, final Class<?> iType, final Object iDefValue) {
    this(iKey, iDescription, iType, iDefValue, false);
  }

  OGlobalConfiguration(final String iKey, final String iDescription, final Class<?> iType, final Object iDefValue,
      final Boolean iCanChange) {
    this(iKey, iDescription, iType, iDefValue, iCanChange, false);
  }

  OGlobalConfiguration(final String iKey, final String iDescription, final Class<?> iType, final Object iDefValue,
      final boolean iCanChange, final boolean iHidden) {
    key = iKey;
    description = iDescription;
    defValue = iDefValue;
    type = iType;
    canChangeAtRuntime = iCanChange;
    hidden = iHidden;
    changeCallback = null;
  }

  public static void dumpConfiguration(final PrintStream out) {
    out.print("OrientDB ");
    out.print(OConstants.getVersion());
    out.println(" configuration dump:");

    String lastSection = "";
    for (OGlobalConfiguration v : values()) {
      final String section = v.key.substring(0, v.key.indexOf('.'));

      if (!lastSection.equals(section)) {
        out.print("- ");
        out.println(section.toUpperCase());
        lastSection = section;
      }
      out.print("  + ");
      out.print(v.key);
      out.print(" = ");
      out.println(v.isHidden() ? "<hidden>" : v.getValue());
    }
  }

  /**
   * Find the OGlobalConfiguration instance by the key. Key is case insensitive.
   *
   * @param iKey Key to find. It's case insensitive.
   *
   * @return OGlobalConfiguration instance if found, otherwise null
   */
  public static OGlobalConfiguration findByKey(final String iKey) {
    for (OGlobalConfiguration v : values()) {
      if (v.getKey().equalsIgnoreCase(iKey))
        return v;
    }
    return null;
  }

  /**
   * Changes the configuration values in one shot by passing a Map of values. Keys can be the Java ENUM names or the string
   * representation of configuration values
   */
  public static void setConfiguration(final Map<String, Object> iConfig) {
    for (Entry<String, Object> config : iConfig.entrySet()) {
      for (OGlobalConfiguration v : values()) {
        if (v.getKey().equals(config.getKey())) {
          v.setValue(config.getValue());
          break;
        } else if (v.name().equals(config.getKey())) {
          v.setValue(config.getValue());
          break;
        }
      }
    }
  }

  /**
   * Assign configuration values by reading system properties.
   */
  private static void readConfiguration() {
    String prop;
    for (OGlobalConfiguration config : values()) {
      prop = System.getProperty(config.key);
      if (prop != null)
        config.setValue(prop);
    }
  }

  public Object getValue() {
    return value != null ? value : defValue;
  }

  public void setValue(final Object iValue) {
    Object oldValue = value;

    if (iValue != null)
      if (type == Boolean.class)
        value = Boolean.parseBoolean(iValue.toString());
      else if (type == Integer.class)
        value = Integer.parseInt(iValue.toString());
      else if (type == Float.class)
        value = Float.parseFloat(iValue.toString());
      else if (type == String.class)
        value = iValue.toString();
      else
        value = iValue;

    if (changeCallback != null) {
      try {
        changeCallback.change(oldValue, value);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  public boolean getValueAsBoolean() {
    final Object v = value != null ? value : defValue;
    return v instanceof Boolean ? (Boolean) v : Boolean.parseBoolean(v.toString());
  }

  public String getValueAsString() {
    return value != null ? value.toString() : defValue != null ? defValue.toString() : null;
  }

  public int getValueAsInteger() {
    final Object v = value != null ? value : defValue;
    return (int) (v instanceof Number ? ((Number) v).intValue() : OFileUtils.getSizeAsNumber(v.toString()));
  }

  public long getValueAsLong() {
    final Object v = value != null ? value : defValue;
    return v instanceof Number ? ((Number) v).longValue() : OFileUtils.getSizeAsNumber(v.toString());
  }

  public float getValueAsFloat() {
    final Object v = value != null ? value : defValue;
    return v instanceof Float ? (Float) v : Float.parseFloat(v.toString());
  }

  public String getKey() {
    return key;
  }

  public Boolean isChangeableAtRuntime() {
    return canChangeAtRuntime;
  }

  public boolean isHidden() {
    return hidden;
  }

  public Object getDefValue() {
    return defValue;
  }

  public Class<?> getType() {
    return type;
  }

  public String getDescription() {
    return description;
  }
}
