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
import com.orientechnologies.orient.core.engine.local.OEngineLocalPaginated;
import com.orientechnologies.orient.core.metadata.OMetadataDefault;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerBinary;
import com.orientechnologies.orient.core.storage.cache.local.O2QCache;

import java.io.File;
import java.io.PrintStream;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
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
  ENVIRONMENT_DUMP_CFG_AT_STARTUP("environment.dumpCfgAtStartup", "Dumps the configuration at application startup", Boolean.class,
      Boolean.FALSE),

  ENVIRONMENT_CONCURRENT("environment.concurrent",
      "Specifies if running in multi-thread environment. Setting this to false turns off the internal lock management",
      Boolean.class, Boolean.TRUE),

  ENVIRONMENT_ALLOW_JVM_SHUTDOWN("environment.allowJVMShutdown", "Allows to shutdown the JVM if needed/requested", Boolean.class,
      true, true),

  ENVNRONMENT_CONCURRENCY_LEVEL("envinronment.concurrency.level", "Level of paralellization for structures which are "
      + "split their internal container on several partitions to increase multicore scalability"
      + " like ConcurrentHashMap, bigger value means bigger memory consumption.", Integer.class, Runtime.getRuntime().availableProcessors()),

  // SCRIPT
      SCRIPT_POOL("script.pool.maxSize", "Maximum number of instances in the pool of script engines", Integer.class, 20),

  // MEMORY
      MEMORY_USE_UNSAFE("memory.useUnsafe", "Indicates whether Unsafe will be used if it is present", Boolean.class, true),

  DIRECT_MEMORY_SAFE_MODE("memory.directMemory.safeMode",
      "Indicates whether to do perform range check before each direct memory update, it is true by default, "
          + "but usually it can be safely put to false. It is needed to set to true only after dramatic changes in storage structures.",
      Boolean.class, true),

  DIRECT_MEMORY_TRACK_MODE("memory.directMemory.trackMode",
      "If 'track mode' is switched on then following steps are performed: "
          + "1. direct memory JMX bean is registered. 2. You may check amount of allocated direct memory as property of JMX bean. "
          + "3. If memory leak is detected then JMX event will be fired. "
          + "This mode provides big overhead and may be used only for testing purpose",
      Boolean.class, false),

  DIRECT_MEMORY_ONLY_ALIGNED_ACCESS("memory.directMemory.onlyAlignedMemoryAccess",
      "Some architectures does not allow unaligned memory access or suffer from speed degradation, on this platforms flag should be set to true",
      Boolean.class, true),

  JVM_GC_DELAY_FOR_OPTIMIZE("jvm.gc.delayForOptimize",
      "Minimal amount of time (seconds) since last System.gc() when called after tree optimization", Long.class, 600),

  // STORAGE
      DISK_CACHE_PINNED_PAGES("storage.diskCache.pinnedPages",
          "Maximum amount of pinned pages which may be contained in cache,"
              + " if this percent is reached next pages will be left in unpinned state. You can not set value more than 50",
          Integer.class, 20, false),

  DISK_CACHE_SIZE("storage.diskCache.bufferSize",
      "Size of disk buffer in megabytes, disk size may be changed at runtime, "
          + "but if does not enough to contain all pinned pages exception will be thrown.",
      Integer.class, 4 * 1024, new OConfigurationChangeCallback() {
        @Override
        public void change(Object currentValue, Object newValue) {
          final OEngineLocalPaginated engineLocalPaginated = (OEngineLocalPaginated) Orient.instance()
              .getEngine(OEngineLocalPaginated.NAME);

          if (engineLocalPaginated != null) {
            engineLocalPaginated.changeCacheSize(((Integer) (newValue)) * 1024L * 1024L);
          }
        }
      }),

  DISK_WRITE_CACHE_PART("storage.diskCache.writeCachePart", "Percent of disk cache which is use as write cache", Integer.class, 15),

  DISK_WRITE_CACHE_PAGE_TTL("storage.diskCache.writeCachePageTTL", "Max time till page will be flushed from write cache in seconds",
      Long.class, 24 * 60 * 60),

  DISK_WRITE_CACHE_PAGE_FLUSH_INTERVAL("storage.diskCache.writeCachePageFlushInterval",
      "Interval between flushing of pages from write cache in ms.", Integer.class, 25),

  DISK_WRITE_CACHE_FLUSH_WRITE_INACTIVITY_INTERVAL("storage.diskCache.writeCacheFlushInactivityInterval",
      "Interval between 2 writes to the disk cache,"
          + " if writes are done with interval more than provided all files will be fsynced before next write,"
          + " which allows do not do data restore after server crash (in ms).",
      Long.class, 60 * 1000),

  DISK_WRITE_CACHE_FLUSH_LOCK_TIMEOUT("storage.diskCache.writeCacheFlushLockTimeout",
      "Maximum amount of time till write cache will be wait before page flush in ms.", Integer.class, -1),

  DISK_CACHE_FREE_SPACE_LIMIT("storage.diskCache.diskFreeSpaceLimit",
      "Minimum amount of space on disk after which database will " + "work only in read mode, in megabytes", Long.class, 100),

  @Deprecated

  DISC_CACHE_FREE_SPACE_CHECK_INTERVAL("storage.diskCache.diskFreeSpaceCheckInterval",
      "The interval (in seconds), after which the storage periodically "
          + "checks whether the amount of free disk space is enough to work in write mode", Integer.class, 5),

  /**
   * The interval (how many new pages should be added before free space will be checked), after which the storage periodically
   * checks whether the amount of free disk space is enough to work in write mode.
   */
  DISC_CACHE_FREE_SPACE_CHECK_INTERVAL_IN_PAGES("storage.diskCache.diskFreeSpaceCheckIntervalInPages",
      "The interval (how many new pages should be added before free space will be checked), after which the storage periodically "
          + "checks whether the amount of free disk space is enough to work in write mode", Integer.class, 4096),

  /**
   * Keep disk cache state between moment when storage is closed and moment when it is opened again.
   * <code>true</code> by default.
   */
  STORAGE_KEEP_DISK_CACHE_STATE("storage.diskCache.keepState",
      "Keep disk cache state between moment when storage is closed and moment when it is opened again. true by default.",
      Boolean.class, true),

  STORAGE_CONFIGURATION_SYNC_ON_UPDATE("storage.configuration.syncOnUpdate",
      "Should we perform force sync of storage configuration for each update", Boolean.class, true),

  STORAGE_COMPRESSION_METHOD("storage.compressionMethod", "Record compression method is used in storage."
      + " Possible values : gzip, nothing, snappy, snappy-native. Default is snappy.", String.class, "nothing"),

  USE_WAL("storage.useWAL", "Whether WAL should be used in paginated storage", Boolean.class, true),

  WAL_SYNC_ON_PAGE_FLUSH("storage.wal.syncOnPageFlush", "Should we perform force sync during WAL page flush", Boolean.class, true),

  WAL_CACHE_SIZE("storage.wal.cacheSize",
      "Maximum size of WAL cache (in amount of WAL pages, each page is 64k) <= 0 means that caching will be switched off.",
      Integer.class, 3000),

  WAL_MAX_SEGMENT_SIZE("storage.wal.maxSegmentSize", "Maximum size of single. WAL segment in megabytes.", Integer.class, 128),

  WAL_MAX_SIZE("storage.wal.maxSize", "Supposed, maximum size of WAL on disk in megabytes. This size may be more or less. ",
      Integer.class, 4096),

  WAL_COMMIT_TIMEOUT("storage.wal.commitTimeout", "Maximum interval between WAL commits (in ms.)", Integer.class, 1000),

  WAL_SHUTDOWN_TIMEOUT("storage.wal.shutdownTimeout", "Maximum wait interval between events when background flush thread"
      + " will receive shutdown command and when background flush will be stopped (in ms.)", Integer.class, 10000),

  WAL_FUZZY_CHECKPOINT_INTERVAL("storage.wal.fuzzyCheckpointInterval", "Interval between fuzzy checkpoints (in seconds)",
      Integer.class, 300),

  WAL_REPORT_AFTER_OPERATIONS_DURING_RESTORE("storage.wal.reportAfterOperationsDuringRestore",
      "Amount of processed log operations, after which status of data restore procedure will be printed 0 or negative value, means that status will not be printed",
      Integer.class, 10000),

  WAL_RESTORE_BATCH_SIZE("storage.wal.restore.batchSize",
      "Amount of wal records are read at once in single batch during restore procedure", Integer.class, 1000),

  WAL_READ_CACHE_SIZE("storage.wal.readCacheSize", "Size of WAL read cache in amount of pages", Integer.class, 1000),

  WAL_FUZZY_CHECKPOINT_SHUTDOWN_TIMEOUT("storage.wal.fuzzyCheckpointShutdownWait",
      "Interval which we should wait till shutdown (in seconds)", Integer.class, 60 * 10),

  WAL_FULL_CHECKPOINT_SHUTDOWN_TIMEOUT("storage.wal.fullCheckpointShutdownTimeout",
      "Timeout till DB will wait that full checkpoint is finished during DB close (in seconds))", Integer.class, 60 * 10),

  WAL_LOCATION("storage.wal.path", "Path to the wal file on the disk, by default is placed in DB directory but"
      + " it is highly recomended to use separate disk to store log operations", String.class, null),

  STORAGE_MAKE_FULL_CHECKPOINT_AFTER_CREATE("storage.makeFullCheckpointAfterCreate",
      "Indicates whether full checkpoint should be performed if storage was created.", Boolean.class, true),

  STORAGE_MAKE_FULL_CHECKPOINT_AFTER_OPEN("storage.makeFullCheckpointAfterOpen",
      "Indicates whether full checkpoint should be performed if storage was opened. It is needed to make fuzzy checkpoints to work without issues",
      Boolean.class, true),

  STORAGE_MAKE_FULL_CHECKPOINT_AFTER_CLUSTER_CREATE("storage.makeFullCheckpointAfterClusterCreate",
      "Indicates whether full checkpoint should be performed if storage was opened.", Boolean.class, true),

  DISK_CACHE_PAGE_SIZE("storage.diskCache.pageSize", "Size of page of disk buffer in kilobytes,!!! NEVER CHANGE THIS VALUE !!!",
      Integer.class, 64),

  PAGINATED_STORAGE_LOWEST_FREELIST_BOUNDARY("storage.lowestFreeListBound",
      "The minimal amount of free space (in kb)" + " in page which is tracked in paginated storage", Integer.class, 16),

  @Deprecated STORAGE_USE_CRC32_FOR_EACH_RECORD("storage.cluster.usecrc32",
      "Indicates whether crc32 should be used for each record to check record integrity.", Boolean.class, false),

  STORAGE_LOCK_TIMEOUT("storage.lockTimeout", "Maximum timeout in milliseconds to lock the storage", Integer.class, 0),

  STORAGE_RECORD_LOCK_TIMEOUT("storage.record.lockTimeout", "Maximum timeout in milliseconds to lock a shared record",
      Integer.class, 2000),

  STORAGE_USE_TOMBSTONES("storage.useTombstones",
      "When record will be deleted its cluster" + " position will not be freed but tombstone will be placed instead", Boolean.class,
      false),

  // RECORDS
      RECORD_DOWNSIZING_ENABLED("record.downsizing.enabled",
          "On updates if the record size is lower than before, reduces the space taken accordingly. If enabled this could increase defragmentation, but it reduces the used space",
          Boolean.class, true),

  // DATABASE
          OBJECT_SAVE_ONLY_DIRTY("object.saveOnlyDirty", "Object Database only saves objects bound to dirty records", Boolean.class,
              false, true),

  // DATABASE
              DB_POOL_MIN("db.pool.min", "Default database pool minimum size", Integer.class, 1),

  DB_POOL_MAX("db.pool.max", "Default database pool maximum size", Integer.class, 100),

  DB_POOL_IDLE_TIMEOUT("db.pool.idleTimeout", "Timeout for checking of free database in the pool", Integer.class, 0),

  DB_POOL_IDLE_CHECK_DELAY("db.pool.idleCheckDelay", "Delay time on checking for idle databases", Integer.class, 0),

  DB_MVCC_THROWFAST("db.mvcc.throwfast",
      "Use fast-thrown exceptions for MVCC OConcurrentModificationExceptions. No context information will be available, use where these exceptions are handled and the detail is not neccessary",
      Boolean.class, false, true),

  DB_VALIDATION("db.validation", "Enables or disables validation of records", Boolean.class, true, true),

  // SETTINGS OF NON-TRANSACTIONAL MODE
  NON_TX_RECORD_UPDATE_SYNCH("nonTX.recordUpdate.synch",
      "Executes a synch against the file-system at every record operation. This slows down records updates "
          + "but guarantee reliability on unreliable drives",
      Boolean.class, Boolean.FALSE),

  NON_TX_CLUSTERS_SYNC_IMMEDIATELY("nonTX.clusters.sync.immediately",
      "List of clusters to sync immediately after update separated by commas. Can be useful for manual index", String.class,
      OMetadataDefault.CLUSTER_MANUAL_INDEX_NAME),

  // TRANSACTIONS

  TX_TRACK_ATOMIC_OPERATIONS("tx.trackAtomicOperations",
      "This setting is used only for debug purpose, it track stac trace of methods where atomic operation is started.",
      Boolean.class, false),

  // INDEX
      INDEX_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD("index.embeddedToSbtreeBonsaiThreshold",
          "Amount of values after which index implementation will use sbtree as values container. Set to -1 to force always using it",
          Integer.class, 40),

  INDEX_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD("index.sbtreeBonsaiToEmbeddedThreshold",
      "Amount of values after which index implementation will use embedded values container (disabled by default)", Integer.class,
      -1),

  HASH_TABLE_SPLIT_BUCKETS_BUFFER_LENGTH("hashTable.slitBucketsBuffer.length",
      "Length of buffer (in pages) where buckets "
          + "that were splited but not flushed to the disk are kept. This buffer is used to minimize random IO overhead.",
      Integer.class, 1500),

  INDEX_SYNCHRONOUS_AUTO_REBUILD("index.auto.synchronousAutoRebuild",
      "Synchronous execution of auto rebuilding of indexes in case of db crash.", Boolean.class, Boolean.TRUE),

  INDEX_AUTO_LAZY_UPDATES("index.auto.lazyUpdates",
      "Configure the TreeMaps for automatic indexes as buffered or not. -1 means buffered until tx.commit() or db.close() are called",
      Integer.class, 10000),

  INDEX_FLUSH_AFTER_CREATE("index.flushAfterCreate", "Flush storage buffer after index creation", Boolean.class, true),

  INDEX_MANUAL_LAZY_UPDATES("index.manual.lazyUpdates",
      "Configure the TreeMaps for manual indexes as buffered or not. -1 means buffered until tx.commit() or db.close() are called",
      Integer.class, 1),

  INDEX_DURABLE_IN_NON_TX_MODE("index.durableInNonTxMode",
      "Indicates whether index implementation for plocal storage will be durable in non-Tx mode, false by default", Boolean.class,
      false),

  INDEX_TX_MODE("index.txMode",
      "Indicates index durability level in TX mode. Can be ROLLBACK_ONLY or FULL (ROLLBACK_ONLY by default)", String.class, "FULL"),

  INDEX_CURSOR_PREFETCH_SIZE("index.cursor.prefetchSize", "Default prefetch size of index cursor", Integer.class, 500000),

  // SBTREE
  SBTREE_MAX_DEPTH("sbtree.maxDepth",
      "Maximum depth of sbtree which will be traversed during key look up till it will be treated like broken (64 by default)",
      Integer.class, 64),

  SBTREE_MAX_KEY_SIZE("sbtree.maxKeySize", "Maximum size of key which can be put in SBTree in bytes (10240 by default)",
      Integer.class, 10240),

  SBTREE_MAX_EMBEDDED_VALUE_SIZE("sbtree.maxEmbeddedValueSize",
      "Maximum size of value which can be put in SBTree without creation link to standalone page in bytes (40960 by default)",
      Integer.class, 40960),

  SBTREEBONSAI_BUCKET_SIZE("sbtreebonsai.bucketSize",
      "Size of bucket in OSBTreeBonsai in kB. Contract: bucketSize < storagePageSize, storagePageSize % bucketSize == 0.",
      Integer.class, 2),

  SBTREEBONSAI_LINKBAG_CACHE_SIZE("sbtreebonsai.linkBagCache.size",
      "Amount of LINKBAG collections are cached to avoid constant reloading of data", Integer.class, 100000),

  SBTREEBONSAI_LINKBAG_CACHE_EVICTION_SIZE("sbtreebonsai.linkBagCache.evictionSize",
      "How many items of cached LINKBAG collections will be removed when cache limit is reached", Integer.class, 1000),

  SBTREEBOSAI_FREE_SPACE_REUSE_TRIGGER("sbtreebonsai.freeSpaceReuseTrigger",
      "How much free space should be in sbtreebonsai file before it will be reused during next allocation", Float.class, 0.5),

  // RIDBAG
      RID_BAG_EMBEDDED_DEFAULT_SIZE("ridBag.embeddedDefaultSize", "Size of embedded RidBag array when created (empty)",
          Integer.class, 4),

  RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD("ridBag.embeddedToSbtreeBonsaiThreshold",
      "Amount of values after which LINKBAG implementation will use sbtree as values container. Set to -1 to force always using it",
      Integer.class, 40, true),

  RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD("ridBag.sbtreeBonsaiToEmbeddedToThreshold",
      "Amount of values after which LINKBAG implementation will use embedded values container (disabled by default)", Integer.class,
      -1, true),

  // COLLECTIONS
      PREFER_SBTREE_SET("collections.preferSBTreeSet", "This config is experimental.", Boolean.class, false),

  // FILE
      TRACK_FILE_CLOSE("file.trackFileClose",
          "Log all the cases when files are closed. This is needed only for internal debugging purpose", Boolean.class, false),

  FILE_LOCK("file.lock", "Locks files when used. Default is true", boolean.class, true),

  FILE_DELETE_DELAY("file.deleteDelay", "Delay time in ms to wait for another attempt to delete a locked file", Integer.class, 10),

  FILE_DELETE_RETRY("file.deleteRetry", "Number of retries to delete a locked file", Integer.class, 50),

  JNA_DISABLE_USE_SYSTEM_LIBRARY("jna.disable.system.library",
      "This property disable to using JNA installed in your system. And use JNA bundled with database.", boolean.class, true),

  // NETWORK
      NETWORK_MAX_CONCURRENT_SESSIONS("network.maxConcurrentSessions", "Maximum number of concurrent sessions", Integer.class, 1000,
          true),

  NETWORK_SOCKET_BUFFER_SIZE("network.socketBufferSize", "TCP/IP Socket buffer size", Integer.class, 32768, true),

  NETWORK_LOCK_TIMEOUT("network.lockTimeout", "Timeout in ms to acquire a lock against a channel", Integer.class, 15000, true),

  NETWORK_SOCKET_TIMEOUT("network.socketTimeout", "TCP/IP Socket timeout in ms", Integer.class, 15000, true),

  NETWORK_REQUEST_TIMEOUT("network.requestTimeout", "Request completion timeout in ms ", Integer.class, 3600000 /* one hour */,
      true),

  NETWORK_SOCKET_RETRY("network.retry", "Number of times the client retries its connection to the server on failure", Integer.class,
      5, true),

  NETWORK_SOCKET_RETRY_DELAY("network.retryDelay", "Number of ms the client waits before reconnecting to the server on failure",
      Integer.class, 500, true),

  NETWORK_BINARY_DNS_LOADBALANCING_ENABLED("network.binary.loadBalancing.enabled",
      "Asks for DNS TXT record to determine if load balancing is supported", Boolean.class, Boolean.FALSE, true),

  NETWORK_BINARY_DNS_LOADBALANCING_TIMEOUT("network.binary.loadBalancing.timeout",
      "Maximum time (in ms) to wait for the answer from DNS about the TXT record for load balancing", Integer.class, 2000, true),

  NETWORK_BINARY_MAX_CONTENT_LENGTH("network.binary.maxLength", "TCP/IP max content length in bytes of BINARY requests",
      Integer.class, 32736, true),

  NETWORK_BINARY_READ_RESPONSE_MAX_TIMES("network.binary.readResponse.maxTimes",
      "Maximum times to wait until response will be read. Otherwise response will be dropped from chanel", Integer.class, 20, true),

  NETWORK_BINARY_DEBUG("network.binary.debug", "Debug mode: print all data incoming on the binary channel", Boolean.class, false,
      true),

  NETWORK_HTTP_MAX_CONTENT_LENGTH("network.http.maxLength", "TCP/IP max content length in bytes for HTTP requests", Integer.class,
      1000000, true),

  NETWORK_HTTP_CONTENT_CHARSET("network.http.charset", "Http response charset", String.class, "utf-8", true),

  NETWORK_HTTP_JSON_RESPONSE_ERROR("network.http.jsonResponseError", "Http response error in json", Boolean.class, true, true),

  NETWORK_HTTP_JSONP_ENABLED("network.http.jsonp",
      "Enable the usage of JSONP if requested by the client. The parameter name to use is 'callback'", Boolean.class, false, true),

  @Deprecated OAUTH2_SECRETKEY("oauth2.secretkey", "Http OAuth2 secret key", String.class, ""),

  NETWORK_HTTP_SESSION_EXPIRE_TIMEOUT("network.http.sessionExpireTimeout",
      "Timeout after which an http session is considered tp have expired (seconds)", Integer.class, 300),

  NETWORK_HTTP_USE_TOKEN("network.http.useToken", "Enable Token based sessions for http", Boolean.class, false),

  NETWORK_TOKEN_SECRETKEY("network.token.secretyKey", "Network token sercret key", String.class, ""),

  NETWORK_TOKEN_ENCRIPTION_ALGORITHM("network.token.encriptionAlgorithm", "Network token algorithm", String.class, "HmacSHA256"),

  NETWORK_TOKEN_EXPIRE_TIMEOUT("network.token.expireTimeout",
      "Timeout after which an binary session is considered tp have expired (minutes)", Integer.class, 60),

  // PROFILER
      PROFILER_ENABLED("profiler.enabled", "Enable the recording of statistics and counters", Boolean.class, false,
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

  PROFILER_AUTODUMP_INTERVAL("profiler.autoDump.interval",
      "Dumps the profiler values at regular intervals. Time is expressed in seconds", Integer.class, 0,
      new OConfigurationChangeCallback() {
        public void change(final Object iCurrentValue, final Object iNewValue) {
          Orient.instance().getProfiler().setAutoDump((Integer) iNewValue);
        }
      }),

  PROFILER_MAXVALUES("profiler.maxValues", "Maximum values to store. Values are managed in a LRU", Integer.class, 200),

  // LOG
  LOG_CONSOLE_LEVEL("log.console.level", "Console logging level", String.class, "info", new OConfigurationChangeCallback() {
    public void change(final Object iCurrentValue, final Object iNewValue) {
      OLogManager.instance().setLevel((String) iNewValue, ConsoleHandler.class);
    }
  }),

  LOG_FILE_LEVEL("log.file.level", "File logging level", String.class, "fine", new OConfigurationChangeCallback() {
    public void change(final Object iCurrentValue, final Object iNewValue) {
      OLogManager.instance().setLevel((String) iNewValue, FileHandler.class);
    }
  }),

  // COMMAND
  COMMAND_TIMEOUT("command.timeout", "Default timeout for commands expressed in milliseconds", Long.class, 0, true),

  // QUERY
  QUERY_SCAN_THRESHOLD_TIP("query.scanThresholdTip",
      "If total number of records scanned in a query is major than this threshold a warning is given. Use 0 to disable it",
      Long.class, 50000),

  QUERY_LIMIT_THRESHOLD_TIP("query.limitThresholdTip",
      "If total number of returned records in a query is major than this threshold a warning is given. Use 0 to disable it",
      Long.class, 10000),

  // GRAPH
      SQL_GRAPH_CONSISTENCY_MODE("sql.graphConsistencyMode",
          "Consistency mode for graphs. It can be 'tx' (default), 'notx_sync_repair' and 'notx_async_repair'. "
              + "'tx' uses transactions to maintain consistency. Instead both 'notx_sync_repair' and 'notx_async_repair' do not use transactions, "
              + "and the consistency, in case of JVM crash, is guaranteed by a database repair operation that run at startup. "
              + "With 'notx_sync_repair' the repair is synchronous, so the database comes online after the repair is ended, while "
              + "with 'notx_async_repair' the repair is a background process",
          String.class, "tx"),

  /**
   * Maximum size of pool of network channels between client and server. A channel is a TCP/IP connection.
   */
  CLIENT_CHANNEL_MAX_POOL("client.channel.maxPool",
      "Maximum size of pool of network channels between client and server. A channel is a TCP/IP connection.", Integer.class, 100),

  /**
   * Maximum time which client should wait a connection from the pool when all connection are used.
   */
  CLIENT_CONNECT_POOL_WAIT_TIMEOUT("client.connectionPool.waitTimeout",
      "Maximum time which client should wait a connection from the pool when all connection are used", Integer.class, 5000, true),

  CLIENT_DB_RELEASE_WAIT_TIMEOUT("client.channel.dbReleaseWaitTimeout",
      "Delay in ms. after which data modification command will be resent if DB was frozen", Integer.class, 10000, true),

  CLIENT_USE_SSL("client.ssl.enabled", "Use SSL for client connections", Boolean.class, false),

  CLIENT_SSL_KEYSTORE("client.ssl.keyStore", "Use SSL for client connections", String.class, null),

  CLIENT_SSL_KEYSTORE_PASSWORD("client.ssl.keyStorePass", "Use SSL for client connections", String.class, null),

  CLIENT_SSL_TRUSTSTORE("client.ssl.trustStore", "Use SSL for client connections", String.class, null),

  CLIENT_SSL_TRUSTSTORE_PASSWORD("client.ssl.trustStorePass", "Use SSL for client connections", String.class, null),

  CLIENT_SESSION_TOKEN_BASED("client.session.tokenBased", "Request a token based session to the server", Boolean.class, false),

  // SERVER
  SERVER_CHANNEL_CLEAN_DELAY("server.channel.cleanDelay", "Time in ms of delay to check pending closed connections", Integer.class,
      5000),

  SERVER_CACHE_FILE_STATIC("server.cache.staticFile", "Cache static resources loading", Boolean.class, false),

  SERVER_LOG_DUMP_CLIENT_EXCEPTION_LEVEL("server.log.dumpClientExceptionLevel",
      "Logs client exceptions. Use any level supported by Java java.util.logging.Level class: OFF, FINE, CONFIG, INFO, WARNING, SEVERE",
      Level.class, Level.FINE),

  SERVER_LOG_DUMP_CLIENT_EXCEPTION_FULLSTACKTRACE("server.log.dumpClientExceptionFullStackTrace",
      "Dumps the full stack trace of the exception to sent to the client", Boolean.class, Boolean.FALSE, true),

  // DISTRIBUTED
      DISTRIBUTED_CRUD_TASK_SYNCH_TIMEOUT("distributed.crudTaskTimeout",
          "Maximum timeout in milliseconds to wait for CRUD remote tasks", Long.class, 3000l, true),

  DISTRIBUTED_COMMAND_TASK_SYNCH_TIMEOUT("distributed.commandTaskTimeout",
      "Maximum timeout in milliseconds to wait for Command remote tasks", Long.class, 10000l, true),

  DISTRIBUTED_COMMAND_LONG_TASK_SYNCH_TIMEOUT("distributed.commandLongTaskTimeout",
      "Maximum timeout in milliseconds to wait for Long-running remote tasks", Long.class, 24 * 60 * 60 * 1000, true),

  DISTRIBUTED_DEPLOYDB_TASK_SYNCH_TIMEOUT("distributed.deployDbTaskTimeout",
      "Maximum timeout in milliseconds to wait for database deployment", Long.class, 1200000l, true),

  DISTRIBUTED_DEPLOYCHUNK_TASK_SYNCH_TIMEOUT("distributed.deployChunkTaskTimeout",
      "Maximum timeout in milliseconds to wait for database chunk deployment", Long.class, 15000l, true),

  DISTRIBUTED_DEPLOYDB_TASK_COMPRESSION("distributed.deployDbTaskCompression",
      "Compression level between 0 and 9 to use in backup for database deployment", Integer.class, 7, true),

  DISTRIBUTED_QUEUE_TIMEOUT("distributed.queueTimeout", "Maximum timeout in milliseconds to wait for the response in replication",
      Long.class, 5000l, true),

  DISTRIBUTED_ASYNCH_QUEUE_SIZE("distributed.asynchQueueSize",
      "Queue size to handle distributed asynchronous operations. 0 = dynamic allocation (up to 2^31-1 entries)", Integer.class, 0),

  DISTRIBUTED_ASYNCH_RESPONSES_TIMEOUT("distributed.asynchResponsesTimeout",
      "Maximum timeout in milliseconds to collect all the asynchronous responses from replication", Long.class, 15000l),

  DISTRIBUTED_PURGE_RESPONSES_TIMER_DELAY("distributed.purgeResponsesTimerDelay",
      "Maximum timeout in milliseconds to collect all the asynchronous responses from replication", Long.class, 15000l),

  /**
   * @Since 2.1.3
   */
  @OApi(maturity = OApi.MATURITY.NEW) DISTRIBUTED_QUEUE_MAXSIZE("distributed.queueMaxSize",
      "Maximum queue size to mark a node as stalled. If the number of messages in queue are more than this values, the node is restarted with a remote command (0 = no maximum, which means up to 2^31-1 entries).",
      Integer.class, 10000),

  /**
   * @Since 2.1.3
   */
  @OApi(maturity = OApi.MATURITY.NEW) DISTRIBUTED_BACKUP_DIRECTORY("distributed.backupDirectory",
      "Directory where to copy an existent database before to download from the cluster", String.class, "../backup/databases"),

  /**
   * @Since 2.1
   */
  @OApi(maturity = OApi.MATURITY.NEW) DISTRIBUTED_CONCURRENT_TX_MAX_AUTORETRY("distributed.concurrentTxMaxAutoRetry",
      "Maximum retries the transaction coordinator can execute a transaction automatically if records are locked. Minimum is 1 (no retry)",
      Integer.class, 10, true),

  /**
   * @Since 2.1
   */
  @OApi(maturity = OApi.MATURITY.NEW) DISTRIBUTED_CONCURRENT_TX_AUTORETRY_DELAY("distributed.concurrentTxAutoRetryDelay",
      "Delay in ms between attempts on executing a distributed transaction failed because of records locked. 0=no delay",
      Integer.class, 100, true),

  DB_MAKE_FULL_CHECKPOINT_ON_INDEX_CHANGE("db.makeFullCheckpointOnIndexChange",
      "When index metadata is changed full checkpoint is performed", Boolean.class, true, true),

  DB_MAKE_FULL_CHECKPOINT_ON_SCHEMA_CHANGE("db.makeFullCheckpointOnSchemaChange",
      "When index schema is changed full checkpoint is performed", Boolean.class, true, true),

  DB_DOCUMENT_SERIALIZER("db.document.serializer", "The default record serializer used by the document database", String.class,
      ORecordSerializerBinary.NAME),

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

  // TREEMAP OF RIDS
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

  @Deprecated
  // DEPRECATED IN 2.0
  STORAGE_KEEP_OPEN("storage.keepOpen", "Deprecated", Boolean.class, Boolean.TRUE),

  // DEPRECATED IN 2.0, LEVEL1 CACHE CANNOT BE DISABLED ANYMORE
  @Deprecated CACHE_LOCAL_ENABLED("cache.local.enabled", "Deprecated, Level1 cache cannot be disabled anymore", Boolean.class,
      true);

  private final String key;
  private final Object defValue;
  private final Class<?> type;
  private volatile Object value = null;
  private final String description;
  private final OConfigurationChangeCallback changeCallback;
  private final Boolean canChangeAtRuntime;

  // AT STARTUP AUTO-CONFIG
  static {
    readConfiguration();
    autoConfig();
  }

  OGlobalConfiguration(final String iKey, final String iDescription, final Class<?> iType, final Object iDefValue,
      final OConfigurationChangeCallback iChangeAction) {
    key = iKey;
    description = iDescription;
    defValue = iDefValue;
    type = iType;
    canChangeAtRuntime = true;
    changeCallback = iChangeAction;
  }

  OGlobalConfiguration(final String iKey, final String iDescription, final Class<?> iType, final Object iDefValue) {
    this(iKey, iDescription, iType, iDefValue, false);
  }

  OGlobalConfiguration(final String iKey, final String iDescription, final Class<?> iType, final Object iDefValue,
      final Boolean iCanChange) {
    key = iKey;
    description = iDescription;
    defValue = iDefValue;
    type = iType;
    canChangeAtRuntime = iCanChange;
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
      out.println(v.getValue());
    }
  }

  /**
   * Find the OGlobalConfiguration instance by the key. Key is case insensitive.
   *
   * @param iKey
   *          Key to find. It's case insensitive.
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

  private static void autoConfig() {
    final long freeSpaceInMB = new File(".").getFreeSpace() / 1024 / 1024;

    if (System.getProperty(DISK_CACHE_SIZE.key) == null)
      autoConfigDiskCacheSize(freeSpaceInMB);

    if (System.getProperty(WAL_RESTORE_BATCH_SIZE.key) == null) {
      final long jvmMaxMemory = Runtime.getRuntime().maxMemory();
      if (jvmMaxMemory > 2 * OFileUtils.GIGABYTE)
        // INCREASE WAL RESTORE BATCH SIZE TO 50K INSTEAD OF DEFAULT 1K
        WAL_RESTORE_BATCH_SIZE.setValue(50000);
      else
        if (jvmMaxMemory > 512 * OFileUtils.MEGABYTE)
          // INCREASE WAL RESTORE BATCH SIZE TO 10K INSTEAD OF DEFAULT 1K
          WAL_RESTORE_BATCH_SIZE.setValue(10000);
    }
  }

  private static void autoConfigDiskCacheSize(final long freeSpaceInMB) {
    final OperatingSystemMXBean mxBean = ManagementFactory.getOperatingSystemMXBean();
    try {
      final Method memorySize = mxBean.getClass().getDeclaredMethod("getTotalPhysicalMemorySize");
      memorySize.setAccessible(true);

      final long osMemory = (Long) memorySize.invoke(mxBean);
      final long jvmMaxMemory = Runtime.getRuntime().maxMemory();

      // DISK-CACHE IN MB = OS MEMORY - MAX HEAP JVM MEMORY - 2 GB
      long diskCacheInMB = (osMemory - jvmMaxMemory) / (1024 * 1024) - 2 * 1024;
      if (diskCacheInMB > 0) {

        // CHECK IF CANDIDATE DISK-CACHE IS BIGGER THAN 80% OF FREE SPACE IN DISK
        if (diskCacheInMB > freeSpaceInMB * 80 / 100)
          // LOW DISK SPACE: REDUCE DISK CACHE SIZE TO HALF SIZE OF FREE DISK SPACE
          diskCacheInMB = freeSpaceInMB * 50 / 100;

        OLogManager.instance().info(null, "OrientDB auto-config DISKCACHE=%,dMB (heap=%,dMB os=%,dMB disk=%,dMB)", diskCacheInMB,
            jvmMaxMemory / 1024 / 1024, osMemory / 1024 / 1024, freeSpaceInMB);

        DISK_CACHE_SIZE.setValue(diskCacheInMB);
      } else {
        // LOW MEMORY: SET IT TO 256MB ONLY
        OLogManager.instance().warn(null,
            "Not enough physical memory available for DISKCACHE: %,dMB (heap=%,dMB). Set lower Maximum Heap (-Xmx setting on JVM) and restart OrientDB. Now running with DISKCACHE="
                + O2QCache.MIN_CACHE_SIZE + "MB",
            osMemory / 1024 / 1024, jvmMaxMemory / 1024 / 1024);
        DISK_CACHE_SIZE.setValue(O2QCache.MIN_CACHE_SIZE);

        OLogManager.instance().info(null, "OrientDB config DISKCACHE=%,dMB (heap=%,dMB os=%,dMB disk=%,dMB)", diskCacheInMB,
            jvmMaxMemory / 1024 / 1024, osMemory / 1024 / 1024, freeSpaceInMB);
      }

    } catch (NoSuchMethodException e) {
    } catch (InvocationTargetException e) {
    } catch (IllegalAccessException e) {
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
    return v instanceof Boolean ? ((Boolean) v).booleanValue() : Boolean.parseBoolean(v.toString());
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
    return v instanceof Float ? ((Float) v).floatValue() : Float.parseFloat(v.toString());
  }

  public String getKey() {
    return key;
  }

  public Boolean isChangeableAtRuntime() {
    return canChangeAtRuntime;
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
