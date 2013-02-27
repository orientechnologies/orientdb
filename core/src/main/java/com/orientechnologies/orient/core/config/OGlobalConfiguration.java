/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.core.config;

import java.io.PrintStream;
import java.lang.management.OperatingSystemMXBean;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.ConsoleHandler;
import java.util.logging.FileHandler;
import java.util.logging.Level;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.OConstants;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.cache.ODefaultCache;
import com.orientechnologies.orient.core.storage.fs.OMMapManagerOld;

/**
 * Keeps all configuration settings. At startup assigns the configuration values by reading system properties.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public enum OGlobalConfiguration {
  // ENVIRONMENT
  ENVIRONMENT_DUMP_CFG_AT_STARTUP("environment.dumpCfgAtStartup", "Dumps the configuration at application startup", Boolean.class,
      Boolean.FALSE),

  ENVIRONMENT_CONCURRENT("environment.concurrent",
      "Specifies if running in multi-thread environment. Setting this to false turns off the internal lock management",
      Boolean.class, Boolean.TRUE),

  // MEMORY
  @Deprecated
  MEMORY_OPTIMIZE_THRESHOLD("memory.optimizeThreshold", "Threshold for heap memory at which optimization of memory usage starts. ",
      Float.class, 0.70),

  GC_DELAY_FOR_OPTIMIZE("gc.delay.forOptimize", "Minimal amount of time (seconds) since last System.gc() when called after tree optimization", Long.class, 3600),

  // STORAGE
  USE_NODE_ID_CLUSTER_POSITION("storage.cluster.useNodeIdAsClusterPosition", "Indicates whether cluster position should be"
      + " treated as node id not as long value.", Boolean.class, Boolean.FALSE),

  STORAGE_KEEP_OPEN(
      "storage.keepOpen",
      "Tells to the engine to not close the storage when a database is closed. Storages will be closed when the process shuts down",
      Boolean.class, Boolean.FALSE),

  STORAGE_LOCK_TIMEOUT("storage.lockTimeout", "Maximum timeout in milliseconds to lock the storage", Integer.class, 5000),

  STORAGE_RECORD_LOCK_TIMEOUT("storage.record.lockTimeout", "Maximum timeout in milliseconds to lock a shared record",
      Integer.class, 5000),

  STORAGE_USE_TOMBSTONES("storage.useTombstones", "When record will be deleted its cluster"
      + " position will not be freed but tombstone will be placed instead", Boolean.class, false),

  // CACHE
  CACHE_LEVEL1_ENABLED("cache.level1.enabled", "Use the level-1 cache", Boolean.class, true),

  CACHE_LEVEL1_SIZE("cache.level1.size", "Size of the cache that keeps the record in memory", Integer.class, -1),

  CACHE_LEVEL2_ENABLED("cache.level2.enabled", "Use the level-2 cache", Boolean.class, true),

  CACHE_LEVEL2_SIZE("cache.level2.size", "Size of the cache that keeps the record in memory", Integer.class, -1),

  CACHE_LEVEL2_IMPL("cache.level2.impl", "Actual implementation of secondary cache", String.class, ODefaultCache.class
      .getCanonicalName()),

  CACHE_LEVEL2_STRATEGY("cache.level2.strategy",
      "Strategy to use when a database requests a record: 0 = pop the record, 1 = copy the record", Integer.class, 0,
      new OConfigurationChangeCallback() {
        public void change(final Object iCurrentValue, final Object iNewValue) {
          // UPDATE ALL THE OPENED STORAGES SETTING THE NEW STRATEGY
          // for (OStorage s : com.orientechnologies.orient.core.Orient.instance().getStorages()) {
          // s.getCache().setStrategy((Integer) iNewValue);
          // }
        }
      }),

  // DATABASE
  OBJECT_SAVE_ONLY_DIRTY("object.saveOnlyDirty", "Object Database only saves objects bound to dirty records", Boolean.class, false),

  DB_MVCC("db.mvcc", "Enables or disables MVCC (Multi-Version Concurrency Control) even outside transactions", Boolean.class, true),

  DB_MVCC_THROWFAST(
      "db.mvcc.throwfast",
      "Use fast-thrown exceptions for MVCC OConcurrentModificationExceptions. No context information will be available, use where these exceptions are handled and the detail is not neccessary",
      Boolean.class, false),

  DB_VALIDATION("db.validation", "Enables or disables validation of records", Boolean.class, true),

  DB_USE_DISTRIBUTED_VERSION("db.use.distributedVersion", "Use extended version that is safe in distributed environment",
      Boolean.class, Boolean.FALSE),

  // SETTINGS OF NON-TRANSACTIONAL MODE
  NON_TX_RECORD_UPDATE_SYNCH("nonTX.recordUpdate.synch",
      "Executes a synch against the file-system at every record operation. This slows down records updates "
          + "but guarantee reliability on unreliable drives", Boolean.class, Boolean.FALSE),

  NON_TX_CLUSTERS_SYNC_IMMEDIATELY("nonTX.clusters.sync.immediately",
      "List of clusters to sync immediately after update separated by commas. Can be useful for manual index", String.class,
      "manindex"),

  // TRANSACTIONS
  TX_USE_LOG("tx.useLog", "Transactions use log file to store temporary data to be rolled back in case of crash", Boolean.class,
      true),

  TX_LOG_TYPE("tx.log.fileType", "File type to handle transaction logs: mmap or classic", String.class, "classic"),

  TX_LOG_SYNCH(
      "tx.log.synch",
      "Executes a synch against the file-system at every log entry. This slows down transactions but guarantee transaction reliability on unreliable drives",
      Boolean.class, Boolean.FALSE),

  TX_COMMIT_SYNCH("tx.commit.synch", "Synchronizes the storage after transaction commit", Boolean.class, false),

  // GRAPH
  @Deprecated
  BLUEPRINTS_TX_MODE("blueprints.graph.txMode",
      "Transaction mode used in TinkerPop Blueprints implementation. 0 = Automatic (default), 1 = Manual", Integer.class, 0),

  // INDEX
  INDEX_AUTO_REBUILD_AFTER_NOTSOFTCLOSE("index.auto.rebuildAfterNotSoftClose",
      "Auto rebuild all automatic indexes after upon database open when wasn't closed properly", Boolean.class, true),

  INDEX_AUTO_LAZY_UPDATES(
      "index.auto.lazyUpdates",
      "Configure the TreeMaps for automatic indexes as buffered or not. -1 means buffered until tx.commit() or db.close() are called",
      Integer.class, 10000),

  INDEX_MANUAL_LAZY_UPDATES("index.manual.lazyUpdates",
      "Configure the TreeMaps for manual indexes as buffered or not. -1 means buffered until tx.commit() or db.close() are called",
      Integer.class, 1),

  // TREEMAP
  MVRBTREE_TIMEOUT("mvrbtree.timeout", "Maximum timeout to get lock against the OMVRB-Tree", Integer.class, 5000),

  MVRBTREE_NODE_PAGE_SIZE("mvrbtree.nodePageSize",
      "Page size of each node. 256 means that 256 entries can be stored inside each node", Integer.class, 256),

  MVRBTREE_LOAD_FACTOR("mvrbtree.loadFactor", "HashMap load factor", Float.class, 0.7f),

  MVRBTREE_OPTIMIZE_THRESHOLD(
      "mvrbtree.optimizeThreshold",
      "Auto optimize the TreeMap every X tree rotations. This forces the optimization of the tree after many changes to recompute entry points. -1 means never",
      Integer.class, 100000),

  MVRBTREE_ENTRYPOINTS("mvrbtree.entryPoints", "Number of entry points to start searching entries", Integer.class, 64),

  MVRBTREE_OPTIMIZE_ENTRYPOINTS_FACTOR("mvrbtree.optimizeEntryPointsFactor",
      "Multiplicand factor to apply to entry-points list (parameter mvrbtree.entrypoints) to determine optimization is needed",
      Float.class, 1.0f),

  MVRBTREE_ENTRY_KEYS_IN_MEMORY("mvrbtree.entryKeysInMemory", "Keep unserialized keys in memory", Boolean.class, Boolean.FALSE),

  MVRBTREE_ENTRY_VALUES_IN_MEMORY("mvrbtree.entryValuesInMemory", "Keep unserialized values in memory", Boolean.class,
      Boolean.FALSE),

  // TREEMAP OF RIDS
  MVRBTREE_RID_BINARY_THRESHOLD(
      "mvrbtree.ridBinaryThreshold",
      "Valid for set of rids. It's the threshold as number of entries to use the binary streaming instead of classic string streaming. -1 means never use binary streaming",
      Integer.class, 8),

  MVRBTREE_RID_NODE_PAGE_SIZE("mvrbtree.ridNodePageSize",
      "Page size of each treeset node. 16 means that 16 entries can be stored inside each node", Integer.class, 16),

  MVRBTREE_RID_NODE_SAVE_MEMORY("mvrbtree.ridNodeSaveMemory",
      "Save memory usage by avoid keeping RIDs in memory but creating them at every access", Boolean.class, Boolean.FALSE),

  // COLLECTIONS
  LAZYSET_WORK_ON_STREAM("lazyset.workOnStream", "Upon add avoid unmarshalling set", Boolean.class, true),

  // FILE
  FILE_LOCK("file.lock", "Locks files when used. Default is false", boolean.class, false),

  FILE_DEFRAG_STRATEGY("file.defrag.strategy", "Strategy to recycle free space: 0 = synchronous defrag, 1 = asynchronous defrag, ",
      Integer.class, 0),

  FILE_DEFRAG_HOLE_MAX_DISTANCE(
      "file.defrag.holeMaxDistance",
      "Max distance in bytes between holes to cause their defrag. Set it to -1 to use dynamic size. Beware that if the db is huge moving blocks to defrag could be expensive",
      Integer.class, 32768),

  FILE_MMAP_USE_OLD_MANAGER("file.mmap.useOldManager",
      "Manager that will be used to handle mmap files. true = USE OLD MANAGER, false = USE NEW MANAGER", boolean.class, false),

  FILE_MMAP_AUTOFLUSH_TIMER("file.mmap.autoFlush.timer", "Auto flushes memory mapped blocks every X seconds. 0 = disabled",
      int.class, 0),

  FILE_MMAP_AUTOFLUSH_UNUSED_TIME("file.mmap.autoFlush.unusedTime",
      "Remove memory mapped blocks with unused time major than this value. Time is in seconds", int.class, 60),

  FILE_MMAP_LOCK_MEMORY("file.mmap.lockMemory",
      "When using new map manager this parameter specify prevent memory swap or not. true = LOCK MEMORY, false = NOT LOCK MEMORY",
      boolean.class, true),

  FILE_MMAP_STRATEGY(
      "file.mmap.strategy",
      "Strategy to use with memory mapped files. 0 = USE MMAP ALWAYS, 1 = USE MMAP ON WRITES OR ON READ JUST WHEN THE BLOCK POOL IS FREE, 2 = USE MMAP ON WRITES OR ON READ JUST WHEN THE BLOCK IS ALREADY AVAILABLE, 3 = USE MMAP ONLY IF BLOCK IS ALREADY AVAILABLE, 4 = NEVER USE MMAP",
      Integer.class, 0),

  FILE_MMAP_BLOCK_SIZE("file.mmap.blockSize", "Size of the memory mapped block, default is 1Mb", Integer.class, 1048576,
      new OConfigurationChangeCallback() {
        public void change(final Object iCurrentValue, final Object iNewValue) {
          OMMapManagerOld.setBlockSize(((Number) iNewValue).intValue());
        }
      }),

  FILE_MMAP_BUFFER_SIZE("file.mmap.bufferSize", "Size of the buffer for direct access to the file through the channel",
      Integer.class, 8192),

  FILE_MMAP_MAX_MEMORY(
      "file.mmap.maxMemory",
      "Max memory allocatable by memory mapping manager. Note that on 32bit operating systems, the limit is 2Gb but will vary between operating systems",
      Long.class, 134217728, new OConfigurationChangeCallback() {
        public void change(final Object iCurrentValue, final Object iNewValue) {
          OMMapManagerOld.setMaxMemory(OFileUtils.getSizeAsNumber(iNewValue));
        }
      }),

  FILE_MMAP_OVERLAP_STRATEGY(
      "file.mmap.overlapStrategy",
      "Strategy to use when a request overlaps in-memory buffers: 0 = Use the channel access, 1 = force the in-memory buffer and use the channel access, 2 = always create an overlapped in-memory buffer (default)",
      Integer.class, 2, new OConfigurationChangeCallback() {
        public void change(final Object iCurrentValue, final Object iNewValue) {
          OMMapManagerOld.setOverlapStrategy((Integer) iNewValue);
        }
      }),

  FILE_MMAP_FORCE_DELAY("file.mmap.forceDelay",
      "Delay time in ms to wait for another forced flush of the memory-mapped block to disk", Integer.class, 10),

  FILE_MMAP_FORCE_RETRY("file.mmap.forceRetry", "Number of times the memory-mapped block will try to flush to disk", Integer.class,
      50),

  JNA_DISABLE_USE_SYSTEM_LIBRARY("jna.disable.system.library",
      "This property disable to using JNA installed in your system. And use JNA bundled with database.", boolean.class, true),

  USE_LHPEPS_CLUSTER("file.cluster.useLHPEPS", "Indicates whether cluster file should be saved as simple persistent"
      + " list or as hash map. Persistent list is used by default.", Boolean.class, Boolean.FALSE),

  USE_LHPEPS_MEMORY_CLUSTER("file.cluster.useMemoryLHCluster",
      "Indicates whether cluster file should be saved as simple persistent"
          + " list or as hash map. Persistent list is used by default.", Boolean.class, Boolean.FALSE),

  // NETWORK
  NETWORK_SOCKET_BUFFER_SIZE("network.socketBufferSize", "TCP/IP Socket buffer size", Integer.class, 32768),

  NETWORK_LOCK_TIMEOUT("network.lockTimeout", "Timeout in ms to acquire a lock against a channel", Integer.class, 15000),

  NETWORK_SOCKET_TIMEOUT("network.socketTimeout", "TCP/IP Socket timeout in ms", Integer.class, 10000),

  NETWORK_SOCKET_RETRY("network.retry", "Number of times the client retries its connection to the server on failure",
      Integer.class, 5),

  NETWORK_SOCKET_RETRY_DELAY("network.retryDelay", "Number of ms the client waits before reconnecting to the server on failure",
      Integer.class, 500),

  NETWORK_BINARY_DNS_LOADBALANCING_ENABLED("network.binary.loadBalancing.enabled",
      "Asks for DNS TXT record to determine if load balancing is supported", Boolean.class, Boolean.FALSE),

  NETWORK_BINARY_DNS_LOADBALANCING_TIMEOUT("network.binary.loadBalancing.timeout",
      "Maximum time (in ms) to wait for the answer from DNS about the TXT record for load balancing", Integer.class, 2000),

  NETWORK_BINARY_MAX_CONTENT_LENGTH("network.binary.maxLength", "TCP/IP max content length in bytes of BINARY requests",
      Integer.class, 32736),

  NETWORK_BINARY_READ_RESPONSE_MAX_TIMES("network.binary.readResponse.maxTimes",
      "Maximum times to wait until response will be read. Otherwise response will be dropped from chanel", Integer.class, 20),

  NETWORK_BINARY_DEBUG("network.binary.debug", "Debug mode: print all data incoming on the binary channel", Boolean.class, false),

  NETWORK_HTTP_MAX_CONTENT_LENGTH("network.http.maxLength", "TCP/IP max content length in bytes for HTTP requests", Integer.class,
      1000000),

  NETWORK_HTTP_CONTENT_CHARSET("network.http.charset", "Http response charset", String.class, "utf-8"),

  NETWORK_HTTP_SESSION_EXPIRE_TIMEOUT("network.http.sessionExpireTimeout",
      "Timeout after which an http session is considered tp have expired (seconds)", Integer.class, 300),

  // PROFILER
  PROFILER_ENABLED("profiler.enabled", "Enable the recording of statistics and counters", Boolean.class, false,
      new OConfigurationChangeCallback() {
        public void change(final Object iCurrentValue, final Object iNewValue) {
          if ((Boolean) iNewValue)
            Orient.instance().getProfiler().startRecording();
          else
            Orient.instance().getProfiler().stopRecording();
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

  @Deprecated
  PROFILER_AUTODUMP_RESET("profiler.autoDump.reset", "Resets the profiler at every auto dump", Boolean.class, true),

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

  // CLIENT
  CLIENT_CHANNEL_MIN_POOL("client.channel.minPool", "Minimum pool size", Integer.class, 1),

  CLIENT_CHANNEL_MAX_POOL("client.channel.maxPool", "Maximum channel pool size", Integer.class, 5),

  CLIENT_CONNECT_POOL_WAIT_TIMEOUT("client.connectionPool.waitTimeout",
      "Maximum time which client should wait connection from the pool", Integer.class, 5000),

  CLIENT_DB_RELEASE_WAIT_TIMEOUT("client.channel.dbReleaseWaitTimeout",
      "Delay in ms. after which data modification command will be resent if DB was frozen", Integer.class, 10000),

  // SERVER
  SERVER_CHANNEL_CLEAN_DELAY("server.channel.cleanDelay", "Time in ms of delay to check pending closed connections", Integer.class,
      5000),

  SERVER_CACHE_FILE_STATIC("server.cache.staticFile", "Cache static resources loading", Boolean.class, false),

  SERVER_LOG_DUMP_CLIENT_EXCEPTION_LEVEL(
      "server.log.dumpClientExceptionLevel",
      "Logs client exceptions. Use any level supported by Java java.util.logging.Level class: OFF, FINE, CONFIG, INFO, WARNING, SEVERE",
      Level.class, Level.FINE),

  SERVER_LOG_DUMP_CLIENT_EXCEPTION_FULLSTACKTRACE("server.log.dumpClientExceptionFullStackTrace",
      "Dumps the full stack trace of the exception to sent to the client", Level.class, Boolean.TRUE);

  private final String                 key;
  private final Object                 defValue;
  private final Class<?>               type;
  private Object                       value          = null;
  private String                       description;
  private OConfigurationChangeCallback changeCallback = null;

  // AT STARTUP AUTO-CONFIG
  static {
    readConfiguration();
    autoConfig();
  }

  OGlobalConfiguration(final String iKey, final String iDescription, final Class<?> iType, final Object iDefValue,
      final OConfigurationChangeCallback iChangeAction) {
    this(iKey, iDescription, iType, iDefValue);
    changeCallback = iChangeAction;
  }

  OGlobalConfiguration(final String iKey, final String iDescription, final Class<?> iType, final Object iDefValue) {
    key = iKey;
    description = iDescription;
    defValue = iDefValue;
    type = iType;
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

    if (changeCallback != null)
      changeCallback.change(oldValue, value);
  }

  public Object getValue() {
    return value != null ? value : defValue;
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
    return v instanceof Float ? ((Float) v).floatValue() : OFileUtils.getSizeAsNumber(v.toString());
  }

  public String getKey() {
    return key;
  }

  public Class<?> getType() {
    return type;
  }

  public String getDescription() {
    return description;
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
    if (System.getProperty("os.arch").indexOf("64") > -1) {
      // 64 BIT

      if (FILE_MMAP_MAX_MEMORY.getValueAsInteger() == 134217728) {
        final OperatingSystemMXBean bean = java.lang.management.ManagementFactory.getOperatingSystemMXBean();

        try {
          final Class<?> cls = Class.forName("com.sun.management.OperatingSystemMXBean");
          if (cls.isAssignableFrom(bean.getClass())) {
            final Long maxOsMemory = (Long) cls.getMethod("getTotalPhysicalMemorySize", new Class[] {}).invoke(bean);
            final long maxProcessMemory = Runtime.getRuntime().maxMemory();
            long mmapBestMemory = (maxOsMemory.longValue() - maxProcessMemory) / 2;
            FILE_MMAP_MAX_MEMORY.setValue(mmapBestMemory);
          }
        } catch (Exception e) {
          // SUN JMX CLASS NOT AVAILABLE: CAN'T AUTO TUNE THE ENGINE
        }
      }
    } else {
      // 32 BIT, USE THE DEFAULT CONFIGURATION
    }
  }
}
