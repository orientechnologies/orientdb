/*
 * Copyright 1999-2010 Luca Garulli (l.garulli--at--orientechnologies.com)
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

import java.lang.management.OperatingSystemMXBean;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.ConsoleHandler;
import java.util.logging.FileHandler;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.orient.core.memory.OMemoryWatchDog;
import com.orientechnologies.orient.core.storage.fs.OMMapManager;

/**
 * Keeps all configuration settings. At startup assigns the configuration values by reading system properties.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public enum OGlobalConfiguration {
	// ENVIRONMENT
	ENVIRONMENT_CONCURRENT("environment.concurrent",
			"Specifies if running in multi-thread environment. Setting this to false turns off the internal lock management",
			Boolean.class, Boolean.TRUE),

	// MEMORY
	MEMORY_OPTIMIZE_THRESHOLD("memory.optimizeThreshold", "Threshold for heap memory at which optimization of memory usage starts. ",
			Float.class, 0.70, new OConfigurationChangeCallback() {
				public void change(final Object iCurrentValue, final Object iNewValue) {
					OMemoryWatchDog.setPercentageUsageThreshold(((Number) iNewValue).floatValue());
				}
			}),

	// STORAGE
	STORAGE_KEEP_OPEN(
			"storage.keepOpen",
			"Tells to the engine to not close the storage when a database is closed. Storages will be closed when the process shuts down",
			Boolean.class, Boolean.TRUE),

	STORAGE_RECORD_LOCK_TIMEOUT("storage.record.lockTimeout", "Maximum timeout in milliseconds to lock a shared record",
			Integer.class, 5000),

	// CACHE
	CACHE_LEVEL1_ENABLED("cache.level1.enabled", "Use the level-1 cache", Boolean.class, true),

	CACHE_LEVEL1_SIZE("cache.level1.size", "Size of the cache that keeps the record in memory", Integer.class, -1),

	CACHE_LEVEL2_ENABLED("cache.level2.enabled", "Use the level-2 cache", Boolean.class, true),

	CACHE_LEVEL2_SIZE("cache.level2.size", "Size of the cache that keeps the record in memory", Integer.class, -1),

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

	DB_VALIDATION("db.validation", "Enables or disables validation of records", Boolean.class, true),

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
	BLUEPRINTS_TX_MODE("blueprints.graph.txMode",
			"Transaction mode used in TinkerPop Blueprints implementation. 0 = Automatic (default), 1 = Manual", Integer.class, 0),

	// TREEMAP
	MVRBTREE_TIMEOUT("mvrbtree.timeout", "Maximum timeout to get lock against the OMVRB-Tree", Integer.class, 1000),

	MVRBTREE_LAZY_UPDATES(
			"mvrbtree.lazyUpdates",
			"Configure the TreeMaps (indexes and dictionaries) as buffered or not. -1 means buffered until tx.commit() or db.close() are called",
			Integer.class, 20000),

	MVRBTREE_NODE_PAGE_SIZE("mvrbtree.nodePageSize",
			"Page size of each node. 512 means that 512 entries can be stored inside each node", Integer.class, 512),

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

	MVRBTREE_SET_BINARY_THRESHOLD("mvrbtree.setBinaryThreshold",
			"The threshold as number of entries to use the binary streaming instead of classic string streaming", Integer.class, 8),

	// COLLECTIONS
	LAZYSET_WORK_ON_STREAM("lazyset.workOnStream", "Upon add avoid unmarshalling set", Boolean.class, true),

	// FILE
	FILE_LOCK("file.lock", "Locks files when used. Default is false for local connections and true when running as Server",
			boolean.class, true),

	FILE_DEFRAG_STRATEGY(
			"file.defrag.strategy",
			"Strategy to recycle free space. 0 = Recycles the first hole with enough size: fast, 1 = recycles the best hole: better usage of space but slower",
			Integer.class, 0),

	FILE_DEFRAG_HOLE_MAX_DISTANCE(
			"file.defrag.holeMaxDistance",
			"Max distance in bytes between holes to cause their defrag. Set it to -1 to use dynamic size. Beware that if the db is huge moving blocks to defrag could be expensive",
			Integer.class, 32768),

	FILE_MMAP_STRATEGY(
			"file.mmap.strategy",
			"Strategy to use with memory mapped files. 0 = USE MMAP ALWAYS, 1 = USE MMAP ON WRITES OR ON READ JUST WHEN THE BLOCK POOL IS FREE, 2 = USE MMAP ON WRITES OR ON READ JUST WHEN THE BLOCK IS ALREADY AVAILABLE, 3 = USE MMAP ONLY IF BLOCK IS ALREADY AVAILABLE, 4 = NEVER USE MMAP",
			Integer.class, 0),

	FILE_MMAP_BLOCK_SIZE("file.mmap.blockSize", "Size of the memory mapped block, default is 1Mb", Integer.class, 1048576,
			new OConfigurationChangeCallback() {
				public void change(final Object iCurrentValue, final Object iNewValue) {
					OMMapManager.setBlockSize(((Number) iNewValue).intValue());
				}
			}),

	FILE_MMAP_BUFFER_SIZE("file.mmap.bufferSize", "Size of the buffer for direct access to the file through the channel",
			Integer.class, 8192),

	FILE_MMAP_MAX_MEMORY(
			"file.mmap.maxMemory",
			"Max memory allocatable by memory mapping manager. Note that on 32bit operating systems, the limit is 2Gb but will vary between operating systems.",
			Long.class, 134217728, new OConfigurationChangeCallback() {
				public void change(final Object iCurrentValue, final Object iNewValue) {
					OMMapManager.setMaxMemory(OFileUtils.getSizeAsNumber(iNewValue));
				}
			}),

	FILE_MMAP_OVERLAP_STRATEGY(
			"file.mmap.overlapStrategy",
			"Strategy to use when a request overlaps in-memory buffers: 0 = Use the channel access, 1 = force the in-memory buffer and use the channel access, 2 = always create an overlapped in-memory buffer (default)",
			Integer.class, 2, new OConfigurationChangeCallback() {
				public void change(final Object iCurrentValue, final Object iNewValue) {
					OMMapManager.setOverlapStrategy((Integer) iNewValue);
				}
			}),

	FILE_MMAP_FORCE_DELAY("file.mmap.forceDelay",
			"Delay time in ms to wait for another forced flush of the memory-mapped block to disk", Integer.class, 10),

	FILE_MMAP_FORCE_RETRY("file.mmap.forceRetry", "Number of times the memory-mapped block will try to flush to disk", Integer.class,
			50),

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
			Integer.class, 100000),

	NETWORK_BINARY_DEBUG("network.binary.debug", "Debug mode: print all data incoming on the binary channel", Boolean.class, false),

	NETWORK_HTTP_MAX_CONTENT_LENGTH("network.http.maxLength", "TCP/IP max content length in bytes for HTTP requests", Integer.class,
			100000),

	NETWORK_HTTP_CONTENT_CHARSET("network.http.charset", "Http response charset", String.class, "utf-8"),

	NETWORK_HTTP_SESSION_EXPIRE_TIMEOUT("network.http.sessionExpireTimeout",
			"Timeout after which an http session is considered tp have expired (seconds)", Integer.class, 300),

	// PROFILER
	PROFILER_ENABLED("profiler.enabled", "Enable the recording of statistics and counters", Boolean.class, false,
			new OConfigurationChangeCallback() {
				public void change(final Object iCurrentValue, final Object iNewValue) {
					if ((Boolean) iNewValue)
						OProfiler.getInstance().startRecording();
					else
						OProfiler.getInstance().stopRecording();
				}
			}),

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

	CLIENT_CHANNEL_MAX_POOL("client.channel.maxPool", "Maximum channel pool size", Integer.class, 1),

	// SERVER
	SERVER_CACHE_FILE_STATIC("server.cache.file.static", "Cache static resources loading", Boolean.class, false),

	// DISTRIBUTED SERVERS
	DISTRIBUTED_ASYNC_TIME_DELAY("distributed.async.timeDelay",
			"Delay time (in ms) for synchronization with slave nodes. 0 means early synchronization", Integer.class, 0),

	DISTRIBUTED_SYNC_MAXRECORDS_BUFFER("distributed.sync.maxRecordsBuffer",
			"Maximum number of records to buffer before transmitting to slave nodes", Integer.class, 100),

	DISTRIBUTED_LOG_TYPE("distributed.log.fileType", "File type to handle server logs: mmap or classic", String.class, "classic"),

	DISTRIBUTED_LOG_SYNCH(
			"distributed.log.synch",
			"Executes a synch against the file-system at every log entry. This slows down server operations, but guarantee transaction reliability on unreliable drives",
			Boolean.class, Boolean.FALSE);

	private final String									key;
	private final Object									defValue;
	private final Class<?>								type;
	private Object												value						= null;
	private String												description;
	private OConfigurationChangeCallback	changeCallback	= null;

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
		final Object v = value != null ? value : defValue;
		return v.toString();
	}

	public int getValueAsInteger() {
		final Object v = value != null ? value : defValue;
		return v instanceof Number ? ((Number) v).intValue() : Integer.parseInt(v.toString());
	}

	public long getValueAsLong() {
		final Object v = value != null ? value : defValue;
		return v instanceof Number ? ((Number) v).longValue() : Long.parseLong(v.toString());
	}

	public float getValueAsFloat() {
		final Object v = value != null ? value : defValue;
		return v instanceof Float ? ((Float) v).floatValue() : Float.parseFloat(v.toString());
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
	 * Change configuration values in one shot by passing a Map of values.
	 */
	public static void setConfiguration(final Map<String, Object> iConfig) {
		OGlobalConfiguration cfg;
		for (Entry<String, Object> config : iConfig.entrySet()) {
			cfg = valueOf(config.getKey());
			if (cfg != null)
				cfg.setValue(config.getValue());
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
		if (System.getProperty("os.name").indexOf("Windows") > -1) {
			// WINDOWS
		}

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
