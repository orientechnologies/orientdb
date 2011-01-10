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

import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.ConsoleHandler;
import java.util.logging.FileHandler;

import com.orientechnologies.common.log.OLogManager;

/**
 * Keeps all configuration settings. At startup assigns the configuration values by reading system properties.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public enum OGlobalConfiguration {
	// MEMORY
	MEMORY_OPTIMIZE_THRESHOLD("memory.optimizeThreshold", "Threshold of heap memory to start optimizing memory usage", Float.class,
			0.85),

	// LOG
	LOG_CONSOLE_LEVEL("log.console.level", "Console's logging level", String.class, "info", new OConfigurationChangeCallback() {
		public void change(final Object iCurrentValue, final Object iNewValue) {
			OLogManager.instance().setLevel((String) iNewValue, ConsoleHandler.class);
		}
	}), LOG_FILE_LEVEL("log.file.level", "File's logging level", String.class, "fine", new OConfigurationChangeCallback() {
		public void change(final Object iCurrentValue, final Object iNewValue) {
			OLogManager.instance().setLevel((String) iNewValue, FileHandler.class);
		}
	}),

	// STORAGE
	STORAGE_KEEP_OPEN(
			"storage.keepOpen",
			"Tells to the engine to not close the storage when a database is closed. Storages will be closed when the process will shutdown",
			Boolean.class, Boolean.FALSE),

	STORAGE_CACHE_SIZE("storage.cache.size", "Size of the cache that keep the record in memory", Integer.class, 1000),

	// DATABASE
	DB_USE_CACHE("db.cache.enabled", "Uses the storage cache", Boolean.class, true),

	OBJECT_SAVE_ONLY_DIRTY("object.saveOnlyDirty", "Object Database saves only object bound to dirty records", Boolean.class, false),

	// TREEMAP
	MVRBTREE_LAZY_UPDATES("mvrbtree.lazyUpdates", "Configure the TreeMaps (indexes and dictionaries) as buffered or not",
			Integer.class, 10000),

	MVRBTREE_NODE_PAGE_SIZE("mvrbtree.nodePageSize",
			"Page size of each single node. 1,024 means that 1,024 entries can be stored inside a node", Float.class, 1024),

	MVRBTREE_LOAD_FACTOR("mvrbtree.loadFactor", "HashMap load factor", Float.class, 0.7f),

	@Deprecated
	MVRBTREE_OPTIMIZE_THRESHOLD("mvrbtree.optimizeThreshold",
			"Deprecated, auto optimize the TreeMap every X operations as get, put and remove", Integer.class, 100000),

	MVRBTREE_ENTRYPOINTS("mvrbtree.entryPoints", "Number of entry points to start searching entries", Integer.class, 15),

	MVRBTREE_OPTIMIZE_ENTRYPOINTS_FACTOR("mvrbtree.optimizeEntryPointsFactor",
			"Multiplicand factor to apply to entry-points list (parameter mvrbtree.entrypoints) to determine if needs of optimization",
			Float.class, 1.0f),

	// FILE
	FILE_MMAP_BLOCK_SIZE("file.mmap.blockSize", "Size of the memory mapped block", Integer.class, 300000),

	FILE_MMAP_MAX_MEMORY("file.mmap.maxMemory",
			"Max memory allocable by memory mapping manager. Note that on 32bit OS the limit is to 2Gb but can change to OS by OS",
			Integer.class, 110000000),

	FILE_MMAP_FORCE_DELAY("file.mmap.forceDelay",
			"Delay time in ms to wait for another force flush of the memory mapped block to the disk", Integer.class, 500),

	FILE_MMAP_FORCE_RETRY("file.mmap.forceRetry", "Number of times the memory mapped block will try to flush to the disk",
			Integer.class, 10),

	// NETWORK
	NETWORK_SOCKET_BUFFER_SIZE("network.socketBufferSize", "TCP/IP Socket buffer size", Integer.class, 32768),

	NETWORK_SOCKET_TIMEOUT("network.timeout", "TCP/IP Socket timeout in ms", Integer.class, 10000),

	NETWORK_SOCKET_RETRY("network.retry",
			"Number of times the client connection retries to connect to the server in case of failure", Integer.class, 5),

	NETWORK_SOCKET_RETRY_DELAY("network.retryDelay", "Number of ms the client wait to reconnect to the server in case of failure",
			Integer.class, 500),

	NETWORK_BINARY_MAX_CONTENT_LENGTH("network.binary.maxLength", "TCP/IP max content length in bytes of BINARY requests",
			Integer.class, 100000),

	NETWORK_BINARY_DEBUG("network.binary.debug", "Debug mode: print all the incoming data on binary channel", Boolean.class, false),

	NETWORK_HTTP_MAX_CONTENT_LENGTH("network.http.maxLength", "TCP/IP max content length in bytes of HTTP requests", Integer.class,
			100000),

	// PROFILER
	PROFILER_ENABLED("profiler.enabled", "Enable the recording of statistics and counters", Boolean.class, false),

	// SERVER
	SERVER_CACHE_STATIC_RESOURCES("server.cache.staticResources", "Cache static resources after loaded", Boolean.class, false),

	// DISTRIBUTED SERVERS
	DISTRIBUTED_ASYNC_TIME_DELAY("distributed.async.timeDelay",
			"Delay time (in ms) of synchronization with slave nodes. 0 means early synchronization", Integer.class, 0),

	DISTRIBUTED_SYNC_MAXRECORDS_BUFFER("distributed.sync.maxRecordsBuffer",
			"Maximum number of records to buffer before to send to the slave nodes", Integer.class, 100);

	private final String									key;
	private final Object									defValue;
	private final Class<?>								type;
	private Object												value						= null;
	private String												description;
	private OConfigurationChangeCallback	changeCallback	= null;

	// AT STARTUP AUTO-CONFIG
	static {
		readConfiguration();
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
		return v instanceof Integer ? ((Integer) v).intValue() : Integer.parseInt(v.toString());
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
}
