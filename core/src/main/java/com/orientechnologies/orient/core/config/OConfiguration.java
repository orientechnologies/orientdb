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

/**
 * Keeps all configuration settings. At startup assigns the configuration values by reading system properties.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public enum OConfiguration {
	STORAGE_KEEP_OPEN("orientdb.storage.keepOpen", Boolean.class, Boolean.FALSE), STORAGE_CACHE_SIZE("orientdb.storage.cache.size",
			Integer.class, 5000),

	DB_USE_CACHE("orientdb.db.cache.enabled", Boolean.class, true),

	TREEMAP_LAZY_UPDATES("orientdb.treemap.lazyUpdates", Integer.class, 300), TREEMAP_NODE_PAGE_SIZE("orientdb.treemap.nodePageSize",
			Float.class, 1024), TREEMAP_LOAD_FACTOR("orientdb.treemap.loadFactor", Float.class, 0.7f), TREEMAP_OPTIMIZE_THRESHOLD(
			"orientdb.treemap.optimizeThreshold", Integer.class, 50000), TREEMAP_ENTRYPOINTS("orientdb.treemap.entryPoints",
			Integer.class, 30), TREEMAP_OPTIMIZE_ENTRYPOINTS_FACTOR("orientdb.treemap.optimizeEntryPointsFactor", Float.class, 1.0f),

	FILE_MMAP_BLOCK_SIZE("orientdb.file.mmap.blockSize", Integer.class, 300000), FILE_MMAP_MAX_MEMORY("orientdb.file.mmap.maxMemory",
			Integer.class, 110000000), FILE_MMAP_FORCE_DELAY("orientdb.file.mmap.forceDelay", Integer.class, 500), FILE_MMAP_FORCE_RETRY(
			"orientdb.file.mmap.forceRetry", Integer.class, 5),

	NETWORK_SOCKET_BUFFER_SIZE("orientdb.network.socketBufferSize", Integer.class, 32768), NETWORK_HTTP_TIMEOUT(
			"orientdb.network.http.timeout", Integer.class, 10000), NETWORK_HTTP_MAX_CONTENT_LENGTH("orientdb.network.http.maxLength",
			Integer.class, 10000),

	PROFILER_ENABLED("orientdb.profiler.enabled", Boolean.class, false);

	private final String		key;
	private final Object		defValue;
	private final Class<?>	type;
	private Object					value	= null;

	// AT STARTUP AUTO-CONFIG
	static {
		readConfiguration();
	}

	OConfiguration(final String iKey, final Class<?> iType, final Object iDefValue) {
		key = iKey;
		defValue = iDefValue;
		type = iType;
	}

	public void setValue(final Object iValue) {
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

	/**
	 * Assign configuration values by reading system properties.
	 */
	private static void readConfiguration() {
		for (OConfiguration config : values())
			config.setValue(System.getProperty(config.key));
	}

	/**
	 * Change configuration values in one shot by passing a Map of values.
	 */
	public static void setConfiguration(final Map<String, Object> iConfig) {
		OConfiguration cfg;
		for (Entry<String, Object> config : iConfig.entrySet()) {
			cfg = valueOf(config.getKey());
			if (cfg != null)
				cfg.setValue(config.getValue());
		}
	}
}
