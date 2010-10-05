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
	STORAGE_KEEP_OPEN("orientdb.storage.keepOpen", Boolean.FALSE), STORAGE_CACHE_SIZE("orientdb.storage.cache.size", 5000),

	DB_USE_CACHE("orientdb.db.cache", false),

	TREEMAP_LAZY_UPDATES("orientdb.treemap.lazyUpdates", 50000), TREEMAP_NODE_PAGE_SIZE("orientdb.treemap.nodePageSize", 1024), TREEMAP_LOAD_FACTOR(
			"orientdb.treemap.loadFactor", 0.7f), TREEMAP_OPTIMIZE_THRESHOLD("orientdb.treemap.optimizeThreshold", 50000), TREEMAP_OPTIMIZE_ENTRYPOINTS_FACTOR(
			"orientdb.treemap.optimizeEntryPointsFactor", 1.5f), TREEMAP_ENTRYPOINTS("orientdb.treemap.entrypoints", 50),

	FILE_MMAP_BLOCK_SIZE("orientdb.file.mmap.blockSize", 300000), FILE_MMAP_MAX_MEMORY("orientdb.file.mmap.maxMemory", 110000000), FILE_MMAP_FORCE_DELAY(
			"orientdb.file.mmap.forceDelay", 500), FILE_MMAP_FORCE_RETRY("orientdb.file.mmap.forceRetry", 5),

	NETWORK_SOCKET_BUFFER_SIZE("orientdb.network.socketBufferSize", 32768), NETWORK_HTTP_TIMEOUT("orientdb.network.http.timeout",
			10000), NETWORK_HTTP_MAX_CONTENT_LENGTH("orientdb.network.http.maxLength", 10000);

	private final String	key;
	private final Object	defValue;
	private Object				value	= null;

	// AT STARTUP AUTO-CONFIG
	static {
		readConfiguration();
	}

	OConfiguration(final String iKey, final Object iDefValue) {
		key = iKey;
		defValue = iDefValue;
	}

	public void setValue(final Object iValue) {
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
