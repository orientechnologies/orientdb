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

/**
 * Keeps all configuration settings. At startup assigns the configuration values by reading system properties.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public enum OConfiguration {
	STORAGE_KEEP_OPEN("orientdb.storage.keepOpen", Boolean.FALSE), TREEMAP_LAZY_UPDATES("orientdb.treemap.lazyUpdates", 500);

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

	@SuppressWarnings("unchecked")
	public <RET> RET getValue() {
		return (RET) (value != null ? value : defValue);
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

}
