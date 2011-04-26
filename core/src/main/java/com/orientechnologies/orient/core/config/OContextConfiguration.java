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

import java.util.HashMap;
import java.util.Map;

/**
 * Represents a context configuration where custom setting could be defined for the context only. If not defined, globals will be
 * taken.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OContextConfiguration {
	private Map<String, Object>	config	= new HashMap<String, Object>();	;

	/**
	 * Empty constructor to create just a proxy for the OGlobalConfiguration. No values are setted.
	 */
	public OContextConfiguration() {
	}

	/**
	 * Initializes the context with custom parameters.
	 * 
	 * @param iConfig
	 *          Map of parameters of type Map<String, Object>.
	 */
	public OContextConfiguration(final Map<String, Object> iConfig) {
		this.config = iConfig;
	}

	public OContextConfiguration(final OContextConfiguration iParent) {
		if (iParent != null)
			config.putAll(iParent.config);
	}

	public Object setValue(final OGlobalConfiguration iConfig, final Object iValue) {
		return config.put(iConfig.getKey(), iValue);
	}

	public Object setValue(final String iName, final Object iValue) {
		return config.put(iName, iValue);
	}

	public Object getValue(final OGlobalConfiguration iConfig) {
		if (config != null && config.containsKey(iConfig.getKey()))
			return config.get(iConfig.getKey());
		return iConfig.getValue();
	}

	@SuppressWarnings("unchecked")
	public <T> T getValue(final String iName, final T iDefaultValue) {
		if (config != null && config.containsKey(iName))
			return (T) config.get(iName);

		final String sysProperty = System.getProperty("orientdb.www.path");
		if (sysProperty != null)
			return (T) sysProperty;

		return iDefaultValue;
	}

	public boolean getValueAsBoolean(final OGlobalConfiguration iConfig) {
		final Object v = getValue(iConfig);
		return v instanceof Boolean ? ((Boolean) v).booleanValue() : Boolean.parseBoolean(v.toString());
	}

	public String getValueAsString(final String iName, final String iDefaultValue) {
		return getValue(iName, iDefaultValue);
	}

	public String getValueAsString(final OGlobalConfiguration iConfig) {
		final Object v = getValue(iConfig);
		return v.toString();
	}

	public int getValueAsInteger(final OGlobalConfiguration iConfig) {
		final Object v = getValue(iConfig);
		return v instanceof Integer ? ((Integer) v).intValue() : Integer.parseInt(v.toString());
	}

	public float getValueAsFloat(final OGlobalConfiguration iConfig) {
		final Object v = getValue(iConfig);
		return v instanceof Float ? ((Float) v).floatValue() : Float.parseFloat(v.toString());
	}
}
