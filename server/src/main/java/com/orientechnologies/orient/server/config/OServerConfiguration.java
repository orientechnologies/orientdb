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
package com.orientechnologies.orient.server.config;

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlElementRef;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

import com.orientechnologies.orient.core.config.OEntryConfiguration;

@XmlRootElement(name = "orient-server")
public class OServerConfiguration {
	public static final String								FILE_NAME	= "server-config.xml";
	// private static final String HEADER = "OrientDB Server configuration";

	@XmlTransient
	public String															location;

	@XmlElementWrapper
	@XmlElementRef(type = OServerHandlerConfiguration.class)
	public List<OServerHandlerConfiguration>	handlers;

	@XmlElementRef(type = OServerNetworkConfiguration.class)
	public OServerNetworkConfiguration				network;

	@XmlElementWrapper
	@XmlElementRef(type = OServerStorageConfiguration.class)
	public List<OServerStorageConfiguration>	storages;

	@XmlElementWrapper(required = false)
	@XmlElementRef(type = OServerUserConfiguration.class)
	public List<OServerUserConfiguration>			users;

	@XmlElementWrapper
	@XmlElementRef(type = OEntryConfiguration.class)
	public List<OEntryConfiguration>					properties;

	/**
	 * Empty constructor for JAXB
	 */
	public OServerConfiguration() {
	}

	public OServerConfiguration(OConfigurationLoaderXml iFactory) {
		location = FILE_NAME;
		network = new OServerNetworkConfiguration(iFactory);
		storages = new ArrayList<OServerStorageConfiguration>();
	}

	public String getStoragePath(String iURL) {
		for (OServerStorageConfiguration stg : storages)
			if (stg.name.equals(iURL))
				return stg.path;

		return null;
	}

	/**
	 * Returns the property value configured, if any.
	 * 
	 * @param iName
	 *          Property name to find
	 */
	public String getProperty(final String iName) {
		return getProperty(iName, null);
	}

	/**
	 * Returns the property value configured, if any.
	 * 
	 * @param iName
	 *          Property name to find
	 * @param iDefaultValue
	 *          Default value returned if not found
	 */
	public String getProperty(final String iName, final String iDefaultValue) {
		if (properties == null)
			return null;

		for (OEntryConfiguration p : properties) {
			if (p.name.equals(iName))
				return p.value;
		}

		return null;
	}

	public OServerUserConfiguration getUser(final String iName) {
		for (OServerUserConfiguration u : users) {
			if (u.name.equals(iName))
				return u;
		}
		return null;
	}

}
