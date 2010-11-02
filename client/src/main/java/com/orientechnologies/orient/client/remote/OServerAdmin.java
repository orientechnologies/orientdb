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
package com.orientechnologies.orient.client.remote;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;

/**
 * Remote administration class of OrientDB Server instances.
 */
public class OServerAdmin {
	private OStorageRemote	storage;
	private int							clientId;

	/**
	 * Creates the object passing a remote URL to connect.
	 * 
	 * @param iURL
	 *          URL to connect. It supports only the "remote" storage type.
	 * @throws IOException
	 */
	public OServerAdmin(String iURL) throws IOException {
		if (iURL.startsWith(OEngineRemote.NAME))
			iURL = iURL.substring(OEngineRemote.NAME.length() + 1);

		storage = new OStorageRemote(iURL, "");
	}

	/**
	 * Creates the object starting from an existent remote storage.
	 * 
	 * @param iStorage
	 */
	public OServerAdmin(final OStorageRemote iStorage) {
		storage = iStorage;
	}

	public OServerAdmin connect(final String iUserName, final String iUserPassword) throws IOException {
		storage.parseServerURLs();
		storage.createNetworkConnection();

		try {
			storage.writeCommand(OChannelBinaryProtocol.CONNECT);
			storage.getNetwork().writeString(iUserName);
			storage.getNetwork().writeString(iUserPassword);
			storage.getNetwork().flush();

			storage.readStatus();

			clientId = storage.getNetwork().readInt();

		} catch (Exception e) {
			OLogManager.instance().error(this, "Can't create the remote storage: " + storage.getName(), e, OStorageException.class);
			storage.close();
		}

		return this;
	}

	public OServerAdmin createDatabase(String iStorageMode) throws IOException {
		storage.checkConnection();

		try {
			if (iStorageMode == null)
				iStorageMode = "csv";

			storage.writeCommand(OChannelBinaryProtocol.DB_CREATE);
			storage.getNetwork().writeString(storage.getName());
			storage.getNetwork().writeString(iStorageMode);

			storage.readStatus();

		} catch (Exception e) {
			OLogManager.instance().error(this, "Can't create the remote storage: " + storage.getName(), e, OStorageException.class);
			storage.close();
		}
		return this;
	}

	public OServerAdmin deleteDatabase() throws IOException {
		storage.checkConnection();

		try {
			storage.writeCommand(OChannelBinaryProtocol.DB_DELETE);
			storage.getNetwork().writeString(storage.getName());

			storage.readStatus();

		} catch (Exception e) {
			OLogManager.instance().error(this, "Can't delete the remote storage: " + storage.getName(), e, OStorageException.class);
			storage.close();
		}
		return this;
	}

	public Map<String, String> getGlobalConfigurations() throws IOException {
		storage.checkConnection();

		final Map<String, String> config = new HashMap<String, String>();

		try {
			storage.writeCommand(OChannelBinaryProtocol.CONFIG_LIST);
			storage.readStatus();

			final int num = storage.getNetwork().readShort();

			for (int i = 0; i < num; ++i)
				config.put(storage.getNetwork().readString(), storage.getNetwork().readString());

		} catch (Exception e) {
			OLogManager.instance().error(this, "Can't retrieve the configuration list", e, OStorageException.class);
			storage.close();
		}
		return config;
	}

	public String getGlobalConfiguration(final OGlobalConfiguration iConfig) throws IOException {
		storage.checkConnection();

		try {
			storage.writeCommand(OChannelBinaryProtocol.CONFIG_GET);
			storage.getNetwork().writeString(iConfig.getKey());
			storage.readStatus();

			return storage.getNetwork().readString();

		} catch (Exception e) {
			OLogManager.instance().error(this, "Can't retrieve the configuration value: " + iConfig.getKey(), e, OStorageException.class);
			storage.close();
		}
		return null;
	}

	public OServerAdmin setGlobalConfiguration(final OGlobalConfiguration iConfig, final Object iValue) throws IOException {
		storage.checkConnection();

		try {
			storage.writeCommand(OChannelBinaryProtocol.CONFIG_SET);
			storage.getNetwork().writeString(iConfig.getKey());
			storage.getNetwork().writeString(iValue != null ? iValue.toString() : "");
			storage.readStatus();

		} catch (Exception e) {
			OLogManager.instance().error(this, "Can't set the configuration value: " + iConfig.getKey(), e, OStorageException.class);
			storage.close();
		}
		return this;
	}

	public void close() {
		storage.close();
	}
}
