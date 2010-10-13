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
package com.orientechnologies.orient.client.admin;

import java.io.IOException;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.client.remote.OEngineRemote;
import com.orientechnologies.orient.client.remote.OStorageRemote;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;

/**
 * Remote admin of Orient Servers.
 */
public class OServerAdmin extends OStorageRemote {
	public OServerAdmin(String iURL) throws IOException {
		super(iURL, "");

		if (iURL.startsWith(OEngineRemote.NAME))
			url = iURL.substring(OEngineRemote.NAME.length() + 1);
	}

	public OServerAdmin connect() throws IOException {
		parseServerURLs();
		createNetworkConnection();
		return this;
	}

	public OServerAdmin createDatabase(String iStorageMode) throws IOException {
		checkConnection();

		try {
			if (iStorageMode == null)
				iStorageMode = "csv";

			network.writeByte(OChannelBinaryProtocol.DB_CREATE);
			network.writeInt(0);
			network.writeString(name);
			network.writeString(iStorageMode);
			network.flush();

			readStatus();

		} catch (Exception e) {
			OLogManager.instance().error(this, "Can't create the remote storage: " + name, e, OStorageException.class);
			close();
		}
		return this;
	}
}
