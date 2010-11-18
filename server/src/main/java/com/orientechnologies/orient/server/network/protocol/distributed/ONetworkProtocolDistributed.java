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
package com.orientechnologies.orient.server.network.protocol.distributed;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.tool.ODatabaseExport;
import com.orientechnologies.orient.core.db.tool.ODatabaseImport;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryInputStream;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryOutputStream;
import com.orientechnologies.orient.enterprise.channel.distributed.OChannelDistributedProtocol;
import com.orientechnologies.orient.server.OServerMain;
import com.orientechnologies.orient.server.handler.distributed.ODistributedServerLoaderChecker;
import com.orientechnologies.orient.server.handler.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.network.protocol.binary.ONetworkProtocolBinary;

/**
 * Extends binary protocol to include cluster commands.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class ONetworkProtocolDistributed extends ONetworkProtocolBinary {
	private ODistributedServerManager	manager;
	private volatile long							lastHeartBeat;

	public ONetworkProtocolDistributed() {
		super("Distributed-DB");

		manager = OServerMain.server().getHandler(ODistributedServerManager.class);
		if (manager == null)
			throw new OConfigurationException(
					"Can't find a ODistributedServerDiscoveryManager instance registered as handler. Check the server configuration in the handlers section.");

		// FIRST TIME: SCHEDULE THE HEARTBEAT CHECKER
		Orient.getTimer().schedule(new ODistributedServerLoaderChecker(manager, this), manager.getNetworkHeartbeatDelay(),
				manager.getNetworkHeartbeatDelay() / 2);
	}

	@Override
	protected void parseCommand() throws IOException {
		if (requestType < 80) {
			// BINARY REQUESTS
			super.parseCommand();
			return;
		}

		// DISTRIBUTED SERVER REQUESTS
		switch (requestType) {
		case OChannelDistributedProtocol.REQUEST_DISTRIBUTED_HEARTBEAT:
			data.commandInfo = "Keep-alive";
			lastHeartBeat = System.currentTimeMillis();
			sendOk(0);
			break;

		case OChannelDistributedProtocol.REQUEST_DISTRIBUTED_CONNECT: {
			data.commandInfo = "Cluster connection";

			manager.receivedLeaderConnection(this);

			sendOk(0);

			// TRANSMITS FOR ALL THE CONFIGURED STORAGES: STORAGE/VERSION
			Map<String, Long> storages = new HashMap<String, Long>();
			for (OStorage stg : Orient.instance().getStorages()) {
				storages.put(stg.getName(), stg.getVersion());
			}

			channel.writeInt(storages.size());
			for (Entry<String, Long> s : storages.entrySet()) {
				channel.writeString(s.getKey());
				channel.writeLong(s.getValue());
			}

			break;
		}

		case OChannelDistributedProtocol.REQUEST_DISTRIBUTED_DB_SHARE_SENDER:
			data.commandInfo = "Share the database to a remote server";

			try {
				final String dbName = channel.readString();
				final String dbUser = channel.readString();
				final String dbPassword = channel.readString();
				final String remoteServerURL = channel.readString();
				final String remoteServerUser = channel.readString();
				final String remoteServerPassword = channel.readString();

				checkServerAccess("database.share");

				final ODatabaseDocumentTx db = openDatabase(dbName, dbUser, dbPassword);

				new ODatabaseExport(db, new OChannelBinaryOutputStream(channel), null);

				sendOk(0);

			} catch (Exception e) {
				channel.clearInput();

			} finally {

			}
			break;

		case OChannelDistributedProtocol.REQUEST_DISTRIBUTED_DB_SHARE_RECEIVER:
			data.commandInfo = "Received a shared database from a remote server to install";

			try {
				final String dbName = channel.readString();
				final String storageMode = channel.readString();

				checkServerAccess("database.share");

				final ODatabaseDocument db = createDatabase(dbName, storageMode);

				new ODatabaseImport(db, new OChannelBinaryInputStream(channel), null);

				sendOk(0);

			} catch (Exception e) {
				channel.clearInput();

			} finally {

			}
			break;

		case OChannelDistributedProtocol.REQUEST_DISTRIBUTED_DB_CONFIG: {
			data.commandInfo = "Update db configuration from server node leader";
			ODocument config = (ODocument) new ODocument().fromStream(channel.readBytes());

			OLogManager.instance().warn(this, "Changed distributed server configuration:\n%s", config.toJSON("indent:2"));

			sendOk(0);
			break;
		}

		default:
			data.commandInfo = "Command not supported";
			OLogManager.instance().error(this, "Request not supported. Code: " + requestType);
			channel.clearInput();
			sendError(clientTxId, null);
		}
	}

	public long getLastHeartBeat() {
		return lastHeartBeat;
	}
}
