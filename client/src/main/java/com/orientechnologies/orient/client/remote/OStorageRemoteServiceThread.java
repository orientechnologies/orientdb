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

import com.orientechnologies.common.thread.OSoftThread;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryClient;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;
import com.orientechnologies.orient.enterprise.channel.distributed.OChannelDistributedProtocol;

/**
 * Service thread that catches internal messages sent by the server
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 */
public class OStorageRemoteServiceThread extends OSoftThread {
	private final OStorageRemoteThread	storage;
	private OChannelBinaryClient				network;

	public OStorageRemoteServiceThread(final OStorageRemoteThread iStorageRemote, final OChannelBinaryClient iFirstChannel) {
		super("ClientService");
		storage = iStorageRemote;
		network = iFirstChannel;
		start();
	}

	@Override
	protected void execute() throws Exception {
		try {
			try {
				storage.beginResponse(network);
			} catch (IOException ioe) {
				// EXCEPTION RECEIVED (THE SOCKET HAS BEEN CLOSED?) ASSURE TO UNLOCK THE READ AND EXIT THIS THREAD
				sendShutdown();
				storage.closeChannel(network);
				storage.handleException("Network connection lost", ioe);
				return;
			}

			final byte request = network.readByte();

			switch (request) {
			case OChannelBinaryProtocol.REQUEST_PUSH_RECORD:
				final ORecordInternal<?> record = (ORecordInternal<?>) OStorageRemote.readIdentifiable(network, null);

				for (ORemoteServerEventListener listener : storage.getRemoteServerEventListeners())
					listener.onRecordPulled(record);

				// ASYNCHRONOUS PUSH INTO THE LEVEL2 CACHE
				storage.getLevel2Cache().updateRecord(record);
				break;

			case OChannelDistributedProtocol.PUSH_DISTRIBUTED_CONFIG:
				final byte[] clusterConfig = network.readBytes();

				for (ORemoteServerEventListener listener : storage.getRemoteServerEventListeners())
					listener.onClusterConfigurationChange(clusterConfig);

				storage.updateClusterConfiguration(clusterConfig);
				break;
			}

			network.endResponse();

		} catch (Exception e) {
			// OLogManager.instance().error(this, "Error in service thread", e);
			sendShutdown();
		}
	}
}
