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

import com.orientechnologies.common.thread.OSoftThread;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryClient;
import com.orientechnologies.orient.enterprise.channel.distributed.OChannelDistributedProtocol;

/**
 * Service thread that catches internal messages sent by the server
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 */
public class OStorageRemoteServiceThread extends OSoftThread {
	private final OStorage				storage;
	private OChannelBinaryClient	network;

	public static final int				REQ_ID	= -10;

	public OStorageRemoteServiceThread(final OStorage iStorageRemote, final OChannelBinaryClient iFirstChannel) {
		super("ClientService");
		storage = iStorageRemote;
		network = iFirstChannel;
		start();
	}

	@Override
	protected void execute() throws Exception {
		try {
//			storage.beginResponse(network);
//
//			final byte request = network.readByte();
//
//			switch (request) {
//			case OChannelDistributedProtocol.PUSH_DISTRIBUTED_CONFIG:
//				storage.updateClusterConfiguration(network.readBytes());
//				break;
//
//			case OChannelDistributedProtocol.PUSH_SCHEMA_CHANGED:
//				storage.updateClusterConfiguration(network.readBytes());
//				break;
//			}

			// NOT IN FINALLY BECAUSE IF THE SOCKET IS KILLED COULD HAVE NOT THE LOCK
			network.endResponse();

		} catch (Exception e) {
		}
	}
}
