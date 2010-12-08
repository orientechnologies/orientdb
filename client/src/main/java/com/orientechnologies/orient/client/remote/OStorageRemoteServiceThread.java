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
import com.orientechnologies.orient.enterprise.channel.distributed.OChannelDistributedProtocol;

/**
 * Service thread that catches internal messages sent by the server
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 */
public class OStorageRemoteServiceThread extends OSoftThread {
	private final OStorageRemote	storage;

	private static final int			REQ_ID	= -10;

	public OStorageRemoteServiceThread(final OStorageRemote iStorageRemote) {
		super("ClientService");
		storage = iStorageRemote;
		start();
	}

	@Override
	protected void execute() throws Exception {
		try {
			storage.getNetwork().beginResponse(REQ_ID);

			final byte request = storage.getNetwork().readByte();

			switch (request) {
			case OChannelDistributedProtocol.PUSH_DISTRIBUTED_CONFIG:
				storage.updateClusterConfiguration(storage.getNetwork().readBytes());
				break;
			}

			// NOT IN FINALLY BECAUSE IF THE SOCKET IS KILLED COULD HAVE NOT THE LOCK
			storage.getNetwork().endResponse();

		} catch (Exception e) {
		}
	}
}
