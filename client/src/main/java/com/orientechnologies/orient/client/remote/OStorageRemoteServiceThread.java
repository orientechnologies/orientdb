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

import java.util.concurrent.SynchronousQueue;

import com.orientechnologies.common.thread.OSoftThread;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryAsynchRequester;
import com.orientechnologies.orient.enterprise.channel.distributed.OChannelDistributedProtocol;

/**
 * Service thread that catches internal messages sent by the server
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 */
public class OStorageRemoteServiceThread extends OSoftThread implements OChannelBinaryAsynchRequester {
	private final OStorageRemote						storage;
	private final SynchronousQueue<Object>	responseQueue	= new SynchronousQueue<Object>();

	public OStorageRemoteServiceThread(final OStorageRemote iStorageRemote) {
		super("StorageService");
		storage = iStorageRemote;
		iStorageRemote.getNetwork().addRequester(this);
	}

	@Override
	protected void execute() throws Exception {
		storage.getNetwork().readStatus(this);

		try {
			final byte request = storage.getNetwork().readByte();

			switch (request) {
			case OChannelDistributedProtocol.PUSH_DISTRIBUTED_CONFIG:
				storage.updateClusterConfiguration(storage.getNetwork().readBytes());
				break;
			}

		} finally {
			storage.getNetwork().getLockRead().unlock();
		}
	}

	public int getRequesterId() {
		return -10;
	}

	public SynchronousQueue<Object> getRequesterResponseQueue() {
		return responseQueue;
	}

	public boolean isPermanentRequester() {
		return true;
	}
}
