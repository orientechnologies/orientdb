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
	}

	@Override
	protected void execute() throws Exception {
		storage.getNetwork().readStatus(this);
	}

	@Override
	public int getRequesterId() {
		return -1;
	}

	@Override
	public SynchronousQueue<Object> getRequesterResponseQueue() {
		return responseQueue;
	}
}
