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
package com.orientechnologies.orient.enterprise.channel.binary;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import com.orientechnologies.common.thread.OSoftThread;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * Service thread that catches internal messages sent by the server
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 */
public class OAsynchChannelServiceThread extends OSoftThread {
	private OChannelBinaryClient				network;
	private int													sessionId;
	private ORemoteServerEventListener	remoteServerEventListener;
	private static AtomicInteger				sessionIdSerial	= new AtomicInteger(Integer.MIN_VALUE);

	public OAsynchChannelServiceThread(final ORemoteServerEventListener iRemoteServerEventListener,
			final OChannelBinaryClient iFirstChannel, final String iThreadName) {
		super(Orient.getThreadGroup(), iThreadName);
		sessionId = sessionIdSerial.decrementAndGet();
		remoteServerEventListener = iRemoteServerEventListener;
		network = iFirstChannel;
		start();
	}

	@Override
	protected void execute() throws Exception {
		try {
			try {
				network.beginResponse(sessionId);
				final byte request = network.readByte();

				Object obj = null;

				switch (request) {
				case OChannelBinaryProtocol.REQUEST_PUSH_RECORD:
					obj = (ORecordInternal<?>) OChannelBinaryProtocol.readIdentifiable(network);
					break;

				case OChannelBinaryProtocol.PUSH_NODE2CLIENT_DB_CONFIG:
					obj = new ODocument().fromStream(network.readBytes());
					break;
				}

				if (remoteServerEventListener != null)
					remoteServerEventListener.onRequest(request, obj);

			} catch (IOException ioe) {
				// EXCEPTION RECEIVED (THE SOCKET HAS BEEN CLOSED?) ASSURE TO UNLOCK THE READ AND EXIT THIS THREAD
				sendShutdown();
				network.close();

			} finally {
				network.endResponse();
			}

		} catch (Exception e) {
			// OLogManager.instance().error(this, "Error in service thread", e);
			sendShutdown();
		}
	}
}
