/*
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
package com.orientechnologies.orient.server.clustering.leader;

import java.io.IOException;
import java.util.Date;

import com.orientechnologies.orient.enterprise.channel.binary.OAsynchChannelServiceThread;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryClient;

/**
 * Basic abstract class for remote node.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class ORemoteNodeAbstract {
	protected String											id;
	protected String											networkAddress;
	protected int													networkPort;
	protected Date												connectedOn;
	protected OChannelBinaryClient				channel;
	protected final int										sessionId	= 0;
	protected OAsynchChannelServiceThread	serviceThread;

	public ORemoteNodeAbstract(final String iServerAddress, final int iServerPort) {
		networkAddress = iServerAddress;
		networkPort = iServerPort;
		connectedOn = new Date();
		id = networkAddress + ":" + networkPort;
	}

	public OChannelBinaryClient beginRequest(final byte iRequestType) throws IOException {
		channel.beginRequest();
		channel.writeByte(iRequestType);
		channel.writeInt(sessionId);
		return channel;
	}

	public void endRequest() throws IOException {
		if (channel != null)
			channel.endRequest();
	}

	public void beginResponse() throws IOException {
		if (channel != null)
			channel.beginResponse(sessionId);
	}

	public void endResponse() {
		if (channel != null)
			channel.endResponse();
	}

	/**
	 * Check if a remote node is really connected.
	 * 
	 * @return true if it's connected, otherwise false
	 */
	public boolean checkConnection() {
		boolean connected = false;

		if (channel != null && channel.socket != null)
			try {
				connected = channel.socket.isConnected();
			} catch (Exception e) {
			}

		return connected;
	}

	public void disconnect() {
		if (channel != null)
			channel.close();
		channel = null;
		if (serviceThread != null)
			serviceThread.sendShutdown();
	}

	@Override
	public String toString() {
		return id;
	}

	public Object getId() {
		return id;
	}
}
