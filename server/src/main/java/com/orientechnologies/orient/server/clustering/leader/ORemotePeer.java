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
import java.util.logging.Level;

import javax.crypto.SecretKey;

import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.enterprise.channel.binary.OAsynchChannelServiceThread;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryClient;
import com.orientechnologies.orient.server.clustering.OClusterLogger.DIRECTION;
import com.orientechnologies.orient.server.clustering.OClusterLogger.TYPE;
import com.orientechnologies.orient.server.handler.distributed.OClusterProtocol;
import com.orientechnologies.orient.server.replication.ODistributedRemoteAsynchEventListener;

/**
 * Contains all the information about a cluster node managed by the Leader.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class ORemotePeer extends ORemoteNodeAbstract {
	public enum STATUS {
		DISCONNECTED, CONNECTING, CONNECTED, UNREACHABLE, SYNCHRONIZING
	}

	private OLeaderNode						leader;
	private OContextConfiguration	configuration;
	private volatile STATUS				status	= STATUS.DISCONNECTED;

	public ORemotePeer(final OLeaderNode iNode, final String iServerAddress, final int iServerPort) {
		super(iServerAddress, iServerPort);
		leader = iNode;
		configuration = new OContextConfiguration();
		setStatus(STATUS.CONNECTING);

		logger.setNode(iServerAddress);
	}

	/**
	 * Connects the current leader to a remote node peer.
	 * 
	 * @param iTimeout
	 * @param iClusterName
	 * @param iSecurityKey
	 * @return true if the node has been connected, otherwise false. False is the case the other node is a Leader too and wins the
	 *         conflicts.
	 * @throws IOException
	 */
	public boolean connect(final int iTimeout, final String iClusterName, final SecretKey iSecurityKey) throws IOException {
		configuration.setValue(OGlobalConfiguration.NETWORK_SOCKET_TIMEOUT, iTimeout);

		channel = new OChannelBinaryClient(networkAddress, networkPort, configuration, OClusterProtocol.CURRENT_PROTOCOL_VERSION);

		logger.log(this, Level.WARNING, TYPE.CLUSTER, DIRECTION.IN,
				"received joining request from peer node. Checking authorizations...");

		// CONNECT TO THE SERVER
		channel.writeByte(OClusterProtocol.REQUEST_LEADER2PEER_CONNECT);
		channel.writeInt(sessionId);

		final ODocument doc = new ODocument();
		doc.field("clusterName", iClusterName);
		doc.field("clusterKey", iSecurityKey.getEncoded());
		doc.field("leaderNodeAddress", leader.getManager().getId());
		doc.field("leaderNodeRunningSince", leader.getManager().getRunningSince());
		channel.writeBytes(doc.toStream());
		channel.flush();

		final ODocument cfg;

		beginResponse();
		try {
			final byte connectedAsPeer = channel.readByte();
			if (connectedAsPeer == 0) {
				logger.log(this, Level.WARNING, TYPE.CLUSTER, DIRECTION.IN,
						"remote server node has refused the connection because it's the new Leader. Switching to be a Peer Node...");
				leader.getManager().becomePeer(null);
				disconnect();
				return false;
			}

			logger.log(this, Level.INFO, TYPE.CLUSTER, DIRECTION.IN, "joined peer node");

			// READ PEER DATABASES
			cfg = new ODocument().fromStream(channel.readBytes());
		} finally {
			endResponse();
		}

		// SEND BACK THE LIST OF NODES HANDLING ITS DATABASE
		final ODocument answer = leader.updatePeerDatabases(id, cfg);
		channel.writeBytes(answer.toStream());
		channel.flush();

		setStatus(STATUS.CONNECTED);

		serviceThread = new OAsynchChannelServiceThread(new ODistributedRemoteAsynchEventListener(leader.getManager(), null, id),
				channel, "OrientDB <- Asynch Node/" + id);

		return true;
	}

	public boolean sendHeartBeat(final int iNetworkTimeout) throws InterruptedException {
		if (channel == null)
			return false;

		if (status != STATUS.CONNECTED)
			return false;

		configuration.setValue(OGlobalConfiguration.NETWORK_SOCKET_TIMEOUT, iNetworkTimeout);
		logger.log(this, Level.FINE, TYPE.CLUSTER, DIRECTION.OUT, "Sending heartbeat message...");

		try {
			channel.beginRequest();
			try {
				channel.writeByte(OClusterProtocol.REQUEST_LEADER2PEER_HEARTBEAT);
				channel.writeInt(sessionId);
			} finally {
				channel.endRequest();
			}

			logger.log(this, Level.FINE, TYPE.CLUSTER, DIRECTION.IN, "Waiting for the heartbeat response...");

			channel.beginResponse(sessionId, 2000);
			channel.endResponse();

			logger.log(this, Level.FINE, TYPE.CLUSTER, DIRECTION.IN, "Received heartbeat ACK");

		} catch (Exception e) {
			logger.log(this, Level.FINE, TYPE.CLUSTER, DIRECTION.OUT, "Error on sending heartbeat to server node", e);
			return false;
		}

		return true;
	}

	/**
	 * Check if a remote node is really connected.
	 * 
	 * @return true if it's connected, otherwise false
	 */
	@Override
	public boolean checkConnection() {
		final boolean connected = super.checkConnection();

		if (!connected)
			setStatus(STATUS.DISCONNECTED);

		return connected;
	}

	@Override
	public void disconnect() {
		super.disconnect();
		setStatus(STATUS.DISCONNECTED);
	}

	public STATUS getStatus() {
		return status;
	}

	private void setStatus(final STATUS iStatus) {
		logger.log(this, Level.FINE, TYPE.CLUSTER, DIRECTION.NONE, "peer changed status %s -> %s", status, iStatus);
		status = iStatus;
	}
}
