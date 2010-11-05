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
package com.orientechnologies.orient.server.handler.cluster;

import java.io.IOException;
import java.util.Date;

import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryClient;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;

/**
 * Contains all the information about a cluster node.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OClusterSlave {
	private OClusterNode					node;
	public String									clusterNetworkAddress;
	public int										clusterNetworkPort;
	public Date										joinedOn;
	public OChannelBinaryClient		network;
	private OContextConfiguration	configuration;

	public OClusterSlave(final OClusterNode iNode, final String iServerAddress, final int iServerPort) {
		node = iNode;
		clusterNetworkAddress = iServerAddress;
		clusterNetworkPort = iServerPort;
		joinedOn = new Date();
		configuration = new OContextConfiguration();
	}

	public void connect(final int iTimeout) throws IOException {
		network = new OChannelBinaryClient(clusterNetworkAddress, clusterNetworkPort, configuration);

		network.out.writeByte(OChannelBinaryProtocol.NODECLUSTER_CONNECT);
		network.out.writeInt(0);
		network.flush();

		readStatus();
	}

	private boolean readStatus() throws IOException {
		return network.readByte() != OChannelBinaryProtocol.STATUS_ERROR;
	}
}
