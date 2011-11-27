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
package com.orientechnologies.orient.server.handler.distributed;

import java.net.InetAddress;

import javax.crypto.SecretKey;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.security.OSecurityManager;
import com.orientechnologies.orient.core.serialization.OBase64Utils;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.OServerMain;
import com.orientechnologies.orient.server.config.OServerHandlerConfiguration;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;

/**
 * Handles the configuration of a distributed server node.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class ODistributedServerConfiguration {
	public String								name;
	public SecretKey						securityKey;
	public String								securityAlgorithm;
	public InetAddress					networkMulticastAddress;
	public int									networkMulticastPort;
	public int									networkMulticastHeartbeat;					// IN
	public int									networkTimeoutLeader;							// IN
	public int									networkTimeoutNode;								// IN
	public int									networkHeartbeatDelay;							// IN
	public int									serverUpdateDelay;									// IN

	public static final String	CHECKSUM					= "ChEcKsUm1976";

	public static final String	PACKET_HEADER			= "OrientDB v.";
	public static final int			PROTOCOL_VERSION	= 1;
	public static final String	REPLICATOR_USER		= "replicator";

	public ODistributedServerConfiguration(final OServer iServer, final ODistributedServerManager iManager,
			final OServerParameterConfiguration[] iParams) {
		try {
			name = "unknown";
			securityKey = null;
			networkMulticastAddress = InetAddress.getByName("235.1.1.1");
			networkMulticastPort = 2424;
			networkMulticastHeartbeat = 5000;
			networkTimeoutLeader = 2000;
			networkTimeoutNode = 5000;
			networkHeartbeatDelay = 5000;
			securityAlgorithm = "Blowfish";
			serverUpdateDelay = 0;
			byte[] tempSecurityKey = null;

			if (iParams != null)
				for (OServerParameterConfiguration param : iParams) {
					if ("name".equalsIgnoreCase(param.name))
						name = param.value;
					else if ("security.algorithm".equalsIgnoreCase(param.name))
						securityAlgorithm = param.value;
					else if ("security.key".equalsIgnoreCase(param.name))
						tempSecurityKey = OBase64Utils.decode(param.value);
					else if ("network.multicast.address".equalsIgnoreCase(param.name))
						networkMulticastAddress = InetAddress.getByName(param.value);
					else if ("network.multicast.port".equalsIgnoreCase(param.name))
						networkMulticastPort = Integer.parseInt(param.value);
					else if ("network.multicast.heartbeat".equalsIgnoreCase(param.name))
						networkMulticastHeartbeat = Integer.parseInt(param.value);
					else if ("network.timeout.leader".equalsIgnoreCase(param.name))
						networkTimeoutLeader = Integer.parseInt(param.value);
					else if ("network.timeout.connection".equalsIgnoreCase(param.name))
						networkTimeoutNode = Integer.parseInt(param.value);
					else if ("network.heartbeat.delay".equalsIgnoreCase(param.name))
						networkHeartbeatDelay = Integer.parseInt(param.value);
					else if ("server.update.delay".equalsIgnoreCase(param.name))
						serverUpdateDelay = Integer.parseInt(param.value);
				}

			if (OServerMain.server().getUser(REPLICATOR_USER) == null)
				// CREATE REPLICATOR USER
				OServerMain.server().addUser(REPLICATOR_USER, null, "database.passthrough");

			if (tempSecurityKey == null) {
				OLogManager.instance().info(this, "Generating Server security key and saving it in configuration...");
				// GENERATE NEW SECURITY KEY
				securityKey = OSecurityManager.instance().generateKey(securityAlgorithm, 96);

				// CHANGE AND SAVE THE NEW CONFIGURATION
				for (OServerHandlerConfiguration handler : iServer.getConfiguration().handlers) {
					if (handler.clazz.equals(iManager.getClass().getName())) {
						handler.parameters = new OServerParameterConfiguration[iParams.length + 1];
						for (int i = 0; i < iParams.length; ++i) {
							handler.parameters[i] = iParams[i];
						}
						handler.parameters[iParams.length] = new OServerParameterConfiguration("security.key",
								OBase64Utils.encodeBytes(securityKey.getEncoded()));
					}
				}
				iServer.saveConfiguration();

			} else
				// CREATE IT FROM STRING REPRESENTATION
				securityKey = OSecurityManager.instance().createKey(securityAlgorithm, tempSecurityKey);

		} catch (Exception e) {
			throw new OConfigurationException("Cannot configure OrientDB Server as Cluster Node", e);
		}
	}

	public int getServerUpdateDelay() {
		return serverUpdateDelay;
	}

	public int getNetworkHeartbeatDelay() {
		return networkHeartbeatDelay;
	}

	public String getSecurityAlgorithm() {
		return securityAlgorithm;
	}

	public byte[] getSecurityKey() {
		return securityKey.getEncoded();
	}
}
