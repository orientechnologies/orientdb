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
package com.orientechnologies.orient.server;

import java.io.IOException;
import java.util.Arrays;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryClient;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;
import com.orientechnologies.orient.server.config.OServerConfiguration;
import com.orientechnologies.orient.server.config.OServerConfigurationLoaderXml;
import com.orientechnologies.orient.server.config.OServerNetworkListenerConfiguration;
import com.orientechnologies.orient.server.config.OServerUserConfiguration;
import com.orientechnologies.orient.server.network.OServerNetworkListener;
import com.orientechnologies.orient.server.network.protocol.ONetworkProtocolException;

/**
 * Sends a shutdown command to the server.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OServerShutdownMain {
	public String														clusterNetworkAddress;
	public int[]														clusterNetworkPort;
	public OChannelBinaryClient							network;
	protected OServerConfigurationLoaderXml	configurationLoader;
	protected OServerConfiguration					configuration;

	private OContextConfiguration						contextConfig;
	private String													rootPassword;

	public OServerShutdownMain(final String iServerAddress, final String iServerPorts, final String iRootPassword) {
		contextConfig = new OContextConfiguration();

		try {
			if (iRootPassword == null) {
				// LOAD SERVER ROOT'S PASSWORD
				loadConfiguration();
				if (configuration.users != null && configuration.users.length > 0) {
					for (OServerUserConfiguration u : configuration.users) {
						if (u.name.equals(OServerConfiguration.SRV_ROOT_ADMIN)) {
							// FOUND
							rootPassword = u.password;
							break;
						}
					}
				}
			} else
				rootPassword = iRootPassword;

			if (iServerAddress == null) {
				// LOAD SERVER HOST AND PORT FROM FILE
				loadConfiguration();
				for (OServerNetworkListenerConfiguration l : configuration.network.listeners) {
					if (l.protocol.equals("binary")) {
						clusterNetworkAddress = l.ipAddress;
						clusterNetworkPort = OServerNetworkListener.getPorts(l.portRange);
						break;
					}
				}
			} else {
				clusterNetworkAddress = iServerAddress;
				clusterNetworkPort = OServerNetworkListener.getPorts(iServerPorts);
			}

		} catch (IOException e) {
			OLogManager.instance().error(this, "Error on reading server configuration.", OConfigurationException.class);
		}
	}

	private void loadConfiguration() throws IOException {
		if( configurationLoader != null )
			// AREADY LOADED
			return;
		
		String config = OServerConfiguration.DEFAULT_CONFIG_FILE;
		if (System.getProperty(OServerConfiguration.PROPERTY_CONFIG_FILE) != null)
			config = System.getProperty(OServerConfiguration.PROPERTY_CONFIG_FILE);

		configurationLoader = new OServerConfigurationLoaderXml(OServerConfiguration.class, config);
		configuration = configurationLoader.load();
	}

	public void connect(final int iTimeout) throws IOException {
		// TRY TO CONNECT TO THE RIGHT PORT
		for (int port : clusterNetworkPort)
			try {
				network = new OChannelBinaryClient(clusterNetworkAddress, port, contextConfig);
				break;
			} catch (Exception e) {
			}

		if (network == null)
			throw new ONetworkProtocolException("Can't connect to server host '" + clusterNetworkAddress + "', ports: "
					+ Arrays.toString(clusterNetworkPort));

		network.writeByte((byte) OChannelBinaryProtocol.SHUTDOWN);
		network.writeInt(0);
		network.writeString(OServerConfiguration.SRV_ROOT_ADMIN);
		network.writeString(rootPassword);
		network.flush();

		if (network.readByte() == OChannelBinaryProtocol.STATUS_ERROR){
			network.readInt();
			network.readString();
			throw new ONetworkProtocolException(network.readString());
		}
	}

	public static void main(final String[] iArgs) {
		String serverHost = iArgs.length > 0 ? iArgs[0] : null;
		String serverPorts = iArgs.length > 1 ? iArgs[1] : null;
		String rootPassword = iArgs.length > 2 ? iArgs[2] : null;

		System.out.println("Sending shutdown command to remote OrientDB Server instance...");

		try {
			new OServerShutdownMain(serverHost, serverPorts, rootPassword).connect(5000);
			System.out.println("Shutdown executed correctly");
		} catch (Exception e) {
			System.out.println("Error: " + e.getLocalizedMessage());
		}
		System.out.println();
	}
}
