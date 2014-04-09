/*
 * Copyright 2014 Charles Baptiste (cbaptiste--at--blacksparkcorp.com)
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
package com.orientechnologies.orient.enterprise.channel;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.security.NoSuchAlgorithmException;

import javax.net.SocketFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocket;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;

public class OSocketFactory {

	SocketFactory socketFactory;
	boolean useSSL = false;
	SSLContext context = null;
	OContextConfiguration config;

	private OSocketFactory(OContextConfiguration iConfig) {
		config = iConfig;		
		setUseSSL(iConfig.getValueAsBoolean(OGlobalConfiguration.CLIENT_USE_SSL));
	}

	public static OSocketFactory instance(final OContextConfiguration iConfig) {
		return new OSocketFactory(iConfig);
	}

	public static OSocketFactory instance(final OContextConfiguration iConfig,
			SSLContext context) {

		OSocketFactory sFactory = instance(iConfig);
		sFactory.setSSLContext(context);
		return sFactory;
	}

	private SocketFactory getBackingFactory() {
		if (socketFactory == null) {
			if (getUseSSL()) {
				socketFactory = getSSLContext().getSocketFactory();
			} else {
				socketFactory = SocketFactory.getDefault();
			}
		}
		return socketFactory;
	}

	public void setSSLContext(SSLContext context) {
		this.context = context;
		synchronized(socketFactory) {
			socketFactory = null;
		}
	}

	private SSLContext getSSLContext() {
		if (context == null) {
			try {
				context = SSLContext.getDefault();
			} catch (NoSuchAlgorithmException e) {
				OLogManager.instance().error(this,
						"Error creating ssl context", e);
			}
		}
		return context;
	}

	public void setUseSSL(boolean useSSL) {
		this.useSSL = useSSL;
	}

	public boolean getUseSSL() {
		return useSSL;
	}
	
	private Socket configureSocket(Socket socket) throws SocketException {
				
		// Add possible timeouts?
		return socket;
	}

	public Socket createSocket() throws IOException {
		return configureSocket(getBackingFactory().createSocket());
	}

	public Socket createSocket(String host, int port) throws IOException,
			UnknownHostException {
		return configureSocket(getBackingFactory().createSocket(host, port));
	}

	public Socket createSocket(InetAddress host, int port) throws IOException {
		return configureSocket(getBackingFactory().createSocket(host, port));
	}

	public Socket createSocket(String host, int port, InetAddress localHost,
			int localPort) throws IOException, UnknownHostException {
		return configureSocket(getBackingFactory().createSocket(host, port, localHost,
				localPort));
	}

	public Socket createSocket(InetAddress address, int port,
			InetAddress localAddress, int localPort) throws IOException {
		return configureSocket(getBackingFactory().createSocket(address, port, localAddress,
				localPort));
	}

}
