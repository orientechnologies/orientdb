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
package com.orientechnologies.orient.server.network;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;

import com.orientechnologies.orient.server.config.OServerParameterConfiguration;

public abstract class OServerSocketFactory {

	private static OServerSocketFactory theFactory;
	private String name;

	protected OServerSocketFactory() {
	}

	public static OServerSocketFactory getDefault() {
		synchronized (OServerSocketFactory.class) {
			if (theFactory == null) {
				theFactory = new ODefaultServerSocketFactory();
			}
		}

		return theFactory;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public void config(String name,
			final OServerParameterConfiguration[] iParameters) {
		this.name = name;
	}

	public abstract ServerSocket createServerSocket(int port)
			throws IOException;

	public abstract ServerSocket createServerSocket(int port, int backlog)
			throws IOException;

	public abstract ServerSocket createServerSocket(int port, int backlog,
			InetAddress ifAddress) throws IOException;
}

class ODefaultServerSocketFactory extends OServerSocketFactory {

	ODefaultServerSocketFactory() {
	}

	public ServerSocket createServerSocket() throws IOException {
		return new ServerSocket();
	}

	@Override
	public ServerSocket createServerSocket(int port) throws IOException {
		return new ServerSocket(port);
	}

	@Override
	public ServerSocket createServerSocket(int port, int backlog)
			throws IOException {
		return new ServerSocket(port, backlog);
	}

	@Override
	public ServerSocket createServerSocket(int port, int backlog,
			InetAddress ifAddress) throws IOException {
		return new ServerSocket(port, backlog, ifAddress);
	}

	@Override
	public void config(String name, OServerParameterConfiguration[] iParameters) {
		super.config(name, iParameters);
	}

	@Override
	public String getName() {
		return "default";
	}
}
