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
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.server.config.OConfigurationLoaderXml;
import com.orientechnologies.orient.server.config.OServerConfiguration;
import com.orientechnologies.orient.server.config.OServerNetworkListenerConfiguration;
import com.orientechnologies.orient.server.config.OServerNetworkProtocolConfiguration;
import com.orientechnologies.orient.server.managed.OrientServer;
import com.orientechnologies.orient.server.network.OServerNetworkListener;
import com.orientechnologies.orient.server.network.protocol.ONetworkProtocol;

public class OServer {
	private static final String																PROPERTY_CONFIG_FILE	= "orient.config.file";

	public static final String																DEFAULT_CONFIG_FILE		= "config/orient-server.config";

	protected ReentrantReadWriteLock													lock									= new ReentrantReadWriteLock();

	protected OServerConfiguration														configuration;
	protected OServerShutdownHook															shutdownHook;

	protected Map<String, Class<? extends ONetworkProtocol>>	protocols							= new HashMap<String, Class<? extends ONetworkProtocol>>();
	protected List<OServerNetworkListener>										listeners							= new ArrayList<OServerNetworkListener>();

	protected static ThreadGroup															threadGroup;

	public OServer() throws ClassNotFoundException, MalformedObjectNameException, NullPointerException,
			InstanceAlreadyExistsException, MBeanRegistrationException, NotCompliantMBeanException {
		threadGroup = new ThreadGroup("Orient Database Server");

		// START MANAGED BEANS JAVA PLATFORM
		MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();

		// REGISTER PROFILER
		mbs.registerMBean(OProfiler.getInstance().startRecording(), new ObjectName("orient:type=Profiler"));
		// REGISTER SERVER
		mbs.registerMBean(new OrientServer(), new ObjectName("orient:type=OrientServer"));

		shutdownHook = new OServerShutdownHook();
	}

	@SuppressWarnings("unchecked")
	public void startup() throws InstantiationException, IllegalAccessException, ClassNotFoundException {
		loadConfiguration();

		Orient.instance();
		Orient.instance().removeShutdownHook();

		// REGISTER PROTOCOLS
		for (OServerNetworkProtocolConfiguration p : configuration.network.protocols) {
			protocols.put(p.name, (Class<? extends ONetworkProtocol>) Class.forName(p.implementation));
		}

		// STARTUP LISTENERS
		for (OServerNetworkListenerConfiguration l : configuration.network.listeners) {
			listeners.add(new OServerNetworkListener(l.ipAddress, l.port, protocols.get(l.protocol)));
		}
	}

	public void shutdown() {
		OLogManager.instance().config(this, "Orient Database Server is shutdowning...");

		try {
			lock.writeLock().lock();

			// SHUTDOWN LISTENERS
			for (OServerNetworkListener l : listeners) {
				OLogManager.instance().info(this, "Shutdowning connection listener...");
				l.shutdown();
			}

			Orient.instance().shutdown();

		} finally {
			lock.writeLock().unlock();
		}

		OLogManager.instance().config(this, "Orient Database Server shutdown complete");
	}

	public String getStoragePath(final String iURL) {
		String dbPath = configuration.getStoragePath(iURL);

		if (dbPath == null)
			throw new OConfigurationException("Database '" + iURL + "' is not configured on server");
		return dbPath;
	}

	public ThreadGroup getServerThreadGroup() {
		return threadGroup;
	}

	protected void loadConfiguration() {
		try {
			String config = DEFAULT_CONFIG_FILE;
			if (System.getProperty(PROPERTY_CONFIG_FILE) != null)
				config = System.getProperty(PROPERTY_CONFIG_FILE);

			configuration = (OServerConfiguration) new OConfigurationLoaderXml(OServerConfiguration.class, config).load();
		} catch (IOException e) {
			OLogManager.instance().error(this, "Error on reading server configuration.", OConfigurationException.class);
		}
	}

	public static ThreadGroup getThreadGroup() {
		return threadGroup;
	}
}
