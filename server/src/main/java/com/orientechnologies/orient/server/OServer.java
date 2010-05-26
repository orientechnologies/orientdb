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

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.parser.OSystemVariableResolver;
import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.orient.core.OConstants;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.security.OSecurityManager;
import com.orientechnologies.orient.server.config.OConfigurationLoaderXml;
import com.orientechnologies.orient.server.config.OServerConfiguration;
import com.orientechnologies.orient.server.config.OServerHandlerConfiguration;
import com.orientechnologies.orient.server.config.OServerNetworkListenerConfiguration;
import com.orientechnologies.orient.server.config.OServerNetworkProtocolConfiguration;
import com.orientechnologies.orient.server.config.OServerUserConfiguration;
import com.orientechnologies.orient.server.managed.OrientServer;
import com.orientechnologies.orient.server.network.OServerNetworkListener;
import com.orientechnologies.orient.server.network.protocol.ONetworkProtocol;

public class OServer {
	private static final String																PROPERTY_CONFIG_FILE	= "orient.config.file";

	public static final String																DEFAULT_CONFIG_FILE		= "config/orient-server.config";

	protected ReentrantReadWriteLock													lock									= new ReentrantReadWriteLock();

	protected OConfigurationLoaderXml													configurationLoader;
	protected OServerConfiguration														configuration;
	protected OServerShutdownHook															shutdownHook;

	protected List<Object>																		handlers							= new ArrayList<Object>();
	protected Map<String, Class<? extends ONetworkProtocol>>	protocols							= new HashMap<String, Class<? extends ONetworkProtocol>>();
	protected List<OServerNetworkListener>										listeners							= new ArrayList<OServerNetworkListener>();
	protected Map<String, ODatabaseRecord<?>>									memoryDatabases				= new HashMap<String, ODatabaseRecord<?>>();
	protected static ThreadGroup															threadGroup;

	private OrientServer																			managedServer;

	public OServer() throws ClassNotFoundException, MalformedObjectNameException, NullPointerException,
			InstanceAlreadyExistsException, MBeanRegistrationException, NotCompliantMBeanException {
		threadGroup = new ThreadGroup("OrientDB Server");

		System.setProperty("com.sun.management.jmxremote", "true");

		MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();

		// REGISTER PROFILER
		mBeanServer.registerMBean(OProfiler.getInstance().startRecording(), new ObjectName("OrientDB:type=Profiler"));

		// REGISTER SERVER
		managedServer = new OrientServer();
		mBeanServer.registerMBean(managedServer, new ObjectName("OrientDB:type=Server"));

		shutdownHook = new OServerShutdownHook();
	}

	@SuppressWarnings("unchecked")
	public void startup() throws InstantiationException, IllegalAccessException, ClassNotFoundException {
		OLogManager.instance().config(this, "Orient Database Server v" + OConstants.ORIENT_VERSION + " is starting up...");

		loadConfiguration();

		OLogManager.instance().setLevel(configuration.getProperty("log.level"));

		Orient.instance();
		Orient.instance().removeShutdownHook();

		if (configuration.handlers != null)
			// ACTIVATE HANDLERS
			for (OServerHandlerConfiguration h : configuration.handlers) {
				handlers.add(Class.forName(h.clazz).newInstance());
			}

		// REGISTER PROTOCOLS
		for (OServerNetworkProtocolConfiguration p : configuration.network.protocols) {
			protocols.put(p.name, (Class<? extends ONetworkProtocol>) Class.forName(p.implementation));
		}

		// STARTUP LISTENERS
		for (OServerNetworkListenerConfiguration l : configuration.network.listeners) {
			listeners.add(new OServerNetworkListener(l.ipAddress, l.portRange, l.protocol, protocols.get(l.protocol)));
		}

		OLogManager.instance().config(this, "Orient Database Server v" + OConstants.ORIENT_VERSION + " is active.");
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

	public String getStoragePath(final String iName) {
		// SEARCH IN CONFIGURED PATHS
		String dbPath = configuration.getStoragePath(iName);

		if (dbPath == null) {
			// SEARCH IN DEFAULT DATABASE DIRECTORY
			dbPath = OSystemVariableResolver.resolveSystemVariables("${ORIENT_HOME}/databases/" + iName + "/");
			File f = new File(dbPath + "default.odh");
			if (!f.exists())
				throw new OConfigurationException("Database '" + iName + "' is not configured on server");

			dbPath = "local:" + dbPath + iName;
		}

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

			configurationLoader = new OConfigurationLoaderXml(OServerConfiguration.class, config);
			configuration = configurationLoader.load();

			if (configuration.users == null || configuration.users.size() == 0)
				createAdminUser();

		} catch (IOException e) {
			OLogManager.instance().error(this, "Error on reading server configuration.", OConfigurationException.class);
		}
	}

	/**
	 * Authenticate a server user.
	 * 
	 * @param iUserName
	 *          Username to authenticate
	 * @param iPassword
	 *          Password in clear
	 * @return true if authentication is ok, otherwise false
	 */
	public boolean authenticate(final String iUserName, final String iPassword, final String iResourceToCheck) {
		final OServerUserConfiguration user = configuration.getUser(iUserName);

		if (user != null && user.password.equals(iPassword)) {
			if (user.resources.equals("*"))
				// ACCESS TO ALL
				return true;

			String[] resourceParts = user.resources.split(",");
			for (String r : resourceParts)
				if (r.equals(iResourceToCheck))
					return true;
		}

		// WRONG PASSWORD OR NO AUTHORIZATION
		return false;
	}

	public boolean existsStoragePath(final String iURL) {
		return configuration.getStoragePath(iURL) != null;
	}

	public OServerConfiguration getConfiguration() {
		return configuration;
	}

	public void saveConfiguration() throws IOException {
		configurationLoader.save(configuration);
	}

	public static ThreadGroup getThreadGroup() {
		return threadGroup;
	}

	public Map<String, ODatabaseRecord<?>> getMemoryDatabases() {
		return memoryDatabases;
	}

	public Map<String, Class<? extends ONetworkProtocol>> getProtocols() {
		return protocols;
	}

	public List<OServerNetworkListener> getListeners() {
		return listeners;
	}

	public OrientServer getManagedServer() {
		return managedServer;
	}

	private void createAdminUser() throws IOException {
		configuration.users = new ArrayList<OServerUserConfiguration>();

		final long generatedPassword = new Random(System.currentTimeMillis()).nextLong();
		String encodedPassword = OSecurityManager.instance().digest2String(String.valueOf(generatedPassword));

		configuration.users.add(new OServerUserConfiguration("admin", encodedPassword, "*"));
		configurationLoader.save(configuration);
	}

}
