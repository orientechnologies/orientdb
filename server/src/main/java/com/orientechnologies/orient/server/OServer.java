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
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
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
import com.orientechnologies.orient.core.command.OCommandManager;
import com.orientechnologies.orient.core.command.script.OCommandScript;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.security.OSecurityManager;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.enterprise.command.OCommandExecutorScript;
import com.orientechnologies.orient.server.config.OServerConfiguration;
import com.orientechnologies.orient.server.config.OServerConfigurationLoaderXml;
import com.orientechnologies.orient.server.config.OServerEntryConfiguration;
import com.orientechnologies.orient.server.config.OServerHandlerConfiguration;
import com.orientechnologies.orient.server.config.OServerNetworkListenerConfiguration;
import com.orientechnologies.orient.server.config.OServerNetworkProtocolConfiguration;
import com.orientechnologies.orient.server.config.OServerStorageConfiguration;
import com.orientechnologies.orient.server.config.OServerUserConfiguration;
import com.orientechnologies.orient.server.handler.OServerHandler;
import com.orientechnologies.orient.server.managed.OrientServer;
import com.orientechnologies.orient.server.network.OServerNetworkListener;
import com.orientechnologies.orient.server.network.protocol.ONetworkProtocol;

public class OServer {
	protected ReentrantReadWriteLock													lock				= new ReentrantReadWriteLock();

	protected volatile boolean																running			= true;
	protected OServerConfigurationLoaderXml										configurationLoader;
	protected OServerConfiguration														configuration;
	protected OContextConfiguration														contextConfiguration;
	protected OServerShutdownHook															shutdownHook;
	protected List<OServerHandler>														handlers		= new ArrayList<OServerHandler>();
	protected Map<String, Class<? extends ONetworkProtocol>>	protocols		= new HashMap<String, Class<? extends ONetworkProtocol>>();
	protected List<OServerNetworkListener>										listeners		= new ArrayList<OServerNetworkListener>();
	protected static ThreadGroup															threadGroup;

	private OrientServer																			managedServer;
	private ObjectName																				onProfiler	= new ObjectName("OrientDB:type=Profiler");
	private ObjectName																				onServer		= new ObjectName("OrientDB:type=Server");

	public OServer() throws ClassNotFoundException, MalformedObjectNameException, NullPointerException,
			InstanceAlreadyExistsException, MBeanRegistrationException, NotCompliantMBeanException {
		defaultSettings();

		OLogManager.installCustomFormatter();

		threadGroup = new ThreadGroup("OrientDB Server");

		// REGISTER THE COMMAND SCRIPT
		OCommandManager.instance().registerExecutor(OCommandScript.class, OCommandExecutorScript.class);

		OGlobalConfiguration.STORAGE_KEEP_OPEN.setValue(true);
		System.setProperty("com.sun.management.jmxremote", "true");

		MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();

		// REGISTER PROFILER
		mBeanServer.registerMBean(OProfiler.getInstance().startRecording(), onProfiler);

		// REGISTER SERVER
		managedServer = new OrientServer();
		mBeanServer.registerMBean(managedServer, onServer);

		shutdownHook = new OServerShutdownHook();
	}

	public void startup() throws InstantiationException, IllegalAccessException, ClassNotFoundException, IllegalArgumentException,
			SecurityException, InvocationTargetException, NoSuchMethodException {
		String config = OServerConfiguration.DEFAULT_CONFIG_FILE;
		if (System.getProperty(OServerConfiguration.PROPERTY_CONFIG_FILE) != null)
			config = System.getProperty(OServerConfiguration.PROPERTY_CONFIG_FILE);

		startup(new File(config));
	}

	public void startup(final File iConfigurationFile) throws InstantiationException, IllegalAccessException, ClassNotFoundException,
			IllegalArgumentException, SecurityException, InvocationTargetException, NoSuchMethodException {
		startup(loadConfigurationFromFile(iConfigurationFile));
	}

	public void startup(final String iConfiguration) throws InstantiationException, IllegalAccessException, ClassNotFoundException,
			IllegalArgumentException, SecurityException, InvocationTargetException, NoSuchMethodException, IOException {

		configurationLoader = new OServerConfigurationLoaderXml(OServerConfiguration.class, iConfiguration);
		configuration = configurationLoader.load();

		startup(configuration);
	}

	@SuppressWarnings("unchecked")
	public void startup(final OServerConfiguration iConfiguration) throws InstantiationException, IllegalAccessException,
			ClassNotFoundException, IllegalArgumentException, SecurityException, InvocationTargetException, NoSuchMethodException {
		OLogManager.instance().info(this, "OrientDB Server v" + OConstants.getVersion() + " is starting up...");

		loadConfiguration(iConfiguration);

		Orient.instance();
		Orient.instance().removeShutdownHook();

		// REGISTER PROTOCOLS
		for (OServerNetworkProtocolConfiguration p : configuration.network.protocols)
			protocols.put(p.name, (Class<? extends ONetworkProtocol>) Class.forName(p.implementation));

		// STARTUP LISTENERS
		for (OServerNetworkListenerConfiguration l : configuration.network.listeners)
			listeners.add(new OServerNetworkListener(this, l.ipAddress, l.portRange, l.protocol, protocols.get(l.protocol), l.parameters,
					l.commands));

		registerHandlers();

		OLogManager.instance().info(this, "OrientDB Server v" + OConstants.ORIENT_VERSION + " is active.");
	}

	public void shutdown() {
		if (!running)
			return;

		running = false;

		OLogManager.instance().info(this, "OrientDB Server is shutdowning...");

		try {
			lock.writeLock().lock();

			// SHUTDOWN LISTENERS
			for (OServerNetworkListener l : listeners) {
				OLogManager.instance().info(this, "Shutdowning connection listener '" + l + "'...");
				l.shutdown();
			}

			// SHUTDOWN HANDLERS
			for (OServerHandler h : handlers) {
				OLogManager.instance().info(this, "Shutdowning handler %s...", h.getName());
				try {
					h.shutdown();
				} catch (Throwable t) {
				}
			}

			// PROTOCOL HANDLERS
			protocols.clear();
			try {
				MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
				mBeanServer.unregisterMBean(onProfiler);
				mBeanServer.unregisterMBean(onServer);
			} catch (Exception e) {
				OLogManager.instance().error(this, "OrientDB Server v" + OConstants.ORIENT_VERSION + " unregisterMBean error.", e);
			}
			Orient.instance().shutdown();

		} finally {
			lock.writeLock().unlock();
		}

		OLogManager.instance().info(this, "OrientDB Server shutdown complete");
		System.out.println();
	}

	public String getStoragePath(final String iName) {
		final String name = iName.indexOf(':') > -1 ? iName.substring(iName.indexOf(':') + 1) : iName;

		final OStorage stg = Orient.instance().getStorage(name);
		if (stg != null)
			// ALREADY OPEN
			return stg.getURL();

		// SEARCH IN CONFIGURED PATHS
		String dbPath = configuration.getStoragePath(name);

		if (dbPath == null) {
			// SEARCH IN DEFAULT DATABASE DIRECTORY
			dbPath = OSystemVariableResolver.resolveSystemVariables("${ORIENTDB_HOME}/databases/" + name + "/");
			File f = new File(dbPath + "default.odh");
			if (!f.exists())
				throw new OConfigurationException("Database '" + name + "' is not configured on server");

			dbPath = "local:${ORIENTDB_HOME}/databases/" + name;
		}

		return dbPath;
	}

	public Map<String, String> getAvailableStorageNames() {
		// SEARCH IN CONFIGURED PATHS
		final Map<String, String> storages = new HashMap<String, String>();
		if (configuration.storages != null && configuration.storages.length > 0)
			for (OServerStorageConfiguration s : configuration.storages)
				storages.put(s.name, s.path);

		// SEARCH IN DEFAULT DATABASE DIRECTORY
		final String rootDirectory = getDatabaseDirectory();
		scanDatabaseDirectory(rootDirectory, new File(rootDirectory), storages);

		for (OStorage storage : Orient.instance().getStorages()) {
			final String storageName = storage.getName();
			if (!storages.containsKey(storageName))
				storages.put(storageName, storage.getURL());
		}

		return storages;
	}

	public String getDatabaseDirectory() {
		return OSystemVariableResolver.resolveSystemVariables("${ORIENTDB_HOME}/databases/");
	}

	public ThreadGroup getServerThreadGroup() {
		return threadGroup;
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
		final OServerUserConfiguration user = getUser(iUserName);

		if (user != null && (iPassword == null || user.password.equals(iPassword))) {
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

	public OServerUserConfiguration getUser(final String iUserName) {
		return configuration.getUser(iUserName);
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

	public Map<String, Class<? extends ONetworkProtocol>> getProtocols() {
		return protocols;
	}

	public List<OServerNetworkListener> getListeners() {
		return listeners;
	}

	@SuppressWarnings("unchecked")
	public <RET extends OServerNetworkListener> RET getListenerByProtocol(final Class<? extends ONetworkProtocol> iProtocolClass) {
		for (OServerNetworkListener l : listeners)
			if (l.getProtocolType().equals(iProtocolClass))
				return (RET) l;

		return null;
	}

	public OrientServer getManagedServer() {
		return managedServer;
	}

	public static String getOrientHome() {
		String v = System.getenv("ORIENTDB_HOME");

		if (v == null)
			v = System.getProperty("orient.home");

		return v;
	}

	public List<OServerHandler> getHandlers() {
		return handlers;
	}

	public OContextConfiguration getContextConfiguration() {
		return contextConfiguration;
	}

	@SuppressWarnings("unchecked")
	public <RET extends OServerHandler> RET getHandler(final Class<RET> iHandlerClass) {
		for (OServerHandler h : handlers)
			if (h.getClass().equals(iHandlerClass))
				return (RET) h;

		return null;
	}

	protected void loadConfiguration(final OServerConfiguration iConfiguration) {
		try {
			configuration = iConfiguration;

			// FILL THE CONTEXT CONFIGURATION WITH SERVER'S PARAMETERS
			contextConfiguration = new OContextConfiguration();
			if (iConfiguration.properties != null)
				for (OServerEntryConfiguration prop : iConfiguration.properties)
					contextConfiguration.setValue(prop.name, prop.value);

			loadStorages();
			loadUsers();

		} catch (IOException e) {
			OLogManager.instance().error(this, "Error on reading server configuration.", OConfigurationException.class);
		}
	}

	protected OServerConfiguration loadConfigurationFromFile(final File iFile) {
		try {
			configurationLoader = new OServerConfigurationLoaderXml(OServerConfiguration.class, iFile);
			return configurationLoader.load();

		} catch (IOException e) {
			OLogManager.instance().error(this, "Error on reading server configuration from file: " + iFile, e,
					OConfigurationException.class);
		}
		return null;
	}

	protected void loadUsers() throws IOException {
		if (configuration.users != null && configuration.users.length > 0) {
			for (OServerUserConfiguration u : configuration.users) {
				if (u.name.equals(OServerConfiguration.SRV_ROOT_ADMIN))
					// FOUND
					return;
			}
		}

		createAdminAndDbListerUsers();
	}

	/**
	 * Load configured storages.
	 */
	protected void loadStorages() {
		if (configuration.storages == null)
			return;

		String type;
		for (OServerStorageConfiguration stg : configuration.storages)
			if (stg.loadOnStartup) {
				// @COMPATIBILITY
				if (stg.userName == null)
					stg.userName = "admin";
				if (stg.userPassword == null)
					stg.userPassword = "admin";

				type = stg.path.substring(0, stg.path.indexOf(':'));

				ODatabaseDocument db = null;
				try {
					db = new ODatabaseDocumentTx(stg.path);

					if (db.exists())
						db.open(stg.userName, stg.userPassword);
					else
						db.create();

					OLogManager.instance().info(this, "-> Loaded " + type + " database '" + stg.name + "'");
				} catch (Exception e) {
					OLogManager.instance().error(this, "-> Can't load " + type + " database '" + stg.name + "': " + e);

				} finally {
					if (db != null)
						db.close();
				}
			}
	}

	public void addUser(final String iName, String iPassword, final String iPermissions) throws IOException {
		if (iName == null || iName.length() == 0)
			throw new IllegalArgumentException("User name null or empty");

		if (iPermissions == null || iPermissions.length() == 0)
			throw new IllegalArgumentException("User permissions null or empty");

		configuration.users = Arrays.copyOf(configuration.users, configuration.users.length + 1);

		if (iPassword == null)
			// AUTO GENERATE PASSWORD
			iPassword = OSecurityManager.instance().digest2String(String.valueOf(new Random(System.currentTimeMillis()).nextLong()),
					false);

		configuration.users[configuration.users.length - 1] = new OServerUserConfiguration(iName, iPassword, iPermissions);

		saveConfiguration();
	}

	protected void createAdminAndDbListerUsers() throws IOException {
		addUser(OServerConfiguration.SRV_ROOT_ADMIN, null, "*");
		addUser(OServerConfiguration.SRV_ROOT_GUEST, OServerConfiguration.SRV_ROOT_GUEST, "connect,server.listDatabases");
		saveConfiguration();
	}

	protected void registerHandlers() throws InstantiationException, IllegalAccessException, ClassNotFoundException {
		if (configuration.handlers != null) {
			// ACTIVATE HANDLERS
			OServerHandler handler;
			for (OServerHandlerConfiguration h : configuration.handlers) {
				handler = (OServerHandler) Class.forName(h.clazz).newInstance();
				handlers.add(handler);

				handler.config(this, h.parameters);
				handler.startup();
			}
		}
	}

	protected void defaultSettings() {
		// OGlobalConfiguration.CACHE_LEVEL2_ENABLED.setValue(Boolean.FALSE);
		// OGlobalConfiguration.CACHE_LEVEL2_SIZE.setValue(0);
		OGlobalConfiguration.CACHE_LEVEL1_ENABLED.setValue(Boolean.FALSE);
		OGlobalConfiguration.CACHE_LEVEL1_SIZE.setValue(0);
		OGlobalConfiguration.FILE_LOCK.setValue(true);
		// OGlobalConfiguration.MVRBTREE_LAZY_UPDATES.setValue(1);
		// OGlobalConfiguration.LAZYSET_WORK_ON_STREAM.setValue(false);
		OGlobalConfiguration.TX_USE_LOG.setValue(true);
		OGlobalConfiguration.TX_COMMIT_SYNCH.setValue(true);
	}

	protected void scanDatabaseDirectory(final String iRootDirectory, final File iDirectory, final Map<String, String> iStorages) {
		if (iDirectory.exists() && iDirectory.isDirectory()) {
			for (File db : iDirectory.listFiles()) {
				if (db.isDirectory()) {
					final File f = new File(db.getAbsolutePath() + "/default.odh");
					if (f.exists()) {
						final String dbPath = db.getPath().replace('\\', '/');
						// FOUND DB FOLDER
						iStorages.put(dbPath.substring(iRootDirectory.length()), "local:" + dbPath);
					} else
						// TRY TO GO IN DEEP RECURSIVELY
						scanDatabaseDirectory(iRootDirectory, db, iStorages);
				}
			}
		}
	}
}
