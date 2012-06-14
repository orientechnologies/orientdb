/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.ReentrantLock;

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
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseComplex;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.exception.OSecurityAccessException;
import com.orientechnologies.orient.core.exception.OSecurityException;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.OUser;
import com.orientechnologies.orient.core.security.OSecurityManager;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.impl.memory.OStorageMemory;
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
  protected ReentrantLock                                  lock               = new ReentrantLock();

  protected volatile boolean                               running            = true;
  protected OServerConfigurationLoaderXml                  configurationLoader;
  protected OServerConfiguration                           configuration;
  protected OContextConfiguration                          contextConfiguration;
  protected OServerShutdownHook                            shutdownHook;
  protected Map<String, OServerHandler>                    plugins            = new HashMap<String, OServerHandler>();
  protected Map<String, Class<? extends ONetworkProtocol>> networkProtocols   = new HashMap<String, Class<? extends ONetworkProtocol>>();
  protected List<OServerNetworkListener>                   networkListeners   = new ArrayList<OServerNetworkListener>();
  protected List<OServerLifecycleListener>                 lifecycleListeners = new ArrayList<OServerLifecycleListener>();
  protected static ThreadGroup                             threadGroup;

  private OrientServer                                     managedServer;
  private ObjectName                                       onProfiler         = new ObjectName("OrientDB:type=Profiler");
  private ObjectName                                       onServer           = new ObjectName("OrientDB:type=Server");
  private final CountDownLatch                             startupLatch       = new CountDownLatch(1);

  private Random                                           random             = new Random();
  private Map<String, Object>                              variables          = new HashMap<String, Object>();

  public OServer() throws ClassNotFoundException, MalformedObjectNameException, NullPointerException,
      InstanceAlreadyExistsException, MBeanRegistrationException, NotCompliantMBeanException {
    defaultSettings();

    OLogManager.installCustomFormatter();

    threadGroup = new ThreadGroup("OrientDB Server");

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
    // Startup function split to allow pre-activation changes
    startup(loadConfigurationFromFile(iConfigurationFile));
  }

  public void startup(final String iConfiguration) throws InstantiationException, IllegalAccessException, ClassNotFoundException,
      IllegalArgumentException, SecurityException, InvocationTargetException, NoSuchMethodException, IOException {
    startup(new ByteArrayInputStream(iConfiguration.getBytes()));
  }

  public void startup(final InputStream iInputStream) throws InstantiationException, IllegalAccessException,
      ClassNotFoundException, IllegalArgumentException, SecurityException, InvocationTargetException, NoSuchMethodException,
      IOException {

    configurationLoader = new OServerConfigurationLoaderXml(OServerConfiguration.class, iInputStream);
    configuration = configurationLoader.load();

    // Startup function split to allow pre-activation changes
    startup(configuration);
  }

  public void startup(final OServerConfiguration iConfiguration) throws IllegalArgumentException, SecurityException,
      InvocationTargetException, NoSuchMethodException {
    OLogManager.instance().info(this, "OrientDB Server v" + OConstants.getVersion() + " is starting up...");

    Orient.instance();

    loadConfiguration(iConfiguration);
  }

  @SuppressWarnings("unchecked")
  public void activate() throws ClassNotFoundException, InstantiationException, IllegalAccessException {
    for (OServerLifecycleListener l : lifecycleListeners)
      l.onBeforeActivate();

    // REGISTER PROTOCOLS
    for (OServerNetworkProtocolConfiguration p : configuration.network.protocols)
      networkProtocols.put(p.name, (Class<? extends ONetworkProtocol>) Class.forName(p.implementation));

    // STARTUP LISTENERS
    for (OServerNetworkListenerConfiguration l : configuration.network.listeners)
      networkListeners.add(new OServerNetworkListener(this, l.ipAddress, l.portRange, l.protocol, networkProtocols.get(l.protocol),
          l.parameters, l.commands));

    registerPlugins();

    for (OServerLifecycleListener l : lifecycleListeners)
      l.onAfterActivate();

    OLogManager.instance().info(this, "OrientDB Server v" + OConstants.ORIENT_VERSION + " is active.");
    startupLatch.countDown();
  }

  public void shutdown() {
    if (!running)
      return;

    running = false;

    shutdownHook.cancel();

    for (OServerLifecycleListener l : lifecycleListeners)
      l.onBeforeDeactivate();

    OLogManager.instance().info(this, "OrientDB Server is shutdowning...");

    try {
      Orient.instance().shutdown();
    } catch (Throwable e) {
    }

    try {
      lock.lock();

      if (plugins.size() > 0) {
        // SHUTDOWN HANDLERS
        OLogManager.instance().info(this, "Shutdowning plugins:");
        for (OServerHandler h : plugins.values()) {
          OLogManager.instance().info(this, "- %s", h.getName());
          try {
            h.sendShutdown();
          } catch (Throwable t) {
          }
        }
      }

      if (networkProtocols.size() > 0) {
        // PROTOCOL SHUTDOWN
        OLogManager.instance().info(this, "Shutdowning protocols");
        networkProtocols.clear();
      }

      try {
        MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
        mBeanServer.unregisterMBean(onProfiler);
        mBeanServer.unregisterMBean(onServer);
      } catch (Exception e) {
        OLogManager.instance().error(this, "OrientDB Server v" + OConstants.ORIENT_VERSION + " unregisterMBean error.", e);
      }

      if (networkListeners.size() > 0) {
        // SHUTDOWN LISTENERS
        OLogManager.instance().info(this, "Shutdowning listeners:");
        // SHUTDOWN LISTENERS
        for (OServerNetworkListener l : networkListeners) {
          OLogManager.instance().info(this, "- %s", l);
          try {
            l.shutdown();
          } catch (Throwable e) {
          }
        }
      }

    } finally {
      lock.unlock();
    }

    for (OServerLifecycleListener l : lifecycleListeners)
      try {
        l.onAfterDeactivate();
      } catch (Exception e) {
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
      dbPath = OSystemVariableResolver.resolveSystemVariables("${" + Orient.ORIENTDB_HOME + "}/databases/" + name + "/");
      File f = new File(dbPath + "default.odh");
      if (!f.exists())
        throw new OConfigurationException("Database '" + name + "' is not configured on server");

      dbPath = "local:${" + Orient.ORIENTDB_HOME + "}/databases/" + name;
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

  public String getStorageURL(final String iName) {
    // SEARCH IN CONFIGURED PATHS
    if (configuration.storages != null && configuration.storages.length > 0)
      for (OServerStorageConfiguration s : configuration.storages)
        if (s.name.equals(iName))
          return s.path;

    // SEARCH IN DEFAULT DATABASE DIRECTORY
    final Map<String, String> storages = new HashMap<String, String>();
    final String rootDirectory = getDatabaseDirectory();
    scanDatabaseDirectory(rootDirectory, new File(rootDirectory), storages);

    return storages.get(iName);
  }

  public String getDatabaseDirectory() {
    return OSystemVariableResolver.resolveSystemVariables("${" + Orient.ORIENTDB_HOME + "}/databases/");
  }

  public ThreadGroup getServerThreadGroup() {
    return threadGroup;
  }

  public OServerUserConfiguration serverLogin(final String iUser, final String iPassword, final String iResource) {
    if (!authenticate(iUser, iPassword, iResource))
      throw new OSecurityAccessException(
          "Wrong user/password to [connect] to the remote OrientDB Server instance. Get the user/password from the config/orientdb-server-config.xml file");

    return getUser(iUser);
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

  public Map<String, Class<? extends ONetworkProtocol>> getNetworkProtocols() {
    return networkProtocols;
  }

  public List<OServerNetworkListener> getNetworkListeners() {
    return networkListeners;
  }

  @SuppressWarnings("unchecked")
  public <RET extends OServerNetworkListener> RET getListenerByProtocol(final Class<? extends ONetworkProtocol> iProtocolClass) {
    for (OServerNetworkListener l : networkListeners)
      if (l.getProtocolType().equals(iProtocolClass))
        return (RET) l;

    return null;
  }

  public OrientServer getManagedServer() {
    return managedServer;
  }

  public Collection<OServerHandler> getPlugins() {
    return plugins.values();
  }

  public OContextConfiguration getContextConfiguration() {
    return contextConfiguration;
  }

  @SuppressWarnings("unchecked")
  public <RET extends OServerHandler> RET getHandler(final Class<RET> iHandlerClass) {
    try {
      startupLatch.await();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    for (OServerHandler h : plugins.values())
      if (h.getClass().equals(iHandlerClass))
        return (RET) h;

    return null;
  }

  @SuppressWarnings("unchecked")
  public <RET extends OServerHandler> RET getPlugin(final String iName) {
    try {
      startupLatch.await();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    return (RET) plugins.get(iName);
  }

  public Object getVariable(final String iName) {
    return variables.get(iName);
  }

  public OServer setVariable(final String iName, final Object iValue) {
    if (iValue == null)
      variables.remove(iName);
    else
      variables.put(iName, iValue);
    return this;
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
          stg.userName = OUser.ADMIN;
        if (stg.userPassword == null)
          stg.userPassword = OUser.ADMIN;

        type = stg.path.substring(0, stg.path.indexOf(':'));

        ODatabaseDocument db = null;
        try {
          db = new ODatabaseDocumentTx(stg.path);

          if (db.exists())
            db.open(stg.userName, stg.userPassword);
          else {
            db.create();
            if (stg.userName.equals(OUser.ADMIN)) {
              if (!stg.userPassword.equals(OUser.ADMIN))
                // CHANGE ADMIN PASSWORD
                db.getMetadata().getSecurity().getUser(OUser.ADMIN).setPassword(stg.userPassword);
            } else {
              // CREATE A NEW USER AS ADMIN AND REMOVE THE DEFAULT ONE
              db.getMetadata().getSecurity().createUser(stg.userName, stg.userPassword, new String[] { ORole.ADMIN });
              db.getMetadata().getSecurity().dropUser(OUser.ADMIN);
              db.close();
              db.open(stg.userName, stg.userPassword);
            }
          }

          OLogManager.instance().info(this, "-> Loaded " + type + " database '" + stg.name + "'");
        } catch (Exception e) {
          OLogManager.instance().error(this, "-> Cannot load " + type + " database '" + stg.name + "': " + e);

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
      iPassword = OSecurityManager.instance().digest2String(String.valueOf(random.nextLong()), false);

    configuration.users[configuration.users.length - 1] = new OServerUserConfiguration(iName, iPassword, iPermissions);

    saveConfiguration();
  }

  public OServer registerLifecycleListener(final OServerLifecycleListener iListener) {
    lifecycleListeners.add(iListener);
    return this;
  }

  public OServer unregisterLifecycleListener(final OServerLifecycleListener iListener) {
    lifecycleListeners.remove(iListener);
    return this;
  }

  protected void createAdminAndDbListerUsers() throws IOException {
    addUser(OServerConfiguration.SRV_ROOT_ADMIN, null, "*");
    addUser(OServerConfiguration.SRV_ROOT_GUEST, OServerConfiguration.SRV_ROOT_GUEST, "connect,server.listDatabases");
    saveConfiguration();
  }

  protected void registerPlugins() throws InstantiationException, IllegalAccessException, ClassNotFoundException {
    if (configuration.handlers != null) {
      // ACTIVATE PLUGINS
      OServerHandler handler;
      for (OServerHandlerConfiguration h : configuration.handlers) {
        handler = (OServerHandler) Class.forName(h.clazz).newInstance();
        plugins.put(handler.getName(), handler);

        handler.config(this, h.parameters);
        handler.startup();
      }
    }
  }

  protected void defaultSettings() {
    // OGlobalConfiguration.CACHE_LEVEL2_ENABLED.setValue(Boolean.FALSE);
    // OGlobalConfiguration.CACHE_LEVEL2_SIZE.setValue(0);
    // OGlobalConfiguration.CACHE_LEVEL1_ENABLED.setValue(Boolean.FALSE);
    // OGlobalConfiguration.CACHE_LEVEL1_SIZE.setValue(0);
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

  public ODatabaseComplex<?> openDatabase(final String iDbType, final String iDbUrl, final String iUser, final String iPassword) {
    final String path = OServerMain.server().getStoragePath(iDbUrl);

    final ODatabaseComplex<?> database = Orient.instance().getDatabaseFactory().createDatabase(iDbType, path);

    if (database.isClosed())
      if (database.getStorage() instanceof OStorageMemory)
        database.create();
      else {
        try {
          database.open(iUser, iPassword);
        } catch (OSecurityException e) {
          // TRY WITH SERVER'S USER
          try {
            OServerMain.server().serverLogin(iUser, iPassword, "database.passthrough");
          } catch (OSecurityException ex) {
            throw e;
          }

          // SERVER AUTHENTICATED, BYPASS SECURITY
          database.setProperty(ODatabase.OPTIONS.SECURITY.toString(), Boolean.FALSE);
          database.open(iUser, iPassword);
        }
      }

    // ALWAYS DISABLE LEVEl1 CACHE IN SERVER. IT WILL BE ENABLED IF NEEDED BY SINGLE COMMANDS
    // database.getLevel1Cache().setEnable(false);

    return database;
  }
}
