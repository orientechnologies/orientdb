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
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.ReentrantLock;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;

import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.parser.OSystemVariableResolver;
import com.orientechnologies.common.profiler.OAbstractProfiler.OProfilerHookValue;
import com.orientechnologies.common.profiler.OProfilerMBean.METRIC_TYPE;
import com.orientechnologies.orient.core.OConstants;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseComplex;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentPool;
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
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.handler.OConfigurableHooksManager;
import com.orientechnologies.orient.server.network.OServerNetworkListener;
import com.orientechnologies.orient.server.network.protocol.ONetworkProtocol;
import com.orientechnologies.orient.server.plugin.OServerPlugin;
import com.orientechnologies.orient.server.plugin.OServerPluginInfo;
import com.orientechnologies.orient.server.plugin.OServerPluginManager;

public class OServer {
  protected ReentrantLock                                  lock               = new ReentrantLock();

  protected volatile boolean                               running            = true;
  protected OServerConfigurationLoaderXml                  configurationLoader;
  protected OServerConfiguration                           configuration;
  protected OContextConfiguration                          contextConfiguration;
  protected OServerShutdownHook                            shutdownHook;
  protected Map<String, Class<? extends ONetworkProtocol>> networkProtocols   = new HashMap<String, Class<? extends ONetworkProtocol>>();
  protected List<OServerNetworkListener>                   networkListeners   = new ArrayList<OServerNetworkListener>();
  protected List<OServerLifecycleListener>                 lifecycleListeners = new ArrayList<OServerLifecycleListener>();
  protected OServerPluginManager                           pluginManager;
  protected OConfigurableHooksManager                      hookManager;
  protected ODistributedServerManager                      distributedManager;
  private ODatabaseDocumentPool                            dbPool;
  private final CountDownLatch                             startupLatch       = new CountDownLatch(1);
  private Random                                           random             = new Random();
  private Map<String, Object>                              variables          = new HashMap<String, Object>();
  private String                                           databaseDirectory;

  private static ThreadGroup                               threadGroup;
  private static Map<String, OServer>                      distributedServers = new ConcurrentHashMap<String, OServer>();

  public OServer() throws ClassNotFoundException, MalformedObjectNameException, NullPointerException,
      InstanceAlreadyExistsException, MBeanRegistrationException, NotCompliantMBeanException {
    defaultSettings();

    OLogManager.installCustomFormatter();

    threadGroup = new ThreadGroup("OrientDB Server");

    OGlobalConfiguration.STORAGE_KEEP_OPEN.setValue(true);
    System.setProperty("com.sun.management.jmxremote", "true");

    Orient.instance().startup();

    if (OGlobalConfiguration.PROFILER_ENABLED.getValueAsBoolean() && !Orient.instance().getProfiler().isRecording())
      Orient.instance().getProfiler().startRecording();

    shutdownHook = new OServerShutdownHook(this);
  }

  public OServer startup() throws InstantiationException, IllegalAccessException, ClassNotFoundException, IllegalArgumentException,
      SecurityException, InvocationTargetException, NoSuchMethodException {
    String config = OServerConfiguration.DEFAULT_CONFIG_FILE;
    if (System.getProperty(OServerConfiguration.PROPERTY_CONFIG_FILE) != null)
      config = System.getProperty(OServerConfiguration.PROPERTY_CONFIG_FILE);

    Orient.instance().startup();

    startup(new File(config));

    Orient
        .instance()
        .getProfiler()
        .registerHookValue("system.databases", "List of databases configured in Server", METRIC_TYPE.TEXT,
            new OProfilerHookValue() {
              @Override
              public Object getValue() {
                final StringBuilder dbs = new StringBuilder();
                for (String dbName : getAvailableStorageNames().keySet()) {
                  if (dbs.length() > 0)
                    dbs.append(',');
                  dbs.append(dbName);
                }
                return dbs.toString();
              }
            });

    return this;
  }

  public OServer startup(final File iConfigurationFile) throws InstantiationException, IllegalAccessException,
      ClassNotFoundException, IllegalArgumentException, SecurityException, InvocationTargetException, NoSuchMethodException {
    // Startup function split to allow pre-activation changes
    return startup(loadConfigurationFromFile(iConfigurationFile));
  }

  public OServer startup(final String iConfiguration) throws InstantiationException, IllegalAccessException,
      ClassNotFoundException, IllegalArgumentException, SecurityException, InvocationTargetException, NoSuchMethodException,
      IOException {
    return startup(new ByteArrayInputStream(iConfiguration.getBytes()));
  }

  public OServer startup(final InputStream iInputStream) throws InstantiationException, IllegalAccessException,
      ClassNotFoundException, IllegalArgumentException, SecurityException, InvocationTargetException, NoSuchMethodException,
      IOException {
    if (iInputStream == null)
      throw new OConfigurationException("Configuration file is null");

    configurationLoader = new OServerConfigurationLoaderXml(OServerConfiguration.class, iInputStream);
    configuration = configurationLoader.load();

    // Startup function split to allow pre-activation changes
    return startup(configuration);
  }

  public OServer startup(final OServerConfiguration iConfiguration) throws IllegalArgumentException, SecurityException,
      InvocationTargetException, NoSuchMethodException {
    OLogManager.instance().info(this, "OrientDB Server v" + OConstants.getVersion() + " is starting up...");

    Orient.instance();

    loadConfiguration(iConfiguration);

    if (OGlobalConfiguration.USE_NODE_ID_CLUSTER_POSITION.getValueAsBoolean())
      OLogManager.instance().info(
          this,
          "ONodeId will be used as presentation of cluster position, " + " please do not forget to set "
              + OGlobalConfiguration.USE_NODE_ID_CLUSTER_POSITION.getKey() + " property to \"true\" value on client side ...");

    if (OGlobalConfiguration.ENVIRONMENT_DUMP_CFG_AT_STARTUP.getValueAsBoolean()) {
      System.out.println("Dumping environment after server startup...");
      OGlobalConfiguration.dumpConfiguration(System.out);
    }

    dbPool = new ODatabaseDocumentPool();
    dbPool.setup(contextConfiguration.getValueAsInteger(OGlobalConfiguration.DB_POOL_MIN),
        contextConfiguration.getValueAsInteger(OGlobalConfiguration.DB_POOL_MAX));

    databaseDirectory = contextConfiguration.getValue("server.database.path", "${" + Orient.ORIENTDB_HOME + "}/databases/");
    databaseDirectory = OSystemVariableResolver.resolveSystemVariables(databaseDirectory);
    databaseDirectory = databaseDirectory.replace("//", "/");

    OLogManager.instance().info(this, "Databases directory: " + new File(databaseDirectory).getAbsolutePath());

    return this;
  }

  @SuppressWarnings("unchecked")
  public OServer activate() throws ClassNotFoundException, InstantiationException, IllegalAccessException {
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

    return this;
  }

  public void shutdown() {
    if (!running)
      return;

    running = false;

    shutdownHook.cancel();

    Orient.instance().getProfiler().unregisterHookValue("system.databases");

    for (OServerLifecycleListener l : lifecycleListeners)
      l.onBeforeDeactivate();

    OLogManager.instance().info(this, "OrientDB Server is shutting down...");

    if (!Orient.isRegisterDatabaseByPath())
      try {
        Orient.instance().shutdown();
      } catch (Throwable e) {
        OLogManager.instance().error(this, "Error during OrientDB shutdown", e);
      }

    lock.lock();
    try {

      final String[] plugins = pluginManager.getPluginNames();
      if (plugins.length > 0) {
        // SHUTDOWN HANDLERS
        OLogManager.instance().info(this, "Shutting down plugins:");
        for (String pluginName : plugins) {

          OLogManager.instance().info(this, "- %s", pluginName);
          final OServerPluginInfo plugin = pluginManager.getPluginByName(pluginName);
          try {
            plugin.shutdown();
          } catch (Throwable t) {
            OLogManager.instance().error(this, "Error during server plugin %s shutdown.", t, plugin);
          }
        }
      }

      if (networkProtocols.size() > 0) {
        // PROTOCOL SHUTDOWN
        OLogManager.instance().info(this, "Shutting down protocols");
        networkProtocols.clear();
      }

      if (networkListeners.size() > 0) {
        // SHUTDOWN LISTENERS
        OLogManager.instance().info(this, "Shutting down listeners:");
        // SHUTDOWN LISTENERS
        for (OServerNetworkListener l : networkListeners) {
          OLogManager.instance().info(this, "- %s", l);
          try {
            l.shutdown();
          } catch (Throwable e) {
            OLogManager.instance().error(this, "Error during shutdown of listener %s.", e, l);
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
        OLogManager.instance().error(this, "Error during deactivation of server lifecycle listener %s", e, l);
      }

    OLogManager.instance().info(this, "OrientDB Server shutdown complete");
    System.out.println();
  }

  public String getStoragePath(final String iName) {
    if (iName == null)
      throw new IllegalArgumentException("Storage path is null");

    final String name = iName.indexOf(':') > -1 ? iName.substring(iName.indexOf(':') + 1) : iName;

    final String dbName = Orient.isRegisterDatabaseByPath() ? getDatabaseDirectory() + name : name;
    final String dbPath = Orient.isRegisterDatabaseByPath() ? dbName : getDatabaseDirectory() + name;

    final OStorage stg = Orient.instance().getStorage(dbName);
    if (stg != null)
      // ALREADY OPEN
      return stg.getURL();

    // SEARCH IN CONFIGURED PATHS
    String dbURL = configuration.getStoragePath(name);
    if (dbURL == null) {
      // SEARCH IN DEFAULT DATABASE DIRECTORY
      if (new File(OIOUtils.getPathFromDatabaseName(dbPath) + "/default.odh").exists())
        dbURL = "local:" + dbPath;
      else if (new File(OIOUtils.getPathFromDatabaseName(dbPath) + "/default.pcl").exists())
        dbURL = "plocal:" + dbPath;
      else
        throw new OConfigurationException("Database '" + name + "' is not configured on server");
    }

    return dbURL;
  }

  public Map<String, String> getAvailableStorageNames() {
    // SEARCH IN CONFIGURED PATHS
    final Map<String, String> storages = new HashMap<String, String>();
    if (configuration.storages != null && configuration.storages.length > 0)
      for (OServerStorageConfiguration s : configuration.storages)
        storages.put(OIOUtils.getDatabaseNameFromPath(s.name), s.path);

    // SEARCH IN DEFAULT DATABASE DIRECTORY
    final String rootDirectory = getDatabaseDirectory();
    scanDatabaseDirectory(rootDirectory, new File(rootDirectory), storages);

    for (OStorage storage : Orient.instance().getStorages()) {
      final String storageUrl = storage.getURL();
      if (storage.exists() && !storages.containsValue(storageUrl))
        storages.put(OIOUtils.getDatabaseNameFromPath(storage.getName()), storageUrl);
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
    return databaseDirectory;
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
    if (configurationLoader != null)
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
      if (iProtocolClass.isAssignableFrom(l.getProtocolType()))
        return (RET) l;

    return null;
  }

  public Collection<OServerPluginInfo> getPlugins() {
    return pluginManager.getPlugins();
  }

  public OContextConfiguration getContextConfiguration() {
    return contextConfiguration;
  }

  @SuppressWarnings("unchecked")
  public <RET extends OServerPlugin> RET getPluginByClass(final Class<RET> iPluginClass) {
    try {
      startupLatch.await();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    for (OServerPluginInfo h : getPlugins())
      if (h.getInstance() != null && h.getInstance().getClass().equals(iPluginClass))
        return (RET) h.getInstance();

    return null;
  }

  @SuppressWarnings("unchecked")
  public <RET extends OServerPlugin> RET getPlugin(final String iName) {
    try {
      startupLatch.await();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    final OServerPluginInfo p = pluginManager.getPluginByName(iName);
    if (p != null)
      return (RET) p.getInstance();
    return null;
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
      hookManager = new OConfigurableHooksManager(iConfiguration);

    } catch (IOException e) {
      OLogManager.instance().error(this, "Error on reading server configuration.", OConfigurationException.class, e);
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

        int idx = stg.path.indexOf(':');
        if (idx == -1) {
          OLogManager.instance().error(this, "-> Invalid path '" + stg.path + "' for database '" + stg.name + "'");
          return;
        }
        type = stg.path.substring(0, idx);

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

    if (configuration.users == null)
      configuration.users = new OServerUserConfiguration[1];
    else
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
    addUser(OServerConfiguration.SRV_ROOT_GUEST, OServerConfiguration.SRV_ROOT_GUEST, "connect,server.listDatabases,server.dblist");
    saveConfiguration();
  }

  protected void registerPlugins() throws InstantiationException, IllegalAccessException, ClassNotFoundException {
    pluginManager = new OServerPluginManager();
    pluginManager.config(this);
    pluginManager.startup();

    // PLUGINS CONFIGURED IN XML
    if (configuration.handlers != null) {
      // ACTIVATE PLUGINS
      OServerPlugin handler;
      for (OServerHandlerConfiguration h : configuration.handlers) {
        handler = (OServerPlugin) Class.forName(h.clazz).newInstance();

        if (handler instanceof ODistributedServerManager)
          distributedManager = (ODistributedServerManager) handler;

        pluginManager.registerPlugin(new OServerPluginInfo(handler.getName(), null, null, null, handler, null, 0, null));

        handler.config(this, h.parameters);
        handler.startup();
      }
    }
  }

  protected void defaultSettings() {
    OGlobalConfiguration.TX_USE_LOG.setValue(true);
    OGlobalConfiguration.TX_COMMIT_SYNCH.setValue(true);
  }

  protected void scanDatabaseDirectory(final String rootDirectory, final File directory, final Map<String, String> storages) {
    if (directory.exists() && directory.isDirectory()) {
      for (File db : directory.listFiles()) {
        if (db.isDirectory()) {
          final File localFile = new File(db.getAbsolutePath() + "/default.odh");
          final File plocalFile = new File(db.getAbsolutePath() + "/default.pcl");
          if (localFile.exists()) {
            final String dbPath = db.getPath().replace('\\', '/');
            // FOUND DB FOLDER
            storages.put(OIOUtils.getDatabaseNameFromPath(dbPath.substring(rootDirectory.length())), "local:" + dbPath);
          } else if (plocalFile.exists()) {
            final String dbPath = db.getPath().replace('\\', '/');

            storages.put(OIOUtils.getDatabaseNameFromPath(dbPath.substring(rootDirectory.length())), "plocal:" + dbPath);
          } else
            // TRY TO GO IN DEEP RECURSIVELY
            scanDatabaseDirectory(rootDirectory, db, storages);
        }
      }
    }
  }

  public ODatabaseComplex<?> openDatabase(final String iDbType, final String iDbUrl, final String iUser, final String iPassword) {
    final String path = getStoragePath(iDbUrl);

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
            serverLogin(iUser, iPassword, "database.passthrough");
          } catch (OSecurityException ex) {
            throw e;
          }

          // SERVER AUTHENTICATED, BYPASS SECURITY
          database.setProperty(ODatabase.OPTIONS.SECURITY.toString(), Boolean.FALSE);
          database.open(iUser, iPassword);
        }
      }

    return database;
  }

  public ODistributedServerManager getDistributedManager() {
    return distributedManager;
  }

  public ODatabaseDocumentPool getDatabasePool() {
    return dbPool;
  }

  public static OServer getInstance(final String iServerId) {
    return distributedServers.get(iServerId);
  }

  public static void registerServerInstance(final String iServerId, final OServer iServer) {
    distributedServers.put(iServerId, iServer);
  }
}
