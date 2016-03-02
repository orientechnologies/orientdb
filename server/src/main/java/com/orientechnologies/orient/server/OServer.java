/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://www.orientechnologies.com
 *
 */
package com.orientechnologies.orient.server;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.security.SecureRandom;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.ReentrantLock;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;

import com.orientechnologies.common.console.DefaultConsoleReader;
import com.orientechnologies.common.console.OConsoleReader;
import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.parser.OSystemVariableResolver;
import com.orientechnologies.common.profiler.OAbstractProfiler;
import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.orient.core.OConstants;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.db.OPartitionedDatabasePoolFactory;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.*;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.OToken;
import com.orientechnologies.orient.core.metadata.security.OUser;
import com.orientechnologies.orient.core.security.OSecurityManager;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OLocalPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.memory.ODirectMemoryStorage;
import com.orientechnologies.orient.server.config.*;
import com.orientechnologies.orient.server.distributed.ODistributedAbstractPlugin;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.handler.OConfigurableHooksManager;
import com.orientechnologies.orient.server.network.OServerNetworkListener;
import com.orientechnologies.orient.server.network.OServerSocketFactory;
import com.orientechnologies.orient.server.network.protocol.ONetworkProtocol;
import com.orientechnologies.orient.server.network.protocol.ONetworkProtocolData;
import com.orientechnologies.orient.server.plugin.OServerPlugin;
import com.orientechnologies.orient.server.plugin.OServerPluginInfo;
import com.orientechnologies.orient.server.plugin.OServerPluginManager;
import com.orientechnologies.orient.server.security.OSecurityServerUser;

public class OServer {
  private static final String                              ROOT_PASSWORD_VAR      = "ORIENTDB_ROOT_PASSWORD";
  private static ThreadGroup                               threadGroup;
  private static Map<String, OServer>                      distributedServers     = new ConcurrentHashMap<String, OServer>();
  private final CountDownLatch                             startupLatch           = new CountDownLatch(1);
  protected ReentrantLock                                  lock                   = new ReentrantLock();
  protected volatile boolean                               running                = false;
  protected OServerConfigurationLoaderXml                  configurationLoader;
  protected OServerConfiguration                           configuration;
  protected OContextConfiguration                          contextConfiguration;
  protected OServerShutdownHook                            shutdownHook;
  protected Map<String, Class<? extends ONetworkProtocol>> networkProtocols       = new HashMap<String, Class<? extends ONetworkProtocol>>();
  protected Map<String, OServerSocketFactory>              networkSocketFactories = new HashMap<String, OServerSocketFactory>();
  protected List<OServerNetworkListener>                   networkListeners       = new ArrayList<OServerNetworkListener>();
  protected List<OServerLifecycleListener>                 lifecycleListeners     = new ArrayList<OServerLifecycleListener>();
  protected OServerPluginManager                           pluginManager;
  protected OConfigurableHooksManager                      hookManager;
  protected ODistributedServerManager                      distributedManager;
  private OPartitionedDatabasePoolFactory                  dbPoolFactory;
  private SecureRandom                                     random                 = new SecureRandom();
  private Map<String, Object>                              variables              = new HashMap<String, Object>();
  private String                                           serverRootDirectory;
  private String                                           databaseDirectory;
  private final boolean                                    shutdownEngineOnExit;
  private OClientConnectionManager                         clientConnectionManager;

  private ClassLoader                                      extensionClassLoader;

  public OServer() throws ClassNotFoundException, MalformedObjectNameException, NullPointerException,
      InstanceAlreadyExistsException, MBeanRegistrationException, NotCompliantMBeanException {
    this(true);
  }

  public OServer(boolean shutdownEngineOnExit) throws ClassNotFoundException, MalformedObjectNameException, NullPointerException,
      InstanceAlreadyExistsException, MBeanRegistrationException, NotCompliantMBeanException {
    this.shutdownEngineOnExit = shutdownEngineOnExit;

    serverRootDirectory = OSystemVariableResolver.resolveSystemVariables("${" + Orient.ORIENTDB_HOME + "}", ".");

    OLogManager.installCustomFormatter();

    defaultSettings();

    threadGroup = new ThreadGroup("OrientDB Server");

    System.setProperty("com.sun.management.jmxremote", "true");

    Orient.instance().startup();

    if (OGlobalConfiguration.PROFILER_ENABLED.getValueAsBoolean() && !Orient.instance().getProfiler().isRecording())
      Orient.instance().getProfiler().startRecording();

    shutdownHook = new OServerShutdownHook(this);
  }

  /**
   * Set the preferred {@link ClassLoader} used to load extensions.
   *
   * @since 2.1
   */
  public void setExtensionClassLoader(/* @Nullable */final ClassLoader extensionClassLoader) {
    this.extensionClassLoader = extensionClassLoader;
  }

  /**
   * Get the preferred {@link ClassLoader} used to load extensions.
   *
   * @since 2.1
   */
  /* @Nullable */
  public ClassLoader getExtensionClassLoader() {
    return extensionClassLoader;
  }

  public boolean isActive() {
    return running;
  }

  public OClientConnectionManager getClientConnectionManager() {
    return clientConnectionManager;
  }

  public void restart() throws ClassNotFoundException, InvocationTargetException, InstantiationException, NoSuchMethodException,
      IllegalAccessException {
    try {
      shutdown();
    } finally {
      startup(configuration);
      activate();
    }
  }

  /**
   * Load an extension class by name.
   */
  private Class<?> loadClass(final String name) throws ClassNotFoundException {
    Class<?> loaded = tryLoadClass(extensionClassLoader, name);
    if (loaded == null) {
      loaded = tryLoadClass(Thread.currentThread().getContextClassLoader(), name);
      if (loaded == null) {
        loaded = tryLoadClass(getClass().getClassLoader(), name);
        if (loaded == null) {
          loaded = Class.forName(name);
        }
      }
    }
    return loaded;
  }

  /**
   * Attempt to load a class from given class-loader.
   */
  /* @Nullable */
  private Class<?> tryLoadClass(/* @Nullable */final ClassLoader classLoader, final String name) {
    if (classLoader != null) {
      try {
        return classLoader.loadClass(name);
      } catch (ClassNotFoundException e) {
        // ignore
      }
    }
    return null;
  }

  public static OServer getInstance(final String iServerId) {
    return distributedServers.get(iServerId);
  }

  public static OServer getInstanceByPath(final String iPath) {
    for (Map.Entry<String, OServer> entry : distributedServers.entrySet()) {
      if (iPath.startsWith(entry.getValue().getDatabaseDirectory()))
        return entry.getValue();
    }
    return null;
  }

  public static void registerServerInstance(final String iServerId, final OServer iServer) {
    distributedServers.put(iServerId, iServer);
  }

  public OServer startup() throws InstantiationException, IllegalAccessException, ClassNotFoundException, IllegalArgumentException,
      SecurityException, InvocationTargetException, NoSuchMethodException {
    String config = OServerConfiguration.DEFAULT_CONFIG_FILE;
    if (System.getProperty(OServerConfiguration.PROPERTY_CONFIG_FILE) != null)
      config = System.getProperty(OServerConfiguration.PROPERTY_CONFIG_FILE);

    Orient.instance().startup();

    startup(new File(OSystemVariableResolver.resolveSystemVariables(config)));

    return this;
  }

  public OServer startup(final File iConfigurationFile) throws InstantiationException, IllegalAccessException,
      ClassNotFoundException, IllegalArgumentException, SecurityException, InvocationTargetException, NoSuchMethodException {
    // Startup function split to allow pre-activation changes
    return startup(loadConfigurationFromFile(iConfigurationFile));
  }

  public OServer startup(final String iConfiguration) throws InstantiationException, IllegalAccessException, ClassNotFoundException,
      IllegalArgumentException, SecurityException, InvocationTargetException, NoSuchMethodException, IOException {
    return startup(new ByteArrayInputStream(iConfiguration.getBytes()));
  }

  public OServer startup(final InputStream iInputStream)
      throws InstantiationException, IllegalAccessException, ClassNotFoundException, IllegalArgumentException, SecurityException,
      InvocationTargetException, NoSuchMethodException, IOException {
    if (iInputStream == null)
      throw new OConfigurationException("Configuration file is null");

    configurationLoader = new OServerConfigurationLoaderXml(OServerConfiguration.class, iInputStream);
    configuration = configurationLoader.load();

    // Startup function split to allow pre-activation changes
    return startup(configuration);
  }

  public OServer startup(final OServerConfiguration iConfiguration)
      throws IllegalArgumentException, SecurityException, InvocationTargetException, NoSuchMethodException {
    OLogManager.instance().info(this, "OrientDB Server v" + OConstants.getVersion() + " is starting up...");

    Orient.instance();

    clientConnectionManager = new OClientConnectionManager();

    loadConfiguration(iConfiguration);

    if (OGlobalConfiguration.ENVIRONMENT_DUMP_CFG_AT_STARTUP.getValueAsBoolean()) {
      System.out.println("Dumping environment after server startup...");
      OGlobalConfiguration.dumpConfiguration(System.out);
    }

    dbPoolFactory = new OPartitionedDatabasePoolFactory();
    dbPoolFactory.setMaxPoolSize(contextConfiguration.getValueAsInteger(OGlobalConfiguration.DB_POOL_MAX));

    databaseDirectory = contextConfiguration.getValue("server.database.path", serverRootDirectory + "/databases/");
    databaseDirectory = OFileUtils.getPath(OSystemVariableResolver.resolveSystemVariables(databaseDirectory));
    databaseDirectory = databaseDirectory.replace("//", "/");
    if (!databaseDirectory.endsWith("/"))
      databaseDirectory += "/";

    OLogManager.instance().info(this, "Databases directory: " + new File(databaseDirectory).getAbsolutePath());

    Orient.instance().getProfiler().registerHookValue("system.databases", "List of databases configured in Server",
        OProfiler.METRIC_TYPE.TEXT, new OAbstractProfiler.OProfilerHookValue() {
          @Override
          public Object getValue() {
            final StringBuilder dbs = new StringBuilder(64);
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

  @SuppressWarnings("unchecked")
  public OServer activate() throws ClassNotFoundException, InstantiationException, IllegalAccessException {
    try {
      for (OServerLifecycleListener l : lifecycleListeners)
        l.onBeforeActivate();

      if (configuration.network != null) {
        // REGISTER/CREATE SOCKET FACTORIES
        if (configuration.network.sockets != null) {
          for (OServerSocketFactoryConfiguration f : configuration.network.sockets) {
            Class<? extends OServerSocketFactory> fClass = (Class<? extends OServerSocketFactory>) loadClass(f.implementation);
            OServerSocketFactory factory = fClass.newInstance();
            try {
              factory.config(f.name, f.parameters);
              networkSocketFactories.put(f.name, factory);
            } catch (OConfigurationException e) {
              OLogManager.instance().error(this, "Error creating socket factory", e);
            }
          }
        }

        // REGISTER PROTOCOLS
        for (OServerNetworkProtocolConfiguration p : configuration.network.protocols)
          networkProtocols.put(p.name, (Class<? extends ONetworkProtocol>) loadClass(p.implementation));

        // STARTUP LISTENERS
        for (OServerNetworkListenerConfiguration l : configuration.network.listeners)
          networkListeners.add(new OServerNetworkListener(this, networkSocketFactories.get(l.socket), l.ipAddress, l.portRange,
              l.protocol, networkProtocols.get(l.protocol), l.parameters, l.commands));

      } else
        OLogManager.instance().warn(this, "Network configuration was not found");

      try {
        loadStorages();
        loadUsers();
      } catch (IOException e) {
        OLogManager.instance().error(this, "Error on reading server configuration", e, OConfigurationException.class);
      }

      registerPlugins();

      for (OServerLifecycleListener l : lifecycleListeners)
        l.onAfterActivate();

      running = true;

      OLogManager.instance().info(this, "OrientDB Server v" + OConstants.getVersion() + " is active.");
    } catch (ClassNotFoundException e) {
      running = false;
      throw e;
    } catch (InstantiationException e) {
      running = false;
      throw e;
    } catch (IllegalAccessException e) {
      running = false;
      throw e;
    } catch (RuntimeException e) {
      running = false;
      throw e;
    } finally {
      startupLatch.countDown();
    }

    return this;
  }

  public void removeShutdownHook() {
    if (shutdownHook != null) {
      shutdownHook.cancel();
      shutdownHook = null;
    }
  }

  public boolean shutdown() {
    if (!running)
      return false;

    running = false;

    OLogManager.instance().info(this, "OrientDB Server is shutting down...");

    if (shutdownHook != null)
      shutdownHook.cancel();

    Orient.instance().getProfiler().unregisterHookValue("system.databases");

    for (OServerLifecycleListener l : lifecycleListeners)
      l.onBeforeDeactivate();

    lock.lock();
    try {
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

      if (networkProtocols.size() > 0) {
        // PROTOCOL SHUTDOWN
        OLogManager.instance().info(this, "Shutting down protocols");
        networkProtocols.clear();
      }

      for (OServerLifecycleListener l : lifecycleListeners)
        try {
          l.onAfterDeactivate();
        } catch (Exception e) {
          OLogManager.instance().error(this, "Error during deactivation of server lifecycle listener %s", e, l);
        }

      if (pluginManager != null)
        pluginManager.shutdown();

      clientConnectionManager.shutdown();

    } finally {
      lock.unlock();
    }

    if (shutdownEngineOnExit && !Orient.isRegisterDatabaseByPath())
      try {
        OLogManager.instance().info(this, "Shutting down databases:");
        Orient.instance().shutdown();
      } catch (Throwable e) {
        OLogManager.instance().error(this, "Error during OrientDB shutdown", e);
      }

    OLogManager.instance().info(this, "OrientDB Server shutdown complete");
    OLogManager.instance().flush();
    return true;
  }

  public String getStoragePath(final String iName) {
    if (iName == null)
      throw new IllegalArgumentException("Storage path is null");

    final String name = iName.indexOf(':') > -1 ? iName.substring(iName.indexOf(':') + 1) : iName;

    final String dbName = Orient.isRegisterDatabaseByPath() ? getDatabaseDirectory() + name : name;
    final String dbPath = Orient.isRegisterDatabaseByPath() ? dbName : getDatabaseDirectory() + name;

    if (dbPath.contains(".."))
      throw new IllegalArgumentException("Storage path is invalid because contains '..'");

    if (dbPath.contains("*"))
      throw new IllegalArgumentException("Storage path is invalid because the wildcard '*'");

    if (dbPath.startsWith("/")) {
      if (!dbPath.startsWith(getDatabaseDirectory()))
        throw new IllegalArgumentException("Storage path is invalid because points to an absolute directory");
    }

    final OStorage stg = Orient.instance().getStorage(dbName);
    if (stg != null)
      // ALREADY OPEN
      return stg.getURL();

    // SEARCH IN CONFIGURED PATHS
    String dbURL = configuration.getStoragePath(name);
    if (dbURL == null) {
      // SEARCH IN DEFAULT DATABASE DIRECTORY
      if (new File(OIOUtils.getPathFromDatabaseName(dbPath) + "/default.pcl").exists())
        dbURL = "plocal:" + dbPath;
      else
        throw new OConfigurationException(
            "Database '" + name + "' is not configured on server (home=" + getDatabaseDirectory() + ")");
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
    scanDatabaseDirectory(new File(rootDirectory), storages);

    for (OStorage storage : Orient.instance().getStorages()) {
      final String storageUrl = storage.getURL();
      // TEST IT'S OF CURRENT SERVER INSTANCE BY CHECKING THE PATH
      if (storage instanceof OAbstractPaginatedStorage && storage.exists() && !storages.containsValue(storageUrl)
          && isStorageOfCurrentServerInstance(storage))
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
    scanDatabaseDirectory(new File(rootDirectory), storages);

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

    if (user == null) {
      return false;
    }

    // Avoid timing attacks:
    // instead of comparing raw strings, hash the strings and compare their digests
    // with a constant-time comparison function.
    final byte[] providedDigest = OSecurityManager.instance().digestSHA256(iPassword);
    final byte[] expectedDigest = OSecurityManager.instance().digestSHA256(user.password);

    if (OSecurityManager.instance().check(providedDigest, expectedDigest)) {
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

  /**
   * Checks if a server user is allowed to operate with a resource.
   *
   * @param iUserName
   *          Username to authenticate
   * @return true if authentication is ok, otherwise false
   */
  public boolean isAllowed(final String iUserName, final String iResourceToCheck) {
    final OServerUserConfiguration user = getUser(iUserName);

    if (user != null) {
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
    return pluginManager != null ? pluginManager.getPlugins() : null;
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
    if (!running)
      throw new ODatabaseException("Error on plugin lookup the server didn't start correcty.");

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
    if (!running)
      throw new ODatabaseException("Error on plugin lookup: the server did not start correctly");

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

  public void addUser(final String iName, String iPassword, final String iPermissions) throws IOException {
    if (iName == null || iName.length() == 0)
      throw new IllegalArgumentException("User name null or empty");

    if (iPermissions == null || iPermissions.length() == 0)
      throw new IllegalArgumentException("User permissions null or empty");

    if (configuration.users == null)
      configuration.users = new OServerUserConfiguration[1];
    else
      configuration.users = Arrays.copyOf(configuration.users, configuration.users.length + 1);

    if (iPassword == null) {
      // AUTO GENERATE PASSWORD
      final byte[] buffer = new byte[32];
      random.nextBytes(buffer);
      iPassword = OSecurityManager.instance().createSHA256(OSecurityManager.byteArrayToHexStr(buffer));
    }

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

  public ODatabase<?> openDatabase(final String iDbType, final String iDbUrl, final OToken iToken) {
    final String path = getStoragePath(iDbUrl);

    final ODatabaseInternal<?> database = Orient.instance().getDatabaseFactory().createDatabase(iDbType, path);

    if (database.isClosed())
      if (database.getStorage() instanceof ODirectMemoryStorage)
        database.create();
      else {
        database.open(iToken);
      }

    return database;
  }

  public ODatabase<?> openDatabase(final String iDbType, final String iDbUrl, final String user, final String password) {
    return openDatabase(iDbType, iDbUrl, user, password, null);
  }

  public ODatabase<?> openDatabase(final String iDbType, final String iDbUrl, final String user, final String password,
      ONetworkProtocolData data) {
    final String path = getStoragePath(iDbUrl);

    final ODatabaseInternal<?> database = Orient.instance().getDatabaseFactory().createDatabase(iDbType, path);

    final OStorage storage = database.getStorage();
    if (database.isClosed()) {
      if (database.getStorage() instanceof ODirectMemoryStorage && !storage.exists()) {
        try {
          database.create();
        } catch (OStorageException e) {
        }
      } else {
        try {
          database.open(user, password);
          if (data != null) {
            data.serverUser = false;
            data.serverUsername = null;
          }
        } catch (OSecurityException e) {
          // TRY WITH SERVER'S USER
          try {
            serverLogin(user, password, "database.passthrough");
          } catch (OSecurityException ex) {
            throw e;
          }

          // SERVER AUTHENTICATED, BYPASS SECURITY
          database.activateOnCurrentThread();
          database.resetInitialization();
          database.setProperty(ODatabase.OPTIONS.SECURITY.toString(), OSecurityServerUser.class);
          database.open(user, password);
          if (data != null) {
            data.serverUser = true;
            data.serverUsername = user;
          }
        }
      }
    }

    return database;
  }

  public ODatabaseInternal openDatabase(final ODatabaseInternal database) {
    database.activateOnCurrentThread();

    if (database.isClosed())
      if (database.getStorage() instanceof ODirectMemoryStorage)
        database.create();
      else {
        final OServerUserConfiguration replicatorUser = getUser(ODistributedAbstractPlugin.REPLICATOR_USER);
        try {
          serverLogin(replicatorUser.name, replicatorUser.password, "database.passthrough");
        } catch (OSecurityException ex) {
          throw ex;
        }

        // SERVER AUTHENTICATED, BYPASS SECURITY
        database.resetInitialization();
        database.setProperty(ODatabase.OPTIONS.SECURITY.toString(), OSecurityServerUser.class);
        database.open(replicatorUser.name, replicatorUser.password);
      }

    return database;
  }

  public ODistributedServerManager getDistributedManager() {
    return distributedManager;
  }

  public OPartitionedDatabasePoolFactory getDatabasePoolFactory() {
    return dbPoolFactory;
  }

  public void setServerRootDirectory(final String rootDirectory) {
    this.serverRootDirectory = rootDirectory;
  }

  protected void loadConfiguration(final OServerConfiguration iConfiguration) {
    configuration = iConfiguration;

    // FILL THE CONTEXT CONFIGURATION WITH SERVER'S PARAMETERS
    contextConfiguration = new OContextConfiguration();
    if (iConfiguration.properties != null)
      for (OServerEntryConfiguration prop : iConfiguration.properties)
        contextConfiguration.setValue(prop.name, prop.value);

    hookManager = new OConfigurableHooksManager(iConfiguration);
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

    createDefaultServerUsers();
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
              db.getMetadata().getSecurity().createUser(stg.userName, stg.userPassword, ORole.ADMIN);
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

  protected void createDefaultServerUsers() throws IOException {
    // ORIENTDB_ROOT_PASSWORD ENV OR JVM SETTING
    String rootPassword = OSystemVariableResolver.resolveVariable(ROOT_PASSWORD_VAR);

    if (rootPassword != null) {
      rootPassword = rootPassword.trim();
      if (rootPassword.isEmpty())
        rootPassword = null;
    }

    if (rootPassword == null) {
      try {
        // WAIT ANY LOG IS PRINTED
        Thread.sleep(1000);
      } catch (InterruptedException e) {
      }

      System.out.println();
      System.out.println();
      System.out.println("+---------------------------------------------------------------+");
      System.out.println("|                WARNING: FIRST RUN CONFIGURATION               |");
      System.out.println("+---------------------------------------------------------------+");
      System.out.println("| This is the first time the server is running. Please type a   |");
      System.out.println("| password of your choice for the 'root' user or leave it blank |");
      System.out.println("| to auto-generate it.                                          |");
      System.out.println("|                                                               |");
      System.out.println("| To avoid this message set the environment variable or JVM     |");
      System.out.println("| setting ORIENTDB_ROOT_PASSWORD to the root password to use.   |");
      System.out.println("+---------------------------------------------------------------+");
      System.out.print("\nRoot password [BLANK=auto generate it]: ");

      OConsoleReader reader = new DefaultConsoleReader();
      rootPassword = reader.readLine();
      if (rootPassword != null) {
        rootPassword = rootPassword.trim();
        if (rootPassword.isEmpty())
          rootPassword = null;
      }
    }

    addUser(OServerConfiguration.SRV_ROOT_ADMIN, rootPassword, "*");
    addUser(OServerConfiguration.SRV_ROOT_GUEST, OServerConfiguration.SRV_ROOT_GUEST, "connect,server.listDatabases,server.dblist");
    saveConfiguration();
  }

  public OServerPluginManager getPluginManager() {
    return pluginManager;
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
        if (h.parameters != null) {
          // CHECK IF IT'S ENABLED
          boolean enabled = true;

          for (OServerParameterConfiguration p : h.parameters) {
            if (p.name.equals("enabled")) {
              enabled = false;

              String value = OSystemVariableResolver.resolveSystemVariables(p.value);
              if (value != null) {
                value = value.trim();

                if ("true".equalsIgnoreCase(value)) {
                  enabled = true;
                  break;
                }
              }
            }
          }

          if (!enabled)
            // SKIP IT
            continue;
        }

        handler = (OServerPlugin) loadClass(h.clazz).newInstance();

        if (handler instanceof ODistributedServerManager)
          distributedManager = (ODistributedServerManager) handler;

        pluginManager.registerPlugin(new OServerPluginInfo(handler.getName(), null, null, null, handler, null, 0, null));

        handler.config(this, h.parameters);
        handler.startup();
      }
    }
  }

  protected void defaultSettings() {
  }

  private boolean isStorageOfCurrentServerInstance(OStorage storage) {
    if (storage.getUnderlying() instanceof OLocalPaginatedStorage) {
      final String rootDirectory = getDatabaseDirectory();
      return storage.getURL().contains(rootDirectory);
    } else
      return true;
  }

  private void scanDatabaseDirectory(final File directory, final Map<String, String> storages) {
    if (directory.exists() && directory.isDirectory()) {
      final File[] files = directory.listFiles();
      if (files != null)
        for (File db : files) {
          if (db.isDirectory()) {
            final File plocalFile = new File(db.getAbsolutePath() + "/default.pcl");
            final String dbPath = db.getPath().replace('\\', '/');
            final int lastBS = dbPath.lastIndexOf('/', dbPath.length() - 1) + 1;// -1 of dbPath may be ended with slash
            if (plocalFile.exists()) {
              storages.put(OIOUtils.getDatabaseNameFromPath(dbPath.substring(lastBS)), "plocal:" + dbPath);
            } else
              // TRY TO GO IN DEEP RECURSIVELY
              scanDatabaseDirectory(db, storages);
          }
        }
    }
  }
}
