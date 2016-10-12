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

import com.orientechnologies.common.console.OConsoleReader;
import com.orientechnologies.common.console.ODefaultConsoleReader;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.common.log.OAnsiCode;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.parser.OSystemVariableResolver;
import com.orientechnologies.common.profiler.OAbstractProfiler.OProfilerHookValue;
import com.orientechnologies.common.profiler.OProfiler.METRIC_TYPE;
import com.orientechnologies.orient.core.OConstants;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.db.OPartitionedDatabasePoolFactory;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.OSecurityException;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.OToken;
import com.orientechnologies.orient.core.metadata.security.OUser;
import com.orientechnologies.orient.core.security.OSecurityManager;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OLocalPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.memory.ODirectMemoryStorage;
import com.orientechnologies.orient.server.config.*;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.handler.OConfigurableHooksManager;
import com.orientechnologies.orient.server.network.OServerNetworkListener;
import com.orientechnologies.orient.server.network.OServerSocketFactory;
import com.orientechnologies.orient.server.network.protocol.ONetworkProtocol;
import com.orientechnologies.orient.server.network.protocol.ONetworkProtocolData;
import com.orientechnologies.orient.server.network.protocol.http.ONetworkProtocolHttpDb;
import com.orientechnologies.orient.server.plugin.OServerPlugin;
import com.orientechnologies.orient.server.plugin.OServerPluginInfo;
import com.orientechnologies.orient.server.plugin.OServerPluginManager;
import com.orientechnologies.orient.server.security.ODefaultServerSecurity;
import com.orientechnologies.orient.server.security.OSecurityServerUser;
import com.orientechnologies.orient.server.security.OServerSecurity;
import com.orientechnologies.orient.server.token.OTokenHandlerImpl;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
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

public class OServer {
  private static final String                              ROOT_PASSWORD_VAR      = "ORIENTDB_ROOT_PASSWORD";
  private static ThreadGroup                               threadGroup;
  private static Map<String, OServer>                      distributedServers     = new ConcurrentHashMap<String, OServer>();
  private CountDownLatch                                   startupLatch;
  private CountDownLatch                                   shutdownLatch;
  private final boolean                                    shutdownEngineOnExit;
  protected ReentrantLock                                  lock                   = new ReentrantLock();
  protected volatile boolean                               running                = false;
  protected OServerConfigurationManager                    serverCfg;
  protected OContextConfiguration                          contextConfiguration;
  protected OServerShutdownHook                            shutdownHook;
  protected Map<String, Class<? extends ONetworkProtocol>> networkProtocols       = new HashMap<String, Class<? extends ONetworkProtocol>>();
  protected Map<String, OServerSocketFactory>              networkSocketFactories = new HashMap<String, OServerSocketFactory>();
  protected List<OServerNetworkListener>                   networkListeners       = new ArrayList<OServerNetworkListener>();
  protected List<OServerLifecycleListener>                 lifecycleListeners     = new ArrayList<OServerLifecycleListener>();
  protected OServerPluginManager                           pluginManager;
  protected OConfigurableHooksManager                      hookManager;
  protected ODistributedServerManager                      distributedManager;
  protected OServerSecurity                                serverSecurity;
  private OPartitionedDatabasePoolFactory                  dbPoolFactory;
  private SecureRandom                                     random                 = new SecureRandom();
  private Map<String, Object>                              variables              = new HashMap<String, Object>();
  private String                                           serverRootDirectory;
  private String                                           databaseDirectory;
  private OClientConnectionManager                         clientConnectionManager;
  private ClassLoader                                      extensionClassLoader;
  private OTokenHandler                                    tokenHandler;
  private OSystemDatabase                                  systemDatabase;

  public OServer() throws ClassNotFoundException, MalformedObjectNameException, NullPointerException,
      InstanceAlreadyExistsException, MBeanRegistrationException, NotCompliantMBeanException {
    this(true);
  }

  public OServer(boolean shutdownEngineOnExit) throws ClassNotFoundException, MalformedObjectNameException, NullPointerException,
      InstanceAlreadyExistsException, MBeanRegistrationException, NotCompliantMBeanException {
    this.shutdownEngineOnExit = shutdownEngineOnExit;

    serverRootDirectory = OSystemVariableResolver.resolveSystemVariables("${" + Orient.ORIENTDB_HOME + "}", ".");

    OLogManager.instance().installCustomFormatter();

    defaultSettings();

    threadGroup = new ThreadGroup("OrientDB Server");

    System.setProperty("com.sun.management.jmxremote", "true");

    Orient.instance().startup();

    if (OGlobalConfiguration.PROFILER_ENABLED.getValueAsBoolean() && !Orient.instance().getProfiler().isRecording())
      Orient.instance().getProfiler().startRecording();

    shutdownHook = new OServerShutdownHook(this);
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

  public OServerSecurity getSecurity() {
    return serverSecurity;
  }

  public boolean isActive() {
    return running;
  }

  public OClientConnectionManager getClientConnectionManager() {
    return clientConnectionManager;
  }

  public void saveConfiguration() throws IOException {
    serverCfg.saveConfiguration();
  }

  public void restart() throws ClassNotFoundException, InvocationTargetException, InstantiationException, NoSuchMethodException,
      IllegalAccessException, IOException {
    try {
      deinit();
    } finally {
      Orient.instance().startup();
      startup(serverCfg.getConfiguration());
      activate();
    }
  }

  public OSystemDatabase getSystemDatabase() {
    return systemDatabase;
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
   * Attempt to load a class from givenstar class-loader.
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
    try {
      serverCfg = new OServerConfigurationManager(iConfigurationFile);
      return startupFromConfiguration();

    } catch (IOException e) {
      final String message = "Error on reading server configuration from file: " + iConfigurationFile;
      OLogManager.instance().error(this, message, e);
      throw OException.wrapException(new OConfigurationException(message), e);
    }
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

    serverCfg = new OServerConfigurationManager(iInputStream);

    // Startup function split to allow pre-activation changes
    return startupFromConfiguration();
  }

  public OServer startup(final OServerConfiguration iConfiguration)
      throws IllegalArgumentException, SecurityException, InvocationTargetException, NoSuchMethodException, IOException {
    serverCfg = new OServerConfigurationManager(iConfiguration);
    return startupFromConfiguration();
  }

  public OServer startupFromConfiguration()
      throws IllegalArgumentException, SecurityException, InvocationTargetException, NoSuchMethodException, IOException {
    OLogManager.instance().info(this, "OrientDB Server v" + OConstants.getVersion() + " is starting up...");

    Orient.instance();

    if (startupLatch == null)
      startupLatch = new CountDownLatch(1);
    if (shutdownLatch == null)
      shutdownLatch = new CountDownLatch(1);

    clientConnectionManager = new OClientConnectionManager(this);

    initFromConfiguration();

    if (OGlobalConfiguration.ENVIRONMENT_DUMP_CFG_AT_STARTUP.getValueAsBoolean()) {
      System.out.println("Dumping environment after server startup...");
      OGlobalConfiguration.dumpConfiguration(System.out);
    }

    dbPoolFactory = new OPartitionedDatabasePoolFactory();
    dbPoolFactory.setMaxPoolSize(contextConfiguration.getValueAsInteger(OGlobalConfiguration.DB_POOL_MAX));

    databaseDirectory = contextConfiguration.getValue("server.database.path", serverRootDirectory + "/databases/");
    databaseDirectory = OFileUtils.getPath(OSystemVariableResolver.resolveSystemVariables(databaseDirectory));
    databaseDirectory = databaseDirectory.replace("//", "/");

    // CONVERT IT TO ABSOLUTE PATH
    databaseDirectory = (new File(databaseDirectory)).getCanonicalPath();
    databaseDirectory = OFileUtils.getPath(databaseDirectory);

    if (!databaseDirectory.endsWith("/"))
      databaseDirectory += "/";

    OLogManager.instance().info(this, "Databases directory: " + new File(databaseDirectory).getAbsolutePath());

    Orient.instance().getProfiler().registerHookValue("system.databases", "List of databases configured in Server",
        METRIC_TYPE.TEXT, new OProfilerHookValue() {
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
      serverSecurity = new ODefaultServerSecurity(this, serverCfg);
      Orient.instance().setSecurity(serverSecurity);

      // Checks to see if the OrientDB System Database exists and creates it if not.
      // Make sure this happens after setSecurityFactory() is called.
      initSystemDatabase();

      for (OServerLifecycleListener l : lifecycleListeners)
        l.onBeforeActivate();

      final OServerConfiguration configuration = serverCfg.getConfiguration();

      tokenHandler = new OTokenHandlerImpl(this);

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
        loadDatabases();
      } catch (IOException e) {
        final String message = "Error on reading server configuration";
        OLogManager.instance().error(this, message, e);

        throw OException.wrapException(new OConfigurationException(message), e);
      }

      registerPlugins();

      for (OServerLifecycleListener l : lifecycleListeners)
        l.onAfterActivate();

      running = true;

      String httpAddress = "localhost:2480";
      for (OServerNetworkListener listener : getNetworkListeners()) {
        if (listener.getProtocolType().getName().equals(ONetworkProtocolHttpDb.class.getName()))
          httpAddress = listener.getListeningAddress(true);
      }

      OLogManager.instance().info(this, "OrientDB Studio available at $ANSI{blue http://%s/studio/index.html}", httpAddress);
      OLogManager.instance().info(this, "$ANSI{green:italic OrientDB Server is active} v" + OConstants.getVersion() + ".");
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
    try {
      return deinit();
    } finally {
      startupLatch = null;

      if( shutdownLatch != null ) {
        shutdownLatch.countDown();
        shutdownLatch = null;
      }
    }
  }

  protected boolean deinit() {
    if (!running)
      return false;

    try {
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

        clientConnectionManager.shutdown();

        if (pluginManager != null)
          pluginManager.shutdown();

        if (serverSecurity != null)
          serverSecurity.shutdown();

        networkListeners.clear();
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
    } finally {
      OLogManager.instance().info(this, "OrientDB Server shutdown complete\n");
      OLogManager.instance().flush();
    }

    return true;
  }

  public void waitForShutdown() {
    try {
      shutdownLatch.await();
    } catch (InterruptedException e) {
      OLogManager.instance().error(this, "Error during waiting for OrientDB shutdown", e);
    }
  }

  public String getStoragePath(final String iName) {
    if (iName == null)
      throw new IllegalArgumentException("Storage path is null");

    final String name = iName.indexOf(':') > -1 ? iName.substring(iName.indexOf(':') + 1) : iName;

    final String dbName = Orient.isRegisterDatabaseByPath() ? getDatabaseDirectory() + name : name;
    final String dbPath = Orient.isRegisterDatabaseByPath() ? dbName : getDatabaseDirectory() + name;

    if (dbPath.contains(".."))
      throw new IllegalArgumentException("Storage path is invalid because it contains '..'");

    if (dbPath.contains("*"))
      throw new IllegalArgumentException("Storage path is invalid because of the wildcard '*'");

    if (dbPath.startsWith("/")) {
      if (!dbPath.startsWith(getDatabaseDirectory()))
        throw new IllegalArgumentException("Storage path is invalid because it points to an absolute directory");
    }

    final OStorage stg = Orient.instance().getStorage(dbName);
    if (stg != null)
      // ALREADY OPEN
      return stg.getURL();

    // SEARCH IN CONFIGURED PATHS
    final OServerConfiguration configuration = serverCfg.getConfiguration();
    String dbURL = configuration.getStoragePath(name);
    if (dbURL == null) {
      // SEARCH IN DEFAULT DATABASE DIRECTORY
      if (new File(OIOUtils.getPathFromDatabaseName(dbPath) + "/database.ocf").exists())
        dbURL = "plocal:" + dbPath;
      else
        throw new OConfigurationException(
            "Database '" + name + "' is not configured on server (home=" + getDatabaseDirectory() + ")");
    }

    return dbURL;
  }

  public Map<String, String> getAvailableStorageNames() {
    final OServerConfiguration configuration = serverCfg.getConfiguration();

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

    if (storages != null)
      storages.remove(OSystemDatabase.SYSTEM_DB_NAME);

    return storages;
  }

  /**
   * Opens all the available server's databases.
   */
  protected void loadDatabases() {
    if (!OGlobalConfiguration.SERVER_OPEN_ALL_DATABASES_AT_STARTUP.getValueAsBoolean())
      return;

    final String dbPath = getDatabaseDirectory();
    for (Map.Entry<String, String> storageEntry : getAvailableStorageNames().entrySet()) {
      final String databaseName = storageEntry.getKey();

      OLogManager.instance().info(this, "Opening database '%s' at startup...", databaseName);

      final ODatabaseDocumentTx db = new ODatabaseDocumentTx("plocal:" + dbPath + databaseName);
      try {
        try {
          openDatabaseBypassingSecurity(db, null, "internal");
        } catch (OStorageException e) {
          if (e.getCause() instanceof OSecurityException) {
            if (askForEncryptionKey(databaseName)) {
              // RETRY IT
              try {
                openDatabaseBypassingSecurity(db, null, "internal");
              } catch (Exception e2) {
                // LOOK FOR A SECURITY EXCEPTION
                Throwable nested = e2;
                while (nested != null) {
                  if (nested instanceof OSecurityException) {
                    OLogManager.instance().error(this, "Invalid key for database '%s'. Skip database opening", databaseName);
                    return;
                  }
                  nested = nested.getCause();
                }
                OLogManager.instance().error(this, "Error on opening database '%s': %s", e, e.getMessage());
              }
            }
          }
        }
      } finally {
        db.activateOnCurrentThread();
        db.close();
      }
    }
  }

  private boolean askForEncryptionKey(final String iDatabaseName) {
    try {
      Thread.sleep(500);
    } catch (InterruptedException e) {
    }

    System.out.println();
    System.out.println();
    System.out
        .println(OAnsiCode.format("$ANSI{yellow +--------------------------------------------------------------------------+}"));
    System.out.println(OAnsiCode
        .format(String.format("$ANSI{yellow | INSERT THE KEY FOR THE ENCRYPTED DATABASE %-31s|}", "'" + iDatabaseName + "'")));
    System.out
        .println(OAnsiCode.format("$ANSI{yellow +--------------------------------------------------------------------------+}"));
    System.out
        .println(OAnsiCode.format("$ANSI{yellow | To avoid this message set the environment variable or JVM setting        |}"));
    System.out
        .println(OAnsiCode.format("$ANSI{yellow | 'storage.encryptionKey' to the key to use.                               |}"));
    System.out
        .println(OAnsiCode.format("$ANSI{yellow +--------------------------------------------------------------------------+}"));
    System.out.print(OAnsiCode.format("\n$ANSI{yellow Database encryption key [BLANK=to skip opening]: }"));

    final OConsoleReader reader = new ODefaultConsoleReader();
    try {
      String key = reader.readPassword();
      if (key != null) {
        key = key.trim();
        if (!key.isEmpty()) {
          OGlobalConfiguration.STORAGE_ENCRYPTION_KEY.setValue(key);
          return true;
        }
      }
    } catch (IOException e) {
    }
    return false;
  }

  public String getDatabaseDirectory() {
    return databaseDirectory;
  }

  public ThreadGroup getServerThreadGroup() {
    return threadGroup;
  }

  public OServerUserConfiguration serverLogin(final String iUser, final String iPassword, final String iResource) {
    // Returns null if authentication or authorization fails for any reason.
    return authenticateUser(iUser, iPassword, iResource);
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
    // FALSE INDICATES WRONG PASSWORD OR NO AUTHORIZATION
    return authenticateUser(iUserName, iPassword, iResourceToCheck) != null;
  }

  // Returns null if the user cannot be authenticated. Otherwise returns the OServerUserConfiguration user.
  protected OServerUserConfiguration authenticateUser(final String iUserName, final String iPassword,
      final String iResourceToCheck) {
    if (serverSecurity != null && serverSecurity.isEnabled()) {
      // Returns the authenticated username, if successful, otherwise null.
      String authUsername = serverSecurity.authenticate(iUserName, iPassword);

      // Authenticated, now see if the user is authorized.
      if (authUsername != null) {
        if (serverSecurity.isAuthorized(authUsername, iResourceToCheck)) {
          return serverSecurity.getUser(authUsername);
        }
      }
    } else {
      OServerUserConfiguration user = getUser(iUserName);

      if (user != null && user.password != null) {
        if (OSecurityManager.instance().checkPassword(iPassword, user.password) && isAllowed(iUserName, iResourceToCheck)) {
          return user;
        }
      }
    }

    return null;
  }

  /**
   * Checks if a server user is allowed to operate with a resource.
   *
   * @param iUserName
   *          Username to authenticate
   * @return true if authentication is ok, otherwise false
   */
  public boolean isAllowed(final String iUserName, final String iResourceToCheck) {

    if (serverSecurity != null && serverSecurity.isEnabled()) {
      // Let the security plug-in check its users list first.
      if (serverSecurity.isAuthorized(iUserName, iResourceToCheck))
        return true;
    } else {
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
    }

    // WRONG PASSWORD OR NO AUTHORIZATION
    return false;
  }

  public OServerUserConfiguration getUser(final String iUserName) {
    OServerUserConfiguration userCfg = null;

    // First see if iUserName is a security plugin user.
    if (serverSecurity != null && serverSecurity.isEnabled()) {
      userCfg = serverSecurity.getUser(iUserName);
    } else {
      // This will throw an IllegalArgumentException if iUserName is null or empty.
      // However, a null or empty iUserName is possible with some security implementations.
      if (iUserName != null && !iUserName.isEmpty())
        userCfg = serverCfg.getUser(iUserName);
    }

    return userCfg;
  }

  public void dropUser(final String iUserName) throws IOException {
    serverCfg.dropUser(iUserName);
    serverCfg.saveConfiguration();
  }

  public boolean existsStoragePath(final String iURL) {
    return serverCfg.getConfiguration().getStoragePath(iURL) != null;
  }

  public OServerConfiguration getConfiguration() {
    return serverCfg.getConfiguration();
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

  public void addTemporaryUser(final String iName, final String iPassword, final String iPermissions) {
    serverCfg.setEphemeralUser(iName, iPassword, iPermissions);
  }

  public void addUser(final String iName, String iPassword, final String iPermissions) throws IOException {
    if (iPassword == null) {
      // AUTO GENERATE PASSWORD
      final byte[] buffer = new byte[32];
      random.nextBytes(buffer);
      iPassword = OSecurityManager.instance().createSHA256(OSecurityManager.byteArrayToHexStr(buffer));
    }

    // HASH THE PASSWORD
    iPassword = OSecurityManager.instance().createHash(iPassword,
        getContextConfiguration().getValueAsString(OGlobalConfiguration.SECURITY_USER_PASSWORD_DEFAULT_ALGORITHM), true);

    serverCfg.setUser(iName, iPassword, iPermissions);
    serverCfg.saveConfiguration();
  }

  public OServer registerLifecycleListener(final OServerLifecycleListener iListener) {
    lifecycleListeners.add(iListener);
    return this;
  }

  public OServer unregisterLifecycleListener(final OServerLifecycleListener iListener) {
    lifecycleListeners.remove(iListener);
    return this;
  }

  public ODatabase<?> openDatabase(final String iDbUrl, final OToken iToken) {
    final String path = getStoragePath(iDbUrl);

    final ODatabaseInternal<?> database = new ODatabaseDocumentTx(path);
    if (database.isClosed()) {
      final OStorage storage = database.getStorage();
      if (storage instanceof ODirectMemoryStorage && !storage.exists())
        database.create();
      else
        database.open(iToken);
    }

    return database;
  }

  public ODatabase<?> openDatabase(final String iDbUrl, final String user, final String password) {
    return openDatabase(iDbUrl, user, password, null, false);
  }

  public ODatabase<?> openDatabase(final String iDbUrl, final String user, final String password, ONetworkProtocolData data) {
    return openDatabase(iDbUrl, user, password, data, false);
  }

  public ODatabaseDocumentTx openDatabase(final String iDbUrl, final String user, final String password, ONetworkProtocolData data,
      final boolean iBypassAccess) {
    final String path = getStoragePath(iDbUrl);

    final ODatabaseDocumentTx database = new ODatabaseDocumentTx(path);

    return openDatabase(database, user, password, data, iBypassAccess);
  }

  public ODatabaseDocumentTx openDatabase(final ODatabaseDocumentTx database, final String user, final String password,
      final ONetworkProtocolData data, final boolean iBypassAccess) {
    final OStorage storage = database.getStorage();
    if (database.isClosed()) {
      if (storage instanceof ODirectMemoryStorage && !storage.exists()) {
        try {
          database.create();
        } catch (OStorageException e) {
        }
      } else {
        if (iBypassAccess) {
          // BYPASS SECURITY
          openDatabaseBypassingSecurity(database, data, user);
        } else {
          // TRY WITH SERVER'S AUTHENTICATION
          OServerUserConfiguration serverUser = serverLogin(user, password, "database.passthrough");

          if (serverUser != null) {
            // Why do we use the returned serverUser name instead of just passing-in user?
            // Because in some security implementations the user is embedded inside a ticket of some kind
            // that must be decrypted to retrieve the actual user identity. If serverLogin() is successful,
            // that user identity is returned.

            // SERVER AUTHENTICATED, BYPASS SECURITY
            openDatabaseBypassingSecurity(database, data, serverUser.name);
          } else {
            // TRY DATABASE AUTHENTICATION
            database.open(user, password);
            if (data != null) {
              data.serverUser = false;
              data.serverUsername = null;
            }
          }
        }
      }
    }

    return database;
  }

  public void openDatabaseBypassingSecurity(final ODatabaseInternal<?> database, final ONetworkProtocolData data,
      final String user) {
    database.activateOnCurrentThread();
    database.resetInitialization();
    database.setProperty(ODatabase.OPTIONS.SECURITY.toString(), OSecurityServerUser.class);
    database.open(user, "nopassword");
    if (data != null) {
      data.serverUser = true;
      data.serverUsername = user;
    }
  }

  public ODatabaseInternal openDatabase(final ODatabaseInternal database) {
    database.activateOnCurrentThread();

    if (database.isClosed())
      if (database.getStorage() instanceof ODirectMemoryStorage)
        database.create();
      else {
        // SERVER AUTHENTICATED, BYPASS SECURITY
        openDatabaseBypassingSecurity(database, null, "internal");
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

  protected void initFromConfiguration() {
    final OServerConfiguration cfg = serverCfg.getConfiguration();

    // FILL THE CONTEXT CONFIGURATION WITH SERVER'S PARAMETERS
    contextConfiguration = new OContextConfiguration();

    if (cfg.properties != null)
      for (OServerEntryConfiguration prop : cfg.properties)
        contextConfiguration.setValue(prop.name, prop.value);

    hookManager = new OConfigurableHooksManager(cfg);
  }

  protected void loadUsers() throws IOException {
    try {
      final OServerConfiguration configuration = serverCfg.getConfiguration();

      if (configuration.isAfterFirstTime) {
        return;
      }

      configuration.isAfterFirstTime = true;

      if (OGlobalConfiguration.CREATE_DEFAULT_USERS.getValueAsBoolean())
        createDefaultServerUsers();

    } finally {
      // REMOVE THE ENV VARIABLE FOR SECURITY REASONS
      OSystemVariableResolver.setEnv(ROOT_PASSWORD_VAR, "");
    }
  }

  /**
   * Load configured storages.
   */
  protected void loadStorages() {
    final OServerConfiguration configuration = serverCfg.getConfiguration();

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

    if (serverSecurity != null && !serverSecurity.arePasswordsStored())
      return;

    // ORIENTDB_ROOT_PASSWORD ENV OR JVM SETTING
    String rootPassword = OSystemVariableResolver.resolveVariable(ROOT_PASSWORD_VAR);

    if (rootPassword != null) {
      rootPassword = rootPassword.trim();
      if (rootPassword.isEmpty())
        rootPassword = null;
    }

    if (rootPassword == null && !serverCfg.existsUser(OServerConfiguration.DEFAULT_ROOT_USER)) {
      try {
        // WAIT ANY LOG IS PRINTED
        Thread.sleep(1000);
      } catch (InterruptedException e) {
      }

      System.out.println();
      System.out.println();
      System.out.println(OAnsiCode.format("$ANSI{yellow +---------------------------------------------------------------+}"));
      System.out.println(OAnsiCode.format("$ANSI{yellow |                WARNING: FIRST RUN CONFIGURATION               |}"));
      System.out.println(OAnsiCode.format("$ANSI{yellow +---------------------------------------------------------------+}"));
      System.out.println(OAnsiCode.format("$ANSI{yellow | This is the first time the server is running. Please type a   |}"));
      System.out.println(OAnsiCode.format("$ANSI{yellow | password of your choice for the 'root' user or leave it blank |}"));
      System.out.println(OAnsiCode.format("$ANSI{yellow | to auto-generate it.                                          |}"));
      System.out.println(OAnsiCode.format("$ANSI{yellow |                                                               |}"));
      System.out.println(OAnsiCode.format("$ANSI{yellow | To avoid this message set the environment variable or JVM     |}"));
      System.out.println(OAnsiCode.format("$ANSI{yellow | setting ORIENTDB_ROOT_PASSWORD to the root password to use.   |}"));
      System.out.println(OAnsiCode.format("$ANSI{yellow +---------------------------------------------------------------+}"));

      final OConsoleReader console = new ODefaultConsoleReader();

      // ASK FOR PASSWORD + CONFIRM
      do {
        System.out.print(OAnsiCode.format("\n$ANSI{yellow Root password [BLANK=auto generate it]: }"));
        rootPassword = console.readPassword();

        if (rootPassword != null) {
          rootPassword = rootPassword.trim();
          if (rootPassword.isEmpty())
            rootPassword = null;
        }

        if (rootPassword != null) {
          System.out.print(OAnsiCode.format("$ANSI{yellow Please confirm the root password: }"));

          String rootConfirmPassword = console.readPassword();
          if (rootConfirmPassword != null) {
            rootConfirmPassword = rootConfirmPassword.trim();
            if (rootConfirmPassword.isEmpty())
              rootConfirmPassword = null;
          }

          if (!rootPassword.equals(rootConfirmPassword)) {
            System.out.println(OAnsiCode.format(
                "$ANSI{red ERROR: Passwords don't match, please reinsert both of them, or press ENTER to auto generate it}"));
          } else
            // PASSWORDS MATCH
            break;
        }

      } while (rootPassword != null);

    } else
      OLogManager.instance().warn(this, "Found ORIENTDB_ROOT_PASSWORD variable, using this value as root's password", rootPassword);

    if (!serverCfg.existsUser(OServerConfiguration.DEFAULT_ROOT_USER)) {
      addUser(OServerConfiguration.DEFAULT_ROOT_USER, rootPassword, "*");
    }
    if (!serverCfg.existsUser(OServerConfiguration.GUEST_USER)) {
      addUser(OServerConfiguration.GUEST_USER, OServerConfiguration.GUEST_PASS, "connect,server.listDatabases,server.dblist");
    }
  }

  public OServerPluginManager getPluginManager() {
    return pluginManager;
  }

  protected void registerPlugins() throws InstantiationException, IllegalAccessException, ClassNotFoundException {
    pluginManager = new OServerPluginManager();
    pluginManager.config(this);
    pluginManager.startup();

    if (serverSecurity != null)
      serverSecurity.onAfterDynamicPlugins();

    // PLUGINS CONFIGURED IN XML
    final OServerConfiguration configuration = serverCfg.getConfiguration();

    if (configuration.handlers != null) {
      // ACTIVATE PLUGINS
      final List<OServerPlugin> plugins = new ArrayList<OServerPlugin>();

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

        final OServerPlugin plugin = (OServerPlugin) loadClass(h.clazz).newInstance();

        if (plugin instanceof ODistributedServerManager)
          distributedManager = (ODistributedServerManager) plugin;

        pluginManager.registerPlugin(new OServerPluginInfo(plugin.getName(), null, null, null, plugin, null, 0, null));

        pluginManager.callListenerBeforeConfig(plugin, h.parameters);
        plugin.config(this, h.parameters);
        pluginManager.callListenerAfterConfig(plugin, h.parameters);

        plugins.add(plugin);
      }

      // START ALL THE CONFIGURED PLUGINS
      for (OServerPlugin plugin : plugins) {
        pluginManager.callListenerBeforeStartup(plugin);
        plugin.startup();
        pluginManager.callListenerAfterStartup(plugin);
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
            final File plocalFile = new File(db.getAbsolutePath() + "/database.ocf");
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

  public OTokenHandler getTokenHandler() {
    return tokenHandler;
  }

  private void initSystemDatabase() {
    systemDatabase = new OSystemDatabase(this);
  }
}
