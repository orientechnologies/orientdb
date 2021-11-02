/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.server;

import com.orientechnologies.common.console.OConsoleReader;
import com.orientechnologies.common.console.ODefaultConsoleReader;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.log.OAnsiCode;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.parser.OSystemVariableResolver;
import com.orientechnologies.common.profiler.OAbstractProfiler.OProfilerHookValue;
import com.orientechnologies.common.profiler.OProfiler.METRIC_TYPE;
import com.orientechnologies.orient.core.OConstants;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.*;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTxInternal;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.metadata.security.OSecurityUser;
import com.orientechnologies.orient.core.metadata.security.auth.OTokenAuthInfo;
import com.orientechnologies.orient.core.security.OInvalidPasswordException;
import com.orientechnologies.orient.core.security.OParsedToken;
import com.orientechnologies.orient.core.security.OSecuritySystem;
import com.orientechnologies.orient.server.config.*;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.distributed.config.ODistributedConfig;
import com.orientechnologies.orient.server.handler.OConfigurableHooksManager;
import com.orientechnologies.orient.server.network.OServerNetworkListener;
import com.orientechnologies.orient.server.network.OServerSocketFactory;
import com.orientechnologies.orient.server.network.protocol.ONetworkProtocol;
import com.orientechnologies.orient.server.network.protocol.ONetworkProtocolData;
import com.orientechnologies.orient.server.network.protocol.http.OHttpSessionManager;
import com.orientechnologies.orient.server.network.protocol.http.ONetworkProtocolHttpDb;
import com.orientechnologies.orient.server.plugin.OServerPlugin;
import com.orientechnologies.orient.server.plugin.OServerPluginInfo;
import com.orientechnologies.orient.server.plugin.OServerPluginManager;
import com.orientechnologies.orient.server.token.OTokenHandlerImpl;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.ReentrantLock;

public class OServer {
  private static final String ROOT_PASSWORD_VAR = "ORIENTDB_ROOT_PASSWORD";
  private static ThreadGroup threadGroup;
  private static final Map<String, OServer> distributedServers =
      new ConcurrentHashMap<String, OServer>();
  private CountDownLatch startupLatch;
  private CountDownLatch shutdownLatch;
  private final boolean shutdownEngineOnExit;
  protected ReentrantLock lock = new ReentrantLock();
  protected volatile boolean running = false;
  protected volatile boolean rejectRequests = true;
  protected OServerConfigurationManager serverCfg;
  protected OContextConfiguration contextConfiguration;
  protected OServerShutdownHook shutdownHook;
  protected Map<String, Class<? extends ONetworkProtocol>> networkProtocols =
      new HashMap<String, Class<? extends ONetworkProtocol>>();
  protected Map<String, OServerSocketFactory> networkSocketFactories =
      new HashMap<String, OServerSocketFactory>();
  protected List<OServerNetworkListener> networkListeners = new ArrayList<OServerNetworkListener>();
  protected List<OServerLifecycleListener> lifecycleListeners =
      new ArrayList<OServerLifecycleListener>();
  protected OServerPluginManager pluginManager;
  protected OConfigurableHooksManager hookManager;
  protected ODistributedServerManager distributedManager;
  private final Map<String, Object> variables = new HashMap<String, Object>();
  private String serverRootDirectory;
  private String databaseDirectory;
  private OClientConnectionManager clientConnectionManager;
  private OHttpSessionManager httpSessionManager;
  private OPushManager pushManager;
  private ClassLoader extensionClassLoader;
  private OTokenHandler tokenHandler;
  private OrientDB context;
  private OrientDBInternal databases;
  protected Date startedOn = new Date();

  public OServer() {
    this(!Orient.instance().isInsideWebContainer());
  }

  public OServer(boolean shutdownEngineOnExit) {
    final boolean insideWebContainer = Orient.instance().isInsideWebContainer();

    if (insideWebContainer && shutdownEngineOnExit) {
      OLogManager.instance()
          .warnNoDb(
              this,
              "OrientDB instance is running inside of web application, "
                  + "it is highly unrecommended to force to shutdown OrientDB engine on server shutdown");
    }

    this.shutdownEngineOnExit = shutdownEngineOnExit;

    serverRootDirectory =
        OSystemVariableResolver.resolveSystemVariables("${" + Orient.ORIENTDB_HOME + "}", ".");

    OLogManager.instance().installCustomFormatter();

    defaultSettings();

    threadGroup = new ThreadGroup("OrientDB Server");

    System.setProperty("com.sun.management.jmxremote", "true");

    Orient.instance().startup();

    if (OGlobalConfiguration.PROFILER_ENABLED.getValueAsBoolean()
        && !Orient.instance().getProfiler().isRecording())
      Orient.instance().getProfiler().startRecording();

    if (shutdownEngineOnExit) {
      shutdownHook = new OServerShutdownHook(this);
    }
  }

  public static OServer startFromFileConfig(String config)
      throws ClassNotFoundException, InstantiationException, IOException, IllegalAccessException {
    OServer server = new OServer(false);
    server.startup(config);
    server.activate();
    return server;
  }

  public static OServer startFromClasspathConfig(String config)
      throws ClassNotFoundException, InstantiationException, IOException, IllegalAccessException {
    OServer server = new OServer(false);
    server.startup(Thread.currentThread().getContextClassLoader().getResourceAsStream(config));
    server.activate();
    return server;
  }

  public static OServer startFromStreamConfig(InputStream config)
      throws ClassNotFoundException, InstantiationException, IOException, IllegalAccessException {
    OServer server = new OServer(false);
    server.startup(config);
    server.activate();
    return server;
  }

  public static OServer getInstance(final String iServerId) {
    return distributedServers.get(iServerId);
  }

  public static OServer getInstanceByPath(final String iPath) {
    for (Map.Entry<String, OServer> entry : distributedServers.entrySet()) {
      if (iPath.startsWith(entry.getValue().getDatabaseDirectory())) return entry.getValue();
    }
    return null;
  }

  public static void registerServerInstance(final String iServerId, final OServer iServer) {
    distributedServers.put(iServerId, iServer);
  }

  public static void unregisterServerInstance(final String iServerId) {
    distributedServers.remove(iServerId);
  }

  /**
   * Set the preferred {@link ClassLoader} used to load extensions.
   *
   * @since 2.1
   */
  public void setExtensionClassLoader(/* @Nullable */ final ClassLoader extensionClassLoader) {
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

  public OSecuritySystem getSecurity() {
    return databases.getSecuritySystem();
  }

  public boolean isActive() {
    return running;
  }

  public OClientConnectionManager getClientConnectionManager() {
    return clientConnectionManager;
  }

  public OHttpSessionManager getHttpSessionManager() {
    return httpSessionManager;
  }

  public OPushManager getPushManager() {
    return pushManager;
  }

  public void saveConfiguration() throws IOException {
    serverCfg.saveConfiguration();
  }

  public void restart()
      throws ClassNotFoundException, InvocationTargetException, InstantiationException,
          NoSuchMethodException, IllegalAccessException, IOException {
    try {
      deinit();
    } finally {
      Orient.instance().startup();
      startup(serverCfg.getConfiguration());
      activate();
    }
  }

  public OSystemDatabase getSystemDatabase() {
    return databases.getSystemDatabase();
  }

  public String getServerId() {
    return getSystemDatabase().getServerId();
  }

  /** Load an extension class by name. */
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

  /** Attempt to load a class from givenstar class-loader. */
  /* @Nullable */
  private Class<?> tryLoadClass(/* @Nullable */ final ClassLoader classLoader, final String name) {
    if (classLoader != null) {
      try {
        return classLoader.loadClass(name);
      } catch (ClassNotFoundException e) {
        // ignore
      }
    }
    return null;
  }

  public OServer startup() throws OConfigurationException {
    String config = OServerConfiguration.DEFAULT_CONFIG_FILE;
    if (System.getProperty(OServerConfiguration.PROPERTY_CONFIG_FILE) != null)
      config = System.getProperty(OServerConfiguration.PROPERTY_CONFIG_FILE);

    Orient.instance().startup();

    startup(new File(OSystemVariableResolver.resolveSystemVariables(config)));

    return this;
  }

  public OServer startup(final File iConfigurationFile) throws OConfigurationException {
    // Startup function split to allow pre-activation changes
    try {
      serverCfg = new OServerConfigurationManager(iConfigurationFile);
      return startupFromConfiguration();

    } catch (IOException e) {
      final String message =
          "Error on reading server configuration from file: " + iConfigurationFile;
      OLogManager.instance().error(this, message, e);
      throw OException.wrapException(new OConfigurationException(message), e);
    }
  }

  public OServer startup(final String iConfiguration) throws IOException {
    return startup(new ByteArrayInputStream(iConfiguration.getBytes()));
  }

  public OServer startup(final InputStream iInputStream) throws IOException {
    if (iInputStream == null) throw new OConfigurationException("Configuration file is null");

    serverCfg = new OServerConfigurationManager(iInputStream);

    // Startup function split to allow pre-activation changes
    return startupFromConfiguration();
  }

  public OServer startup(final OServerConfiguration iConfiguration)
      throws IllegalArgumentException, SecurityException, IOException {
    serverCfg = new OServerConfigurationManager(iConfiguration);
    return startupFromConfiguration();
  }

  public OServer startupFromConfiguration() throws IOException {
    OLogManager.instance()
        .info(this, "OrientDB Server v" + OConstants.getVersion() + " is starting up...");

    Orient.instance();

    if (startupLatch == null) startupLatch = new CountDownLatch(1);
    if (shutdownLatch == null) shutdownLatch = new CountDownLatch(1);

    initFromConfiguration();

    clientConnectionManager = new OClientConnectionManager(this);
    httpSessionManager = new OHttpSessionManager(this);
    pushManager = new OPushManager();
    rejectRequests = false;

    if (contextConfiguration.getValueAsBoolean(
        OGlobalConfiguration.ENVIRONMENT_DUMP_CFG_AT_STARTUP)) {
      System.out.println("Dumping environment after server startup...");
      OGlobalConfiguration.dumpConfiguration(System.out);
    }

    databaseDirectory =
        contextConfiguration.getValue("server.database.path", serverRootDirectory + "/databases/");
    databaseDirectory =
        OFileUtils.getPath(OSystemVariableResolver.resolveSystemVariables(databaseDirectory));
    databaseDirectory = databaseDirectory.replace("//", "/");

    // CONVERT IT TO ABSOLUTE PATH
    databaseDirectory = (new File(databaseDirectory)).getCanonicalPath();
    databaseDirectory = OFileUtils.getPath(databaseDirectory);

    if (!databaseDirectory.endsWith("/")) databaseDirectory += "/";

    OrientDBConfigBuilder builder = OrientDBConfig.builder();
    for (OServerUserConfiguration user : serverCfg.getUsers()) {
      builder.addGlobalUser(user.getName(), user.getPassword(), user.getResources());
    }
    OrientDBConfig config =
        builder
            .fromContext(contextConfiguration)
            .setSecurityConfig(new OServerSecurityConfig(this, this.serverCfg))
            .build();

    if (contextConfiguration.getValueAsBoolean(
        OGlobalConfiguration.SERVER_BACKWARD_COMPATIBILITY)) {

      databases =
          ODatabaseDocumentTxInternal.getOrCreateEmbeddedFactory(this.databaseDirectory, config);
    } else {
      OServerConfiguration configuration = getConfiguration();

      if (configuration.distributed != null && configuration.distributed.enabled) {
        try {
          OrientDBConfig orientDBConfig =
              ODistributedConfig.buildConfig(
                  contextConfiguration, ODistributedConfig.fromEnv(configuration.distributed));
          databases = OrientDBInternal.distributed(this.databaseDirectory, orientDBConfig);
        } catch (ODatabaseException ex) {
          databases = OrientDBInternal.embedded(this.databaseDirectory, config);
        }
      } else {
        try {
          databases = OrientDBInternal.distributed(this.databaseDirectory, config);
        } catch (ODatabaseException ex) {
          databases = OrientDBInternal.embedded(this.databaseDirectory, config);
        }
      }
    }

    if (databases instanceof OServerAware) {
      ((OServerAware) databases).init(this);
    }

    context = databases.newOrientDB();

    OLogManager.instance()
        .info(this, "Databases directory: " + new File(databaseDirectory).getAbsolutePath());

    Orient.instance()
        .getProfiler()
        .registerHookValue(
            "system.databases",
            "List of databases configured in Server",
            METRIC_TYPE.TEXT,
            new OProfilerHookValue() {
              @Override
              public Object getValue() {
                final StringBuilder dbs = new StringBuilder(64);
                for (String dbName : getAvailableStorageNames().keySet()) {
                  if (dbs.length() > 0) dbs.append(',');
                  dbs.append(dbName);
                }
                return dbs.toString();
              }
            });

    return this;
  }

  @SuppressWarnings("unchecked")
  public OServer activate()
      throws ClassNotFoundException, InstantiationException, IllegalAccessException {
    lock.lock();
    try {
      // Checks to see if the OrientDB System Database exists and creates it if not.
      // Make sure this happens after setSecurity() is called.
      initSystemDatabase();

      for (OServerLifecycleListener l : lifecycleListeners) l.onBeforeActivate();

      final OServerConfiguration configuration = serverCfg.getConfiguration();

      tokenHandler =
          new OTokenHandlerImpl(
              this.databases.getSecuritySystem().getTokenSign(), this.getContextConfiguration());

      if (configuration.network != null) {
        // REGISTER/CREATE SOCKET FACTORIES
        if (configuration.network.sockets != null) {
          for (OServerSocketFactoryConfiguration f : configuration.network.sockets) {
            Class<? extends OServerSocketFactory> fClass =
                (Class<? extends OServerSocketFactory>) loadClass(f.implementation);
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
          networkProtocols.put(
              p.name, (Class<? extends ONetworkProtocol>) loadClass(p.implementation));

        // STARTUP LISTENERS
        for (OServerNetworkListenerConfiguration l : configuration.network.listeners)
          networkListeners.add(
              new OServerNetworkListener(
                  this,
                  networkSocketFactories.get(l.socket),
                  l.ipAddress,
                  l.portRange,
                  l.protocol,
                  networkProtocols.get(l.protocol),
                  l.parameters,
                  l.commands));

      } else OLogManager.instance().warn(this, "Network configuration was not found");

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

      for (OServerLifecycleListener l : lifecycleListeners) l.onAfterActivate();

      running = true;

      String httpAddress = "localhost:2480";
      for (OServerNetworkListener listener : getNetworkListeners()) {
        if (listener.getProtocolType().getName().equals(ONetworkProtocolHttpDb.class.getName()))
          httpAddress = listener.getListeningAddress(true);
      }

      OLogManager.instance()
          .info(
              this,
              "OrientDB Studio available at $ANSI{blue http://%s/studio/index.html}",
              httpAddress);
      OLogManager.instance()
          .info(
              this,
              "$ANSI{green:italic OrientDB Server is active} v" + OConstants.getVersion() + ".");
    } catch (ClassNotFoundException
        | InstantiationException
        | IllegalAccessException
        | RuntimeException e) {
      deinit();
      throw e;
    } finally {
      lock.unlock();
      startupLatch.countDown();
    }
    if (distributedManager != null) {
      try {
        distributedManager.waitUntilNodeOnline();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
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
      boolean res = deinit();
      return res;
    } finally {
      startupLatch = null;
      if (shutdownLatch != null) {
        shutdownLatch.countDown();
        shutdownLatch = null;
      }

      if (shutdownEngineOnExit) {
        OLogManager.instance().shutdown();
      }
    }
  }

  protected boolean deinit() {
    try {
      running = false;

      OLogManager.instance().info(this, "OrientDB Server is shutting down...");

      if (shutdownHook != null) shutdownHook.cancel();

      Orient.instance().getProfiler().unregisterHookValue("system.databases");

      for (OServerLifecycleListener l : lifecycleListeners) l.onBeforeDeactivate();

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
            } catch (Exception e) {
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
            OLogManager.instance()
                .error(this, "Error during deactivation of server lifecycle listener %s", e, l);
          }

        rejectRequests = true;
        pushManager.shutdown();
        clientConnectionManager.shutdown();
        httpSessionManager.shutdown();

        if (pluginManager != null) pluginManager.shutdown();

        networkListeners.clear();
      } finally {
        lock.unlock();
      }

      if (shutdownEngineOnExit && !Orient.isRegisterDatabaseByPath())
        try {
          OLogManager.instance().info(this, "Shutting down databases:");
          Orient.instance().shutdown();
        } catch (Exception e) {
          OLogManager.instance().error(this, "Error during OrientDB shutdown", e);
        }
      if (!getContextConfiguration()
              .getValueAsBoolean(OGlobalConfiguration.SERVER_BACKWARD_COMPATIBILITY)
          && databases != null) {
        databases.close();
        databases = null;
      }
    } finally {
      OLogManager.instance().info(this, "OrientDB Server shutdown complete\n");
      OLogManager.instance().flush();
    }

    return true;
  }

  public boolean rejectRequests() {
    return rejectRequests;
  }

  public void waitForShutdown() {
    try {
      if (shutdownLatch != null) shutdownLatch.await();
    } catch (InterruptedException e) {
      OLogManager.instance().error(this, "Error during waiting for OrientDB shutdown", e);
    }
  }

  public Map<String, String> getAvailableStorageNames() {
    Set<String> dbs = listDatabases();
    Map<String, String> toSend = new HashMap<String, String>();
    for (String dbName : dbs) {
      toSend.put(dbName, dbName);
    }

    return toSend;
  }

  /** Opens all the available server's databases. */
  protected void loadDatabases() {
    if (!getContextConfiguration()
        .getValueAsBoolean(OGlobalConfiguration.SERVER_OPEN_ALL_DATABASES_AT_STARTUP)) return;
    getDatabases().loadAllDatabases();
  }

  private boolean askForEncryptionKey(final String iDatabaseName) {
    try {
      Thread.sleep(500);
    } catch (InterruptedException e) {
    }

    System.out.println();
    System.out.println();
    System.out.println(
        OAnsiCode.format(
            "$ANSI{yellow +--------------------------------------------------------------------------+}"));
    System.out.println(
        OAnsiCode.format(
            String.format(
                "$ANSI{yellow | INSERT THE KEY FOR THE ENCRYPTED DATABASE %-31s|}",
                "'" + iDatabaseName + "'")));
    System.out.println(
        OAnsiCode.format(
            "$ANSI{yellow +--------------------------------------------------------------------------+}"));
    System.out.println(
        OAnsiCode.format(
            "$ANSI{yellow | To avoid this message set the environment variable or JVM setting        |}"));
    System.out.println(
        OAnsiCode.format(
            "$ANSI{yellow | 'storage.encryptionKey' to the key to use.                               |}"));
    System.out.println(
        OAnsiCode.format(
            "$ANSI{yellow +--------------------------------------------------------------------------+}"));
    System.out.print(
        OAnsiCode.format("\n$ANSI{yellow Database encryption key [BLANK=to skip opening]: }"));

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

  /**
   * Authenticate a server user.
   *
   * @param iUserName Username to authenticate
   * @param iPassword Password in clear
   * @return true if authentication is ok, otherwise false
   */
  public boolean authenticate(
      final String iUserName, final String iPassword, final String iResourceToCheck) {
    // FALSE INDICATES WRONG PASSWORD OR NO AUTHORIZATION
    return authenticateUser(iUserName, iPassword, iResourceToCheck) != null;
  }

  // Returns null if the user cannot be authenticated. Otherwise returns the
  // OServerUserConfiguration user.
  public OSecurityUser authenticateUser(
      final String iUserName, final String iPassword, final String iResourceToCheck) {
    return databases
        .getSecuritySystem()
        .authenticateAndAuthorize(iUserName, iPassword, iResourceToCheck);
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
  public <RET extends OServerNetworkListener> RET getListenerByProtocol(
      final Class<? extends ONetworkProtocol> iProtocolClass) {
    for (OServerNetworkListener l : networkListeners)
      if (iProtocolClass.isAssignableFrom(l.getProtocolType())) return (RET) l;

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
    if (startupLatch == null)
      throw new ODatabaseException("Error on plugin lookup: the server did not start correctly");

    try {
      startupLatch.await();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    if (!running)
      throw new ODatabaseException("Error on plugin lookup the server did not start correctly.");

    for (OServerPluginInfo h : getPlugins())
      if (h.getInstance() != null && h.getInstance().getClass().equals(iPluginClass))
        return (RET) h.getInstance();

    return null;
  }

  @SuppressWarnings("unchecked")
  public <RET extends OServerPlugin> RET getPlugin(final String iName) {
    if (startupLatch == null)
      throw new ODatabaseException("Error on plugin lookup: the server did not start correctly");

    try {
      startupLatch.await();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    if (!running)
      throw new ODatabaseException("Error on plugin lookup: the server did not start correctly");

    final OServerPluginInfo p = pluginManager.getPluginByName(iName);
    if (p != null) return (RET) p.getInstance();
    return null;
  }

  public Object getVariable(final String iName) {
    return variables.get(iName);
  }

  public OServer setVariable(final String iName, final Object iValue) {
    if (iValue == null) variables.remove(iName);
    else variables.put(iName, iValue);
    return this;
  }

  public void addTemporaryUser(
      final String iName, final String iPassword, final String iPermissions) {
    databases.getSecuritySystem().addTemporaryUser(iName, iPassword, iPermissions);
  }

  public OServer registerLifecycleListener(final OServerLifecycleListener iListener) {
    lifecycleListeners.add(iListener);
    return this;
  }

  public OServer unregisterLifecycleListener(final OServerLifecycleListener iListener) {
    lifecycleListeners.remove(iListener);
    return this;
  }

  public ODatabaseDocumentInternal openDatabase(final String iDbUrl, final OParsedToken iToken) {
    return databases.open(new OTokenAuthInfo(iToken), OrientDBConfig.defaultConfig());
  }

  public ODatabaseDocumentInternal openDatabase(
      final String iDbUrl, final String user, final String password) {
    return openDatabase(iDbUrl, user, password, null);
  }

  public ODatabaseDocumentInternal openDatabase(
      final String iDbUrl, final String user, final String password, ONetworkProtocolData data) {
    final ODatabaseDocumentInternal database;
    boolean serverAuth = false;
    database = databases.open(iDbUrl, user, password);
    if (OSecurityUser.SERVER_USER_TYPE.equals(database.getUser().getUserType())) {
      serverAuth = true;
    }
    if (serverAuth && data != null) {
      data.serverUser = true;
      data.serverUsername = user;
    } else if (data != null) {
      data.serverUser = false;
      data.serverUsername = null;
    }
    return database;
  }

  public ODatabaseDocumentInternal openDatabase(String database) {
    return getDatabases().openNoAuthorization(database);
  }

  public ODistributedServerManager getDistributedManager() {
    return distributedManager;
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

  public OConfigurableHooksManager getHookManager() {
    return hookManager;
  }

  protected void loadUsers() throws IOException {
    final OServerConfiguration configuration = serverCfg.getConfiguration();

    if (configuration.isAfterFirstTime) {
      return;
    }

    configuration.isAfterFirstTime = true;

    createDefaultServerUsers();
  }

  /** Load configured storages. */
  protected void loadStorages() {
    final OServerConfiguration configuration = serverCfg.getConfiguration();

    if (configuration.storages == null) return;
    for (OServerStorageConfiguration stg : configuration.storages) {
      if (stg.loadOnStartup) {
        String url = stg.path;
        if (url.endsWith("/")) url = url.substring(0, url.length() - 1);
        url = url.replace('\\', '/');

        int typeIndex = url.indexOf(':');
        if (typeIndex <= 0)
          throw new OConfigurationException(
              "Error in database URL: the engine was not specified. Syntax is: "
                  + Orient.URL_SYNTAX
                  + ". URL was: "
                  + url);

        String remoteUrl = url.substring(typeIndex + 1);
        int index = remoteUrl.lastIndexOf('/');
        String baseUrl;
        if (index > 0) {
          baseUrl = remoteUrl.substring(0, index);
        } else {
          baseUrl = "./";
        }
        databases.initCustomStorage(stg.name, baseUrl, stg.userName, stg.userPassword);
      }
    }
  }

  protected void createDefaultServerUsers() throws IOException {

    if (databases.getSecuritySystem() != null
        && !databases.getSecuritySystem().arePasswordsStored()) return;

    // ORIENTDB_ROOT_PASSWORD ENV OR JVM SETTING
    String rootPassword = OSystemVariableResolver.resolveVariable(ROOT_PASSWORD_VAR);

    if (rootPassword != null) {
      rootPassword = rootPassword.trim();
      if (rootPassword.isEmpty()) rootPassword = null;
    }
    boolean existsRoot =
        existsSystemUser(OServerConfiguration.DEFAULT_ROOT_USER)
            || serverCfg.existsUser(OServerConfiguration.DEFAULT_ROOT_USER);

    if (rootPassword == null && !existsRoot) {
      try {
        // WAIT ANY LOG IS PRINTED
        Thread.sleep(1000);
      } catch (InterruptedException e) {
      }

      System.out.println();
      System.out.println();
      System.out.println(
          OAnsiCode.format(
              "$ANSI{yellow +---------------------------------------------------------------+}"));
      System.out.println(
          OAnsiCode.format(
              "$ANSI{yellow |                WARNING: FIRST RUN CONFIGURATION               |}"));
      System.out.println(
          OAnsiCode.format(
              "$ANSI{yellow +---------------------------------------------------------------+}"));
      System.out.println(
          OAnsiCode.format(
              "$ANSI{yellow | This is the first time the server is running. Please type a   |}"));
      System.out.println(
          OAnsiCode.format(
              "$ANSI{yellow | password of your choice for the 'root' user or leave it blank |}"));
      System.out.println(
          OAnsiCode.format(
              "$ANSI{yellow | to auto-generate it.                                          |}"));
      System.out.println(
          OAnsiCode.format(
              "$ANSI{yellow |                                                               |}"));
      System.out.println(
          OAnsiCode.format(
              "$ANSI{yellow | To avoid this message set the environment variable or JVM     |}"));
      System.out.println(
          OAnsiCode.format(
              "$ANSI{yellow | setting ORIENTDB_ROOT_PASSWORD to the root password to use.   |}"));
      System.out.println(
          OAnsiCode.format(
              "$ANSI{yellow +---------------------------------------------------------------+}"));

      final OConsoleReader console = new ODefaultConsoleReader();

      // ASK FOR PASSWORD + CONFIRM
      do {
        System.out.print(
            OAnsiCode.format("\n$ANSI{yellow Root password [BLANK=auto generate it]: }"));
        rootPassword = console.readPassword();

        if (rootPassword != null) {
          rootPassword = rootPassword.trim();
          if (rootPassword.isEmpty()) rootPassword = null;
        }

        if (rootPassword != null) {
          System.out.print(OAnsiCode.format("$ANSI{yellow Please confirm the root password: }"));

          String rootConfirmPassword = console.readPassword();
          if (rootConfirmPassword != null) {
            rootConfirmPassword = rootConfirmPassword.trim();
            if (rootConfirmPassword.isEmpty()) rootConfirmPassword = null;
          }

          if (!rootPassword.equals(rootConfirmPassword)) {
            System.out.println(
                OAnsiCode.format(
                    "$ANSI{red ERROR: Passwords don't match, please reinsert both of them, or press ENTER to auto generate it}"));
          } else
            // PASSWORDS MATCH

            try {
              if (getSecurity() != null) {
                getSecurity().validatePassword("root", rootPassword);
              }
              // PASSWORD IS STRONG ENOUGH
              break;
            } catch (OInvalidPasswordException ex) {
              System.out.println(
                  OAnsiCode.format(
                      "$ANSI{red ERROR: Root password does not match the password policies}"));
              if (ex.getMessage() != null) {
                System.out.println(ex.getMessage());
              }
            }
        }

      } while (rootPassword != null);

    } else
      OLogManager.instance()
          .info(
              this,
              "Found ORIENTDB_ROOT_PASSWORD variable, using this value as root's password",
              rootPassword);

    if (!existsRoot) {
      context.execute(
          "CREATE SYSTEM USER "
              + OServerConfiguration.DEFAULT_ROOT_USER
              + " IDENTIFIED BY ? ROLE root",
          rootPassword);
    }

    if (!existsSystemUser(OServerConfiguration.GUEST_USER)) {
      context.execute(
          "CREATE SYSTEM USER " + OServerConfiguration.GUEST_USER + " IDENTIFIED BY ? ROLE guest",
          OServerConfiguration.DEFAULT_GUEST_PASSWORD);
    }
  }

  private boolean existsSystemUser(String user) {
    return Boolean.TRUE.equals(
        context.execute("EXISTS SYSTEM USER ?", user).next().getProperty("exists"));
  }

  public OServerPluginManager getPluginManager() {
    return pluginManager;
  }

  protected void registerPlugins()
      throws InstantiationException, IllegalAccessException, ClassNotFoundException {
    pluginManager = new OServerPluginManager();
    pluginManager.config(this);
    pluginManager.startup();

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

        pluginManager.registerPlugin(
            new OServerPluginInfo(plugin.getName(), null, null, null, plugin, null, 0, null));

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

  protected void defaultSettings() {}

  public OTokenHandler getTokenHandler() {
    return tokenHandler;
  }

  public ThreadGroup getThreadGroup() {
    return Orient.instance().getThreadGroup();
  }

  private void initSystemDatabase() {
    databases.getSystemDatabase().init();
  }

  public OrientDBInternal getDatabases() {
    return databases;
  }

  public OrientDB getContext() {
    return context;
  }

  public void dropDatabase(String databaseName) {
    if (databases.exists(databaseName, null, null)) {
      databases.drop(databaseName, null, null);
    } else {
      throw new OStorageException("Database with name '" + databaseName + "' does not exist");
    }
  }

  public boolean existsDatabase(String databaseName) {
    return databases.exists(databaseName, null, null);
  }

  public void createDatabase(String databaseName, ODatabaseType type, OrientDBConfig config) {
    databases.create(databaseName, null, null, type, config);
  }

  public Set<String> listDatabases() {
    Set<String> dbs = databases.listDatabases(null, null);
    dbs.remove(OSystemDatabase.SYSTEM_DB_NAME);
    return dbs;
  }

  public void restore(String name, String path) {
    databases.restore(name, null, null, null, path, OrientDBConfig.defaultConfig());
  }

  public Date getStartedOn() {
    return startedOn;
  }
}
