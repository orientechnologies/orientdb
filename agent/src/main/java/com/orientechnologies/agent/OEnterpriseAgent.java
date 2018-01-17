/*
 * Copyright 2010-2013 Orient Technologies LTD (info--at--orientechnologies.com)
 * All Rights Reserved. Commercial License.
 *
 * NOTICE:  All information contained herein is, and remains the property of
 * Orient Technologies LTD and its suppliers, if any.  The intellectual and
 * technical concepts contained herein are proprietary to
 * Orient Technologies LTD and its suppliers and may be covered by United
 * Kingdom and Foreign Patents, patents in process, and are protected by trade
 * secret or copyright law.
 *
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from Orient Technologies LTD.
 *
 * For more information: http://www.orientechnologies.com
 */
package com.orientechnologies.agent;

import com.orientechnologies.agent.backup.OBackupManager;
import com.orientechnologies.agent.ha.OEnterpriseDistributedStrategy;
import com.orientechnologies.agent.http.command.*;
import com.orientechnologies.agent.operation.NodesManager;
import com.orientechnologies.agent.plugins.OEventPlugin;
import com.orientechnologies.agent.profiler.OEnterpriseProfiler;
import com.orientechnologies.agent.profiler.OEnterpriseProfilerListener;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.profiler.OAbstractProfiler;
import com.orientechnologies.common.profiler.OAbstractProfiler.OProfilerHookValue;
import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.common.profiler.OProfilerStub;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.db.ODatabaseLifecycleListener;
import com.orientechnologies.orient.core.engine.OEngine;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;
import com.orientechnologies.orient.enterprise.channel.binary.ODistributedRedirectException;
import com.orientechnologies.orient.server.OClientConnection;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.OServerMain;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;
import com.orientechnologies.orient.server.distributed.ODistributedConfiguration;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.distributed.impl.ODistributedAbstractPlugin;
import com.orientechnologies.orient.server.hazelcast.OHazelcastPlugin;
import com.orientechnologies.orient.server.network.OServerNetworkListener;
import com.orientechnologies.orient.server.network.protocol.http.ONetworkProtocolHttpAbstract;
import com.orientechnologies.orient.server.plugin.OPluginLifecycleListener;
import com.orientechnologies.orient.server.plugin.OServerPlugin;
import com.orientechnologies.orient.server.plugin.OServerPluginAbstract;
import com.orientechnologies.orient.server.plugin.OServerPluginInfo;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

public class OEnterpriseAgent extends OServerPluginAbstract implements ODatabaseLifecycleListener, OPluginLifecycleListener {
  public static final  String EE                         = "ee.";
  private static final String ORIENDB_ENTERPRISE_VERSION = "2.2"; // CHECK IF THE ORIENTDB COMMUNITY EDITION STARTS WITH THIS
  public              OServer server;
  private             String  license;
  public static final String  TOKEN;

  static {
    String t = null;
    try {
      t = OL.encrypt(UUID.randomUUID().toString());

    } catch (Exception e) {
      e.printStackTrace();
    }
    TOKEN = t;
  }

  private   OBackupManager      backupManager;
  protected OEnterpriseProfiler profiler;


  private OEnterpriseCloudManager cloudManager;

  private NodesManager nodesManager;

  public OEnterpriseAgent() {
  }

  @Override
  public void config(OServer oServer, OServerParameterConfiguration[] iParams) {
    enabled = false;
    server = oServer;
    for (OServerParameterConfiguration p : iParams) {
      if (p.name.equals("license"))
        license = p.value;
    }

    if (oServer.getPluginManager() != null) {
      oServer.getPluginManager().registerLifecycleListener(this);
    }
  }

  @Override
  public String getName() {
    return "enterprise-agent";
  }

  @Override
  public void startup() {
    if (checkLicense()) {
      enabled = true;
      installProfiler();
      installBackupManager();
      installCommands();
      installPlugins();

      registerSecurityComponents();

      Thread installer = new Thread(new Runnable() {
        @Override
        public void run() {

          int retry = 0;
          while (true) {
            ODistributedServerManager manager = OServerMain.server().getDistributedManager();
            if (manager == null) {
              if (retry == 5) {
                break;
              }
              try {
                Thread.sleep(2000);
              } catch (InterruptedException e) {
                e.printStackTrace();
              }
              retry++;
              continue;
            } else {
              OHazelcastPlugin plugin = (OHazelcastPlugin) manager;
              nodesManager = new NodesManager(plugin);
              try {
                plugin.waitUntilNodeOnline();
              } catch (InterruptedException e) {
                e.printStackTrace();
              }
              Map<String, Object> map = manager.getConfigurationMap();
              map.put(EE + manager.getLocalNodeName(), TOKEN);
              manager.registerLifecycleListener(profiler);
              break;
            }

          }

        }
      });

      installer.setDaemon(true);
      installer.start();
      Orient.instance().addDbLifecycleListener(this);
      cloudManager = new OEnterpriseCloudManager(this);

      cloudManager.start();
    }
  }

  @Override
  public void shutdown() {
    if (enabled) {

      cloudManager.shutdown();

      unregisterSecurityComponents();
      uninstallBackupManager();
      uninstallCommands();
      uninstallProfiler();

      if (server.getPluginManager() != null) {
        server.getPluginManager().unregisterLifecycleListener(this);
      }

      Orient.instance().removeDbLifecycleListener(this);
    }
  }

  public OBackupManager getBackupManager() {
    return backupManager;
  }

  public NodesManager getNodesManager() {
    return nodesManager;
  }

  @Override
  public PRIORITY getPriority() {
    return PRIORITY.LAST;
  }

  /**
   * Auto register myself as hook.
   */
  @Override
  public void onOpen(final ODatabaseInternal iDatabase) {

  }

  @Override
  public void onCreate(ODatabaseInternal iDatabase) {
    onOpen(iDatabase);
  }

  /**
   * Remove myself as hook.
   */
  @Override
  public void onClose(final ODatabaseInternal iDatabase) {

  }

  @Override
  public void onDrop(final ODatabaseInternal iDatabase) {

  }

  @Override
  public void onCreateClass(final ODatabaseInternal iDatabase, final OClass iClass) {

  }

  @Override
  public void onDropClass(final ODatabaseInternal iDatabase, final OClass iClass) {

  }

  // TODO SEND CPU METRICS ON configuration request;
  @Override
  public void onLocalNodeConfigurationRequest(ODocument iConfiguration) {

    OProfiler profiler = Orient.instance().getProfiler();

    OEngine plocal = Orient.instance().getEngine("plocal");

    if (profiler instanceof OEnterpriseProfiler) {
      iConfiguration.field("cpu", ((OEnterpriseProfiler) profiler).cpuUsage());
    }

  }

  private void installCommands() {
    final OServerNetworkListener listener = server.getListenerByProtocol(ONetworkProtocolHttpAbstract.class);
    if (listener == null)
      throw new OConfigurationException("HTTP listener not found");

    listener.registerStatelessCommand(new OServerCommandGetProfiler());
    listener.registerStatelessCommand(new OServerCommandDistributedManager());
    listener.registerStatelessCommand(new OServerCommandGetLog());
    listener.registerStatelessCommand(new OServerCommandConfiguration());
    listener.registerStatelessCommand(new OServerCommandPostBackupDatabase());
    listener.registerStatelessCommand(new OServerCommandGetDeployDb());
    listener.registerStatelessCommand(new OServerCommandGetSQLProfiler());
    listener.registerStatelessCommand(new OServerCommandPluginManager());
    listener.registerStatelessCommand(new OServerCommandGetNode());
    listener.registerStatelessCommand(new OServerCommandQueryCacheManager());
    listener.registerStatelessCommand(new OServerCommandAuditing(server));
    listener.registerStatelessCommand(new OServerCommandGetSecurityConfig(server.getSecurity()));
    listener.registerStatelessCommand(new OServerCommandPostSecurityReload(server.getSecurity()));
    listener.registerStatelessCommand(new OServerCommandBackupManager(backupManager));
  }

  private void uninstallCommands() {
    final OServerNetworkListener listener = server.getListenerByProtocol(ONetworkProtocolHttpAbstract.class);
    if (listener == null)
      throw new OConfigurationException("HTTP listener not found");

    listener.unregisterStatelessCommand(OServerCommandGetProfiler.class);
    listener.unregisterStatelessCommand(OServerCommandDistributedManager.class);
    listener.unregisterStatelessCommand(OServerCommandGetLog.class);
    listener.unregisterStatelessCommand(OServerCommandConfiguration.class);
    listener.unregisterStatelessCommand(OServerCommandPostBackupDatabase.class);
    listener.unregisterStatelessCommand(OServerCommandGetDeployDb.class);
    listener.unregisterStatelessCommand(OServerCommandGetSQLProfiler.class);
    listener.unregisterStatelessCommand(OServerCommandPluginManager.class);
    listener.unregisterStatelessCommand(OServerCommandGetNode.class);
    listener.unregisterStatelessCommand(OServerCommandQueryCacheManager.class);
    listener.unregisterStatelessCommand(OServerCommandAuditing.class);
    listener.unregisterStatelessCommand(OServerCommandBackupManager.class);
    listener.unregisterStatelessCommand(OServerCommandGetSecurityConfig.class);
    listener.unregisterStatelessCommand(OServerCommandPostSecurityReload.class);
  }

  protected void installProfiler() {
    final OAbstractProfiler currentProfiler = (OAbstractProfiler) Orient.instance().getProfiler();

    profiler = new OEnterpriseProfiler(60, currentProfiler, server);

    Orient.instance().setProfiler(profiler);
    Orient.instance().getProfiler().startup();
    if (currentProfiler.isRecording())
      profiler.startRecording();

    currentProfiler.shutdown();
  }

  public void registerListener(OEnterpriseProfilerListener listener) {
    if (profiler != null) {
      profiler.registerProfilerListener(listener);
    }
  }

  public void unregisterListener(OEnterpriseProfilerListener listener) {
    if (profiler != null) {
      profiler.unregisterProfilerListener(listener);
    }
  }

  private void uninstallProfiler() {
    final OProfiler currentProfiler = Orient.instance().getProfiler();

    Orient.instance().setProfiler(new OProfilerStub((OAbstractProfiler) currentProfiler));
    Orient.instance().getProfiler().startup();

    currentProfiler.shutdown();
    profiler = null;
  }

  private boolean checkLicense() {

    OLogManager.instance().info(this, "");
    OLogManager.instance().info(this, "*****************************************************************************");
    OLogManager.instance().info(this, "*                     ORIENTDB  -  ENTERPRISE EDITION                       *");
    OLogManager.instance().info(this, "*****************************************************************************");
    OLogManager.instance().info(this, "* If you are in Production or Test, you must purchase a commercial license. *");
    OLogManager.instance().info(this, "* For more information look at: http://orientdb.com/orientdb-enterprise/    *");
    OLogManager.instance().info(this, "*****************************************************************************");
    OLogManager.instance().info(this, "");

    Orient.instance().getProfiler()
        .registerHookValue(Orient.instance().getProfiler().getSystemMetric("config.agentVersion"), "Enterprise License",
            OProfiler.METRIC_TYPE.TEXT, new OProfilerHookValue() {

              @Override
              public Object getValue() {
                return ORIENDB_ENTERPRISE_VERSION;
              }
            });

    return true;
  }

  private void installBackupManager() {
    backupManager = new OBackupManager(server);
  }

  private void uninstallBackupManager() {
    if (backupManager != null) {
      backupManager.shutdown();
      backupManager = null;
    }
  }

  private void installPlugins() {

    try {

      final OEventPlugin eventPlugin = new OEventPlugin();
      eventPlugin.config(server, null);
      eventPlugin.startup();
      server.getPluginManager()
          .registerPlugin(new OServerPluginInfo(eventPlugin.getName(), null, null, null, eventPlugin, null, 0, null));
    } catch (Exception ex) {
    }
  }

  private void installComponents() {
    if (server.getDistributedManager() != null) {
      server.getDistributedManager().setDistributedStrategy(new OEnterpriseDistributedStrategy());
    }
  }

  // OPluginLifecycleListener
  public void onBeforeConfig(final OServerPlugin plugin, final OServerParameterConfiguration[] cfg) {
  }

  public void onAfterConfig(final OServerPlugin plugin, final OServerParameterConfiguration[] cfg) {
  }

  public void onBeforeStartup(final OServerPlugin plugin) {
    if (plugin instanceof ODistributedServerManager) {
      installComponents();
    }
  }

  public void onAfterStartup(final OServerPlugin plugin) {
  }

  public void onBeforeShutdown(final OServerPlugin plugin) {
  }

  public void onAfterShutdown(final OServerPlugin plugin) {
  }

  @Override
  public void onBeforeClientRequest(final OClientConnection iConnection, final byte iRequestType) {
    if (iRequestType == OChannelBinaryProtocol.DISTRIBUTED_REQUEST || iRequestType == OChannelBinaryProtocol.DISTRIBUTED_RESPONSE)
      return;

    if (Orient.instance().isRunningDistributed()) {
      final ODatabaseDocumentInternal db = iConnection.getDatabase();
      if (db != null) {
        if (((OAbstractPaginatedStorage) db.getStorage().getUnderlying()).isFrozen()) {
          final ODistributedServerManager manager = OServerMain.server().getDistributedManager();
          final ODistributedConfiguration dCfg = manager.getDatabaseConfiguration(db.getName());
          final List<String> masters = dCfg.getMasterServers();
          masters.remove(manager.getLocalNodeName());

          // FILTER ONLY THE ONLINE SERVERS
          manager.getNodesWithStatus(masters, db.getName(), ODistributedServerManager.DB_STATUS.ONLINE);

          if (!masters.isEmpty()) {
            // GET A RANDOM MASTER SERVER TO REDIRECT THE REQUEST
            final String toServer = masters.get(new Random().nextInt(masters.size()));
            final String toIPAddress = ODistributedAbstractPlugin
                .getListeningBinaryAddress(manager.getNodeConfigurationByUuid(manager.getNodeUuidByName(toServer), true));

            throw new ODistributedRedirectException(manager.getLocalNodeName(), toServer, toIPAddress,
                "The server is frozen (usually because a backup is running). Redirecting the request to another server");
          }
        }
      }
    }
  }

  private void registerSecurityComponents() {
    try {
      if (server.getSecurity() != null) {
        server.getSecurity()
            .registerSecurityClass(com.orientechnologies.agent.security.authenticator.OSecuritySymmetricKeyAuth.class);
        server.getSecurity()
            .registerSecurityClass(com.orientechnologies.agent.security.authenticator.OSystemSymmetricKeyAuth.class);
      }
    } catch (Throwable th) {
      OLogManager.instance().error(this, "registerSecurityComponents()", th);
    }
  }

  private void unregisterSecurityComponents() {
    try {
      if (server.getSecurity() != null) {
        server.getSecurity()
            .unregisterSecurityClass(com.orientechnologies.agent.security.authenticator.OSecuritySymmetricKeyAuth.class);
        server.getSecurity()
            .unregisterSecurityClass(com.orientechnologies.agent.security.authenticator.OSystemSymmetricKeyAuth.class);
      }
    } catch (Throwable th) {
      OLogManager.instance().error(this, "unregisterSecurityComponents()", th);
    }
  }

  public boolean isDistributed() {
    return server.getDistributedManager() != null;
  }

  public ODistributedServerManager getDistributedManager(){

    return server.getDistributedManager();
  }
}
