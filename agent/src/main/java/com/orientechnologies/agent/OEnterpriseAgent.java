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

import com.orientechnologies.agent.functions.OAgentProfilerService;
import com.orientechnologies.agent.ha.OEnterpriseDistributedStrategy;
import com.orientechnologies.agent.http.command.*;
import com.orientechnologies.agent.profiler.OEnterpriseProfiler;
import com.orientechnologies.agent.services.OEnterpriseService;
import com.orientechnologies.agent.services.backup.OBackupService;
import com.orientechnologies.agent.services.distributed.ODistributedService;
import com.orientechnologies.agent.services.metrics.OrientDBMetricsService;
import com.orientechnologies.agent.services.security.OSecurityService;
import com.orientechnologies.agent.services.studio.StudioService;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.profiler.OAbstractProfiler.OProfilerHookValue;
import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.enterprise.server.OEnterpriseServer;
import com.orientechnologies.enterprise.server.OEnterpriseServerImpl;
import com.orientechnologies.orient.core.OConstants;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.db.ODatabaseLifecycleListener;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.engine.OEngine;
import com.orientechnologies.orient.core.enterprise.OEnterpriseEndpoint;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.ORule;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.OClientConnection;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.OServerLifecycleListener;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;
import com.orientechnologies.orient.server.distributed.ODistributedConfiguration;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.distributed.OModifiableDistributedConfiguration;
import com.orientechnologies.orient.server.distributed.impl.ODatabaseDocumentDistributed;
import com.orientechnologies.orient.server.hazelcast.OHazelcastPlugin;
import com.orientechnologies.orient.server.network.OServerNetworkListener;
import com.orientechnologies.orient.server.network.protocol.http.ONetworkProtocolHttpAbstract;
import com.orientechnologies.orient.server.plugin.OPluginLifecycleListener;
import com.orientechnologies.orient.server.plugin.OServerPlugin;
import com.orientechnologies.orient.server.plugin.OServerPluginAbstract;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

public class OEnterpriseAgent extends OServerPluginAbstract
    implements ODatabaseLifecycleListener,
        OPluginLifecycleListener,
        OServerLifecycleListener,
        OEnterpriseEndpoint {
  private static final String PLUGIN_NAME = "enterprise-agent";
  private static final String PATH_TO_EE_AGENT_PROPS = "/com/orientechnologies/agent.properties";
  private static final String EE_VERSION = "version";
  private static final boolean PLUGIN_ENABLED_DEFAULT = false;

  private String enterpriseVersion = "";
  public OServer server;
  private Properties properties = new Properties();

  private List<OEnterpriseService> services = new ArrayList<>();

  private OEnterpriseServer enterpriseServer;

  public OEnterpriseAgent() {}

  @Override
  public void config(final OServer oServer, final OServerParameterConfiguration[] iParams) {
    enabled = PLUGIN_ENABLED_DEFAULT;
    server = oServer;

    enterpriseServer = new OEnterpriseServerImpl(server, this);

    if (oServer.getPluginManager() != null) {
      oServer.getPluginManager().registerLifecycleListener(this);
    }
    registerAndInitServices();
  }

  private void registerAndInitServices() {
    this.services.add(new StudioService());
    this.services.add(new OrientDBMetricsService());
    this.services.add(new OAgentProfilerService());
    this.services.add(new OBackupService());
    this.services.add(new OSecurityService());
    this.services.add(new ODistributedService());

    this.services.forEach((s) -> s.init(this.enterpriseServer));
  }

  @Override
  public String getName() {
    return PLUGIN_NAME;
  }

  @Override
  public void startup() {
    try {
      loadAgentProperties();
      if (checkLicense() && checkVersion()) {
        server.registerLifecycleListener(this);
        enabled = true;
        Orient.instance().addDbLifecycleListener(this);
      }
    } catch (final Exception e) {
      OLogManager.instance()
          .warn(
              this, "Error loading agent.properties file. EE will be disabled: %s", e.getMessage());
    }
  }

  @Override
  public void shutdown() {
    if (enabled) {
      uninstallCommands();
      if (server.getPluginManager() != null) {
        server.getPluginManager().unregisterLifecycleListener(this);
      }
      Orient.instance().removeDbLifecycleListener(this);
    }
  }

  @Override
  public PRIORITY getPriority() {
    return PRIORITY.LAST;
  }

  /** Auto register myself as hook. */
  @Override
  public void onOpen(final ODatabaseInternal iDatabase) {}

  @Override
  public void onCreate(ODatabaseInternal iDatabase) {
    onOpen(iDatabase);
  }

  /** Remove myself as hook. */
  @Override
  public void onClose(final ODatabaseInternal iDatabase) {}

  @Override
  public void onDrop(final ODatabaseInternal iDatabase) {}

  @Override
  public void onCreateClass(final ODatabaseInternal iDatabase, final OClass iClass) {}

  @Override
  public void onDropClass(final ODatabaseInternal iDatabase, final OClass iClass) {}

  // TODO SEND CPU METRICS ON configuration request;
  @Override
  public void onLocalNodeConfigurationRequest(ODocument iConfiguration) {
    final OProfiler profiler = Orient.instance().getProfiler();
    final OEngine plocal = Orient.instance().getEngine("plocal");

    if (profiler instanceof OEnterpriseProfiler) {
      iConfiguration.field("cpu", ((OEnterpriseProfiler) profiler).cpuUsage());
    }
  }

  @Deprecated
  public void installDistributedCommands() {
    final OServerNetworkListener listener =
        server.getListenerByProtocol(ONetworkProtocolHttpAbstract.class);
    if (listener == null) throw new OConfigurationException("HTTP listener not found");
  }

  private void uninstallCommands() {
    final OServerNetworkListener listener =
        server.getListenerByProtocol(ONetworkProtocolHttpAbstract.class);
    if (listener == null) {
      throw new OConfigurationException("HTTP listener not found");
    }
    listener.unregisterStatelessCommand(OServerCommandDistributedManager.class);
  }

  private boolean checkLicense() {
    OLogManager.instance().info(this, "");
    OLogManager.instance()
        .info(
            this, "*****************************************************************************");
    OLogManager.instance()
        .info(
            this, "*                     ORIENTDB  -  ENTERPRISE EDITION                       *");
    OLogManager.instance()
        .info(
            this, "*****************************************************************************");
    OLogManager.instance()
        .info(
            this, "* If you are in Production or Test, you must purchase a commercial license. *");
    OLogManager.instance()
        .info(
            this, "* For more information look at: http://orientdb.com/orientdb-enterprise/    *");
    OLogManager.instance()
        .info(
            this, "*****************************************************************************");
    OLogManager.instance().info(this, "");
    Orient.instance()
        .getProfiler()
        .registerHookValue(
            Orient.instance().getProfiler().getSystemMetric("config.agentVersion"),
            "Enterprise License",
            OProfiler.METRIC_TYPE.TEXT,
            new OProfilerHookValue() {
              @Override
              public Object getValue() {
                return enterpriseVersion;
              }
            });
    return true;
  }

  private void loadAgentProperties() throws IOException {
    final InputStream inputStream =
        OEnterpriseAgent.class.getResourceAsStream(PATH_TO_EE_AGENT_PROPS);
    try {
      properties.load(inputStream);
      enterpriseVersion = properties.getProperty(EE_VERSION);
      if (enterpriseVersion == null || enterpriseVersion.isEmpty()) {
        throw new IllegalArgumentException(
            "Cannot read the agent version from the agent config file.");
      }
    } finally {
      try {
        inputStream.close();
      } catch (IOException e) {
        OLogManager.instance().warn(this, "Failed to close input stream " + inputStream);
      }
    }
  }

  private boolean checkVersion() {
    if (OConstants.getRawVersion().equalsIgnoreCase(enterpriseVersion)) {
      return true;
    }

    String ceLower = OConstants.getRawVersion().toLowerCase(Locale.ENGLISH);
    String eeLower = enterpriseVersion.toLowerCase(Locale.ENGLISH);

    if (ceLower.contains("-snapshot") && eeLower.contains("snapshot")) {
      ceLower.replace("-snapshot", "");
      eeLower.replace("-snapshot", "");
    }

    if (eeLower.contains("-sap")) {
      eeLower = eeLower.substring(0, eeLower.indexOf("-sap"));
    }
    if (eeLower.equals(ceLower)) {
      return true;
    }

    OLogManager.instance()
        .warn(
            this,
            "The current agent version %s is not compatible with OrientDB %s. Please use the same version.",
            enterpriseVersion,
            OConstants.getVersion());
    return false;
  }

  private void installComponents() {
    if (server.getDistributedManager() != null) {
      server.getDistributedManager().setDistributedStrategy(new OEnterpriseDistributedStrategy());
    }
  }

  // OPluginLifecycleListener
  public void onBeforeConfig(
      final OServerPlugin plugin, final OServerParameterConfiguration[] cfg) {}

  public void onAfterConfig(
      final OServerPlugin plugin, final OServerParameterConfiguration[] cfg) {}

  public void onBeforeStartup(final OServerPlugin plugin) {
    if (plugin instanceof ODistributedServerManager) {
      installComponents();
    }
  }

  public void onAfterStartup(final OServerPlugin plugin) {}

  public void onBeforeShutdown(final OServerPlugin plugin) {}

  public void onAfterShutdown(final OServerPlugin plugin) {}

  @Override
  public void onBeforeClientRequest(final OClientConnection iConnection, final byte iRequestType) {}

  public boolean isDistributed() {
    return server.getDistributedManager() != null;
  }

  public ODistributedServerManager getDistributedManager() {

    return server.getDistributedManager();
  }

  public String getNodeName() {
    return isDistributed() ? server.getDistributedManager().getLocalNodeName() : "orientdb";
  }

  @Override
  public void onBeforeActivate() {}

  @Override
  public void onAfterActivate() {
    services.forEach((s) -> s.start());
  }

  @Override
  public void onBeforeDeactivate() {
    services.forEach(
        (s) -> {
          s.stop();
        });
  }

  @Override
  public void onAfterDeactivate() {}

  @Override
  public void onAfterClientRequest(OClientConnection iConnection, byte iRequestType) {
    super.onAfterClientRequest(iConnection, iRequestType);
  }

  public <T extends OEnterpriseService> Optional<T> getServiceByClass(Class<T> klass) {
    return (Optional<T>) this.services.stream().filter(c -> c.getClass().equals(klass)).findFirst();
  }

  @Override
  public void haSetDbStatus(
      final ODatabaseDocument database, final String nodeName, final String status) {
    database.checkSecurity(ORule.ResourceGeneric.SERVER, "status", ORole.PERMISSION_UPDATE);
    if (!(database instanceof ODatabaseDocumentDistributed)) {
      throw new OCommandExecutionException("OrientDB is not started in distributed mode");
    }
    final OHazelcastPlugin dManager =
        (OHazelcastPlugin) ((ODatabaseDocumentDistributed) database).getDistributedManager();
    if (dManager == null || !dManager.isEnabled()) {
      throw new OCommandExecutionException("OrientDB is not started in distributed mode");
    }
    final String databaseName = database.getName();
    dManager.getDatabaseConfiguration(databaseName);
    dManager.setDatabaseStatus(
        nodeName, databaseName, ODistributedServerManager.DB_STATUS.valueOf(status));
  }

  @Override
  public void haSetRole(ODatabaseDocument database, String serverName, String role) {
    database.checkSecurity(ORule.ResourceGeneric.SERVER, "status", ORole.PERMISSION_UPDATE);

    if (!(database instanceof ODatabaseDocumentDistributed)) {
      throw new OCommandExecutionException("OrientDB is not started in distributed mode");
    }

    final OHazelcastPlugin dManager =
        (OHazelcastPlugin) ((ODatabaseDocumentDistributed) database).getDistributedManager();
    if (dManager == null || !dManager.isEnabled()) {
      throw new OCommandExecutionException("OrientDB is not started in distributed mode");
    }
    final String databaseName = database.getName();
    final ODistributedConfiguration cfg = dManager.getDatabaseConfiguration(databaseName);
    final OModifiableDistributedConfiguration newCfg = cfg.modify();
    newCfg.setServerRole(serverName, ODistributedConfiguration.ROLES.valueOf(role));
    dManager.updateCachedDatabaseConfiguration(databaseName, newCfg, true);
  }

  @Override
  public void haSetOwner(ODatabaseDocument database, String clusterName, String owner) {
    database.checkSecurity(ORule.ResourceGeneric.SERVER, "status", ORole.PERMISSION_UPDATE);
    if (!(database instanceof ODatabaseDocumentDistributed)) {
      throw new OCommandExecutionException("OrientDB is not started in distributed mode");
    }
    final OHazelcastPlugin dManager =
        (OHazelcastPlugin) ((ODatabaseDocumentDistributed) database).getDistributedManager();
    if (dManager == null || !dManager.isEnabled()) {
      throw new OCommandExecutionException("OrientDB is not started in distributed mode");
    }
    final String databaseName = database.getName();
    final ODistributedConfiguration cfg = dManager.getDatabaseConfiguration(databaseName);
    final OModifiableDistributedConfiguration newCfg = cfg.modify();
    newCfg.setServerOwner(clusterName, owner);
    dManager.updateCachedDatabaseConfiguration(databaseName, newCfg, true);
  }

  public OEnterpriseServer getEnterpriseServer() {
    return enterpriseServer;
  }
}
