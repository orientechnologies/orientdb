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

import com.orientechnologies.agent.hook.OAuditingHook;
import com.orientechnologies.agent.http.command.*;
import com.orientechnologies.agent.plugins.OEventPlugin;
import com.orientechnologies.agent.profiler.OEnterpriseProfiler;
import com.orientechnologies.common.profiler.OAbstractProfiler;
import com.orientechnologies.common.profiler.OAbstractProfiler.OProfilerHookValue;
import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.common.profiler.OProfilerStub;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.db.ODatabaseLifecycleListener;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.hook.ORecordHook;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.OServerMain;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.network.OServerNetworkListener;
import com.orientechnologies.orient.server.network.protocol.http.ONetworkProtocolHttpAbstract;
import com.orientechnologies.orient.server.plugin.OServerPluginAbstract;
import com.orientechnologies.orient.server.plugin.OServerPluginInfo;

import java.util.Map;

public class OEnterpriseAgent extends OServerPluginAbstract implements ODatabaseLifecycleListener {
  public static final String  EE                         = "ee.";
  private static final String ORIENDB_ENTERPRISE_VERSION = "2.2"; // CHECK IF THE ORIENTDB COMMUNITY EDITION STARTS WITH THIS
  public OServer              server;
  private String              license;
  private boolean             enabled                    = false;
  public OAuditingListener    auditingListener;

  public OEnterpriseAgent() {
  }

  @Override
  public void config(OServer oServer, OServerParameterConfiguration[] iParams) {
    server = oServer;
    for (OServerParameterConfiguration p : iParams) {
      if (p.name.equals("license"))
        license = p.value;
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
      installCommands();

      installPlugins();

      auditingListener = new OAuditingListener(this);

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
              Map<String, Object> map = manager.getConfigurationMap();
              String pwd = OServerMain.server().getUser("root").password;
              try {
                String enc = OL.encrypt(pwd);
                map.put(EE + manager.getLocalNodeName(), enc);
              } catch (Exception e) {
                e.printStackTrace();
              }

              break;
            }

          }

        }
      });

      installer.setDaemon(true);
      installer.start();
      Orient.instance().addDbLifecycleListener(this);
    }
  }

  private void installPlugins() {

    OEventPlugin eventPlugin = new OEventPlugin();

    eventPlugin.config(server, null);
    eventPlugin.startup();
    server.getPluginManager().registerPlugin(
        new OServerPluginInfo(eventPlugin.getName(), null, null, null, eventPlugin, null, 0, null));
  }

  @Override
  public void shutdown() {
    if (enabled) {
      uninstallCommands();
      uninstallProfiler();
      Orient.instance().removeDbLifecycleListener(this);
    }
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
    // if (iDatabase.getProperty(OAuditingLoggingThread.FROM_AUDITING) != null)
    // // DON'T INSTALL HOOK ON THE DB OPEN INSIDE THE AUDITING THREAD
    // return;
    //
    // final String dbUrl = OSystemVariableResolver.resolveSystemVariables(iDatabase.getURL());
    //
    // // REGISTER AUDITING
    // final ODocument auditingCfg = new ODocument();
    // final OAuditingHook auditing = new OAuditingHook(auditingCfg);
    // iDatabase.registerHook(auditing);
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
    // final Map<ORecordHook, ORecordHook.HOOK_POSITION> hooks = iDatabase.getHooks();
    // for (ORecordHook h : hooks.keySet())
    // if (h instanceof OAuditingHook)
    // ((OAuditingHook) h).shutdown(true);
  }

  @Override
  public void onDrop(final ODatabaseInternal iDatabase) {
    final Map<ORecordHook, ORecordHook.HOOK_POSITION> hooks = iDatabase.getHooks();
    for (ORecordHook h : hooks.keySet())
      if (h instanceof OAuditingHook)
        ((OAuditingHook) h).shutdown(false);
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
    listener.registerStatelessCommand(new OServerCommandAuditing(this));

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
  }

  private void installProfiler() {
    final OAbstractProfiler currentProfiler = (OAbstractProfiler) Orient.instance().getProfiler();

    Orient.instance().setProfiler(new OEnterpriseProfiler(60, 24, currentProfiler, server));
    Orient.instance().getProfiler().startup();

    currentProfiler.shutdown();
  }

  private void uninstallProfiler() {
    final OProfiler currentProfiler = Orient.instance().getProfiler();

    Orient.instance().setProfiler(new OProfilerStub((OAbstractProfiler) currentProfiler));
    Orient.instance().getProfiler().startup();

    currentProfiler.shutdown();
  }

  private boolean checkLicense() {

    System.out.printf("\n\n********************************************************************");
    System.out.printf("\n*                 ORIENTDB  -  ENTERPRISE EDITION                  *");
    System.out.printf("\n*                                                                  *");
    System.out.printf("\n********************************************************************");
    Orient
        .instance()
        .getProfiler()
        .registerHookValue(Orient.instance().getProfiler().getSystemMetric("config.agentVersion"), "Enterprise License",
            OProfiler.METRIC_TYPE.TEXT, new OProfilerHookValue() {

              @Override
              public Object getValue() {
                return ORIENDB_ENTERPRISE_VERSION;
              }
            });

    return true;
  }
}
