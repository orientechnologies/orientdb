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
import com.orientechnologies.agent.http.command.OServerCommandAuditing;
import com.orientechnologies.agent.http.command.OServerCommandConfiguration;
import com.orientechnologies.agent.http.command.OServerCommandGetDeployDb;
import com.orientechnologies.agent.http.command.OServerCommandGetDistributed;
import com.orientechnologies.agent.http.command.OServerCommandGetLog;
import com.orientechnologies.agent.http.command.OServerCommandGetProfiler;
import com.orientechnologies.agent.http.command.OServerCommandGetSQLProfiler;
import com.orientechnologies.agent.http.command.OServerCommandPostBackupDatabase;
import com.orientechnologies.agent.profiler.OEnterpriseProfiler;
import com.orientechnologies.common.parser.OSystemVariableResolver;
import com.orientechnologies.common.profiler.OAbstractProfiler;
import com.orientechnologies.common.profiler.OAbstractProfiler.OProfilerHookValue;
import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.common.profiler.OProfilerStub;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.db.ODatabaseLifecycleListener;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.OServerMain;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.network.OServerNetworkListener;
import com.orientechnologies.orient.server.network.protocol.http.ONetworkProtocolHttpAbstract;
import com.orientechnologies.orient.server.plugin.OServerPluginAbstract;

import java.util.Map;

public class OEnterpriseAgent extends OServerPluginAbstract implements ODatabaseLifecycleListener {
  public static final String       EE                         = "ee.";
  private static final String      ORIENDB_ENTERPRISE_VERSION = "2.1"; // CHECK IF THE ORIENTDB COMMUNITY EDITION STARTS WITH THIS
  public OServer                   server;
  private String                   license;
  private String                   version;
  private boolean                  enabled                    = false;
  private DatabaseProfilerResource profilerResource;
  public OAuditingListener         auditingListener;

  public OEnterpriseAgent() {
  }

  @Override
  public void config(OServer oServer, OServerParameterConfiguration[] iParams) {
    server = oServer;
    for (OServerParameterConfiguration p : iParams) {
      if (p.name.equals("license"))
        license = p.value;
      if (p.name.equals("version"))
        version = p.value;
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
      profilerResource = new DatabaseProfilerResource();

      auditingListener = new OAuditingListener(this);
      Orient.instance().addDbLifecycleListener(auditingListener);
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
              String pwd = OServerMain.server().getConfiguration().getUser("root").password;
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
    return PRIORITY.REGULAR;
  }

  /**
   * Auto register myself as hook.
   */
  @Override
  public void onOpen(final ODatabaseInternal iDatabase) {
    final String dbUrl = OSystemVariableResolver.resolveSystemVariables(iDatabase.getURL());

    // REGISTER AUDITING
    final ODocument auditingCfg = new ODocument();
    final OAuditingHook auditing = new OAuditingHook(auditingCfg);
    iDatabase.registerHook(auditing);
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
  public void onDrop(ODatabaseInternal iDatabase) {

  }

  @Override
  public void onCreateClass(ODatabaseInternal iDatabase, OClass iClass) {

  }

  @Override
  public void onDropClass(ODatabaseInternal iDatabase, OClass iClass) {

  }

  private void installCommands() {
    final OServerNetworkListener listener = server.getListenerByProtocol(ONetworkProtocolHttpAbstract.class);
    if (listener == null)
      throw new OConfigurationException("HTTP listener not found");

    listener.registerStatelessCommand(new OServerCommandGetProfiler());
    listener.registerStatelessCommand(new OServerCommandGetDistributed());
    listener.registerStatelessCommand(new OServerCommandGetLog());
    listener.registerStatelessCommand(new OServerCommandConfiguration());
    listener.registerStatelessCommand(new OServerCommandPostBackupDatabase());
    listener.registerStatelessCommand(new OServerCommandGetDeployDb());
    listener.registerStatelessCommand(new OServerCommandGetSQLProfiler());
    listener.registerStatelessCommand(new OServerCommandAuditing(this));
  }

  private void uninstallCommands() {
    final OServerNetworkListener listener = server.getListenerByProtocol(ONetworkProtocolHttpAbstract.class);
    if (listener == null)
      throw new OConfigurationException("HTTP listener not found");

    listener.unregisterStatelessCommand(OServerCommandGetProfiler.class);
    listener.unregisterStatelessCommand(OServerCommandGetDistributed.class);
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

    // final int dayLeft = OL.checkDate(license);
    final int dayLeft = 50;

    System.out.printf("\n\n********************************************************************");
    System.out.printf("\n*                 ORIENTDB  -  ENTERPRISE EDITION                  *");
    System.out.printf("\n*                                                                  *");
    // System.out.printf("\n*            " + OConstants.COPYRIGHT + "           *");
    System.out.printf("\n********************************************************************");

    Orient
        .instance()
        .getProfiler()
        .registerHookValue(Orient.instance().getProfiler().getSystemMetric("config.license"), "Enterprise License",
            OProfiler.METRIC_TYPE.TEXT, new OProfilerHookValue() {

              @Override
              public Object getValue() {
                return license;
              }
            });
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
    Orient
        .instance()
        .getProfiler()
        .registerHookValue(Orient.instance().getProfiler().getSystemMetric("config.dayLeft"), "Enterprise License Day Left",
            OProfiler.METRIC_TYPE.TEXT, new OProfilerHookValue() {

              @Override
              public Object getValue() {
                return dayLeft;
              }
            });

    return true;
  }
}
