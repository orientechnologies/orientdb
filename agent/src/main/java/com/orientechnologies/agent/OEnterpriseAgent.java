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

import com.orientechnologies.agent.http.command.OServerCommandConfiguration;
import com.orientechnologies.agent.http.command.OServerCommandGetDistributed;
import com.orientechnologies.agent.http.command.OServerCommandGetLog;
import com.orientechnologies.agent.http.command.OServerCommandGetProfiler;
import com.orientechnologies.agent.http.command.OServerCommandPostBackupDatabase;
import com.orientechnologies.agent.profiler.OEnterpriseProfiler;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.profiler.OAbstractProfiler;
import com.orientechnologies.common.profiler.OAbstractProfiler.OProfilerHookValue;
import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.common.profiler.OProfilerMBean;
import com.orientechnologies.common.profiler.OProfilerMBean.METRIC_TYPE;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;
import com.orientechnologies.orient.server.network.OServerNetworkListener;
import com.orientechnologies.orient.server.network.protocol.http.ONetworkProtocolHttpAbstract;
import com.orientechnologies.orient.server.plugin.OServerPluginAbstract;

public class OEnterpriseAgent extends OServerPluginAbstract {
  private OServer server;
  private String  license;

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
    checkLicense();
    installProfiler();
    installCommands();
  }

  @Override
  public void shutdown() {
    uninstallCommands();
    uninstallProfiler();
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
  }

  private void installProfiler() {
    final OAbstractProfiler currentProfiler = (OAbstractProfiler) Orient.instance().getProfiler();

    Orient.instance().setProfiler(new OEnterpriseProfiler(60, 60, currentProfiler));
    Orient.instance().getProfiler().startup();

    currentProfiler.shutdown();
  }

  private void uninstallProfiler() {
    final OProfilerMBean currentProfiler = Orient.instance().getProfiler();

    Orient.instance().setProfiler(new OProfiler((OProfiler) currentProfiler));
    Orient.instance().getProfiler().startup();

    currentProfiler.shutdown();
  }

  private void checkLicense() {
    try {
      OLogManager.instance().warn(null, "License check ok. It expires in %d days", OL.checkDate(license));

      Orient
          .instance()
          .getProfiler()
          .registerHookValue(Orient.instance().getProfiler().getSystemMetric("config.license"), "Enterprise License",
              METRIC_TYPE.TEXT, new OProfilerHookValue() {

                @Override
                public Object getValue() {
                  return license;
                }
              });

    } catch (OL.OLicenseException e) {
      OLogManager.instance().warn(null, "Error on validating Enterprise License (%s): enterprise features will be disabled",
          e.getMessage());
    }
  }
}
