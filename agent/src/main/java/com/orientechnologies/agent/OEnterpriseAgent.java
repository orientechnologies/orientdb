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

import java.util.Map;

import com.orientechnologies.agent.OL.OLicenseException;
import com.orientechnologies.agent.http.command.*;
import com.orientechnologies.agent.profiler.OEnterpriseProfiler;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.profiler.OAbstractProfiler;
import com.orientechnologies.common.profiler.OAbstractProfiler.OProfilerHookValue;
import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.common.profiler.OProfilerMBean;
import com.orientechnologies.common.profiler.OProfilerMBean.METRIC_TYPE;
import com.orientechnologies.orient.core.OConstants;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.OServerMain;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.network.OServerNetworkListener;
import com.orientechnologies.orient.server.network.protocol.http.ONetworkProtocolHttpAbstract;
import com.orientechnologies.orient.server.plugin.OServerPluginAbstract;

public class OEnterpriseAgent extends OServerPluginAbstract {
  private OServer             server;
  private String              license;
  private String              version;
  private boolean             enabled                    = false;
  private static final String ORIENDB_ENTERPRISE_VERSION = "1.7-SNAPSHOT"; // CHECK IF THE ORIENTDB COMMUNITY EDITION STARTS WITH
                                                                           // THIS

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

      ODistributedServerManager manager = OServerMain.server().getDistributedManager();
      Map<String, Object> map = manager.getConfigurationMap();
      map.put("ee." + manager.getLocalNodeName(), OServerMain.server().getConfiguration().getUser("root").password);

    }
  }

  @Override
  public void shutdown() {
    if (enabled) {
      uninstallCommands();
      uninstallProfiler();
    }
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

  private boolean checkLicense() {
    try {
      if (!OConstants.ORIENT_VERSION.startsWith(ORIENDB_ENTERPRISE_VERSION))
        throw new OLicenseException("enterprise license v." + ORIENDB_ENTERPRISE_VERSION
            + " is different than linked jars with OrientDB v." + OConstants.ORIENT_VERSION);

      final int dayLeft = OL.checkDate(license);

      System.out.printf("\n\n************************************************");
      System.out.printf("\n*       ORIENTDB  -  ENTERPRISE EDITION        *");
      System.out.printf("\n*                                              *");
      System.out.printf("\n* Copyrights (c) 2014 Orient Technologies LTD  *");
      System.out.printf("\n************************************************");
      System.out.printf("\n* Version...: %-32s *", ORIENDB_ENTERPRISE_VERSION);
      System.out.printf("\n* License...: %-32s *", license);
      System.out.printf("\n* Expires in: %03d days                         *", dayLeft);
      System.out.printf("\n************************************************\n");

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
      Orient
          .instance()
          .getProfiler()
          .registerHookValue(Orient.instance().getProfiler().getSystemMetric("config.agentVersion"), "Enterprise License",
              METRIC_TYPE.TEXT, new OProfilerHookValue() {

                @Override
                public Object getValue() {
                  return version;
                }
              });
    } catch (OL.OLicenseException e) {
      OLogManager.instance().warn(null, "Error on validating Enterprise License (%s): enterprise features will be disabled",
          e.getMessage());
      return false;
    }
    return true;
  }

}
