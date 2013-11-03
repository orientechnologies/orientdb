package com.orientechnologies.orient.server.handler;

import java.lang.management.ManagementFactory;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.OConstants;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;
import com.orientechnologies.orient.server.managed.OrientServer;
import com.orientechnologies.orient.server.plugin.OServerPluginAbstract;

public class OJMXPlugin extends OServerPluginAbstract {
  private OrientServer managedServer;
  private ObjectName   onProfiler;
  private ObjectName   onServer;
  private boolean      profilerManaged;

  public OJMXPlugin() {
  }

  @Override
  public void config(final OServer oServer, final OServerParameterConfiguration[] iParams) {
    for (OServerParameterConfiguration param : iParams) {
      if (param.name.equalsIgnoreCase("enabled")) {
        if (!Boolean.parseBoolean(param.value))
          // DISABLE IT
          return;
      } else if (param.name.equalsIgnoreCase("profilerManaged"))
        profilerManaged = Boolean.parseBoolean(param.value);
    }

    OLogManager.instance().info(this, "JMX plugin installed and active: profilerManaged=%s", profilerManaged);

    final MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();

    try {
      if (profilerManaged) {
        // REGISTER THE PROFILER
        onProfiler = new ObjectName("OrientDB:type=Profiler");
        mBeanServer.registerMBean(Orient.instance().getProfiler(), onProfiler);
      }

      // REGISTER SERVER
      onServer = new ObjectName("OrientDB:type=Server");
      managedServer = new OrientServer();
      mBeanServer.registerMBean(managedServer, onServer);

    } catch (Exception e) {
      throw new OConfigurationException("Cannot initialize JMX server", e);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.orientechnologies.orient.server.handler.OServerHandlerAbstract#shutdown()
   */
  @Override
  public void shutdown() {
    try {
      MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
      if (onProfiler != null)
        if (mBeanServer.isRegistered(onProfiler))
          mBeanServer.unregisterMBean(onProfiler);

      if (onServer != null)
        if (mBeanServer.isRegistered(onServer))
          mBeanServer.unregisterMBean(onServer);
    } catch (Exception e) {
      OLogManager.instance().error(this, "OrientDB Server v" + OConstants.ORIENT_VERSION + " unregisterMBean error.", e);
    }

  }

  @Override
  public String getName() {
    return "jmx";
  }

  public OrientServer getManagedServer() {
    return managedServer;
  }
}
