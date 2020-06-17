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

package com.orientechnologies.orient.server.handler;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.OConstants;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;
import com.orientechnologies.orient.server.plugin.OServerPluginAbstract;
import java.lang.management.ManagementFactory;
import javax.management.MBeanServer;
import javax.management.ObjectName;

public class OJMXPlugin extends OServerPluginAbstract {
  private ObjectName onProfiler;
  private boolean profilerManaged;

  public OJMXPlugin() {}

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

    OLogManager.instance()
        .info(this, "JMX plugin installed and active: profilerManaged=%s", profilerManaged);

    final MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();

    try {
      if (profilerManaged) {
        // REGISTER THE PROFILER
        onProfiler = new ObjectName("com.orientechnologies.common.profiler:type=OProfilerMXBean");
        if (mBeanServer.isRegistered(onProfiler)) mBeanServer.unregisterMBean(onProfiler);
        mBeanServer.registerMBean(Orient.instance().getProfiler(), onProfiler);
      }

    } catch (Exception e) {
      throw OException.wrapException(
          new OConfigurationException("Cannot initialize JMX server"), e);
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
        if (mBeanServer.isRegistered(onProfiler)) mBeanServer.unregisterMBean(onProfiler);

    } catch (Exception e) {
      OLogManager.instance()
          .error(this, "OrientDB Server v" + OConstants.getVersion() + " unregisterMBean error", e);
    }
  }

  @Override
  public String getName() {
    return "jmx";
  }
}
