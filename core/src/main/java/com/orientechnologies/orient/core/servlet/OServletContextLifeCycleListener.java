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
package com.orientechnologies.orient.core.servlet;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.servlet.annotation.WebListener;

/**
 * Listener which is used to automatically start/shutdown OrientDB engine inside of web application
 * container.
 */
@SuppressWarnings("unused")
@WebListener
public class OServletContextLifeCycleListener implements ServletContextListener {
  @Override
  public void contextInitialized(ServletContextEvent sce) {
    if (OGlobalConfiguration.INIT_IN_SERVLET_CONTEXT_LISTENER.getValueAsBoolean()) {
      OLogManager.instance()
          .infoNoDb(this, "Start web application is detected, OrientDB engine is staring up...");
      Orient.startUp(true);
      OLogManager.instance().infoNoDb(this, "OrientDB engine is started");
    }
  }

  @Override
  public void contextDestroyed(ServletContextEvent sce) {
    if (OGlobalConfiguration.INIT_IN_SERVLET_CONTEXT_LISTENER.getValueAsBoolean()) {
      final Orient orient = Orient.instance();
      if (orient != null) {
        OLogManager.instance()
            .infoNoDb(
                this,
                "Shutting down of OrientDB engine because web application is going to be stopped");
        orient.shutdown();
        OLogManager.instance().shutdown();
      }
    }
  }
}
