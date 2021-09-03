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

import com.orientechnologies.common.log.OLogManager;

public class OServerMain {
  private static OServer instance;

  public static OServer create() throws Exception {
    instance = new OServer();
    return instance;
  }

  public static OServer create(boolean shutdownEngineOnExit) throws Exception {
    instance = new OServer(shutdownEngineOnExit);
    return instance;
  }

  public static OServer server() {
    return instance;
  }

  public static void main(final String[] args) throws Exception {
    // STARTS ORIENTDB IN A NON DAEMON THREAD TO PREVENT EXIT
    final Thread t =
        new Thread() {
          @Override
          public void run() {
            try {
              instance = OServerMain.create();
              instance.startup().activate();
              instance.waitForShutdown();
            } catch (Exception e) {
              OLogManager.instance().error(this, "Error during server execution", e);
            }
          }
        };

    t.setDaemon(false);

    t.start();
    t.join();
    System.exit(1);
  }
}
