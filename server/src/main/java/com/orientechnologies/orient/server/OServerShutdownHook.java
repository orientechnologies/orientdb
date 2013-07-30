/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.server;

import com.orientechnologies.orient.core.Orient;

public class OServerShutdownHook extends Thread {
  private OServer server;

  protected OServerShutdownHook(final OServer iServer) {
    server = iServer;
    Orient.instance().removeShutdownHook();
    Runtime.getRuntime().addShutdownHook(this);
  }

  /**
   * Catch the JVM exit and assure to shutdown the Orient Server.
   */
  @Override
  public void run() {
    if (server != null)
      server.shutdown();
  }

  public void cancel() {
    try {
      Runtime.getRuntime().removeShutdownHook(this);
    } catch (IllegalStateException e) {
    }
  }
}
