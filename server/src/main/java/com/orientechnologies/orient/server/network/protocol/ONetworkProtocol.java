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
package com.orientechnologies.orient.server.network.protocol;

import java.io.IOException;
import java.net.Socket;
import java.util.List;

import com.orientechnologies.common.thread.OSoftThread;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.enterprise.channel.OChannel;
import com.orientechnologies.orient.server.OServer;

public abstract class ONetworkProtocol extends OSoftThread {
  protected OServer server;

  public ONetworkProtocol(ThreadGroup group, String name) {
    super(group, name);
  }

  public abstract void config(OServer iServer, Socket iSocket, OContextConfiguration iConfiguration, List<?> statelessCommands,
      List<?> statefulCommands) throws IOException;

  public abstract String getType();

  public abstract int getVersion();

  public abstract OChannel getChannel();

  public String getListeningAddress() {
    final OChannel c = getChannel();
    if (c != null)
      return c.socket.getLocalAddress().getHostAddress();
    return null;
  }

  public OServer getServer() {
    return server;
  }
}
