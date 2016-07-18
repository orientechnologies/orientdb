/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
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
 *  * For more information: http://www.orientechnologies.com
 *
 */
package com.orientechnologies.orient.server;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinary;
import com.orientechnologies.orient.server.config.OServerUserConfiguration;
import com.orientechnologies.orient.server.network.protocol.ONetworkProtocol;
import com.orientechnologies.orient.server.network.protocol.ONetworkProtocolData;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class OClientConnection {
  public final    int                      id;
  public final    long                     since;
  public volatile ONetworkProtocol         protocol;
  public volatile ODatabaseDocumentTx      database;
  public volatile OServerUserConfiguration serverUser;
  public Boolean                        tokenBased;
  private Lock lock = new ReentrantLock();

  public ONetworkProtocolData data = new ONetworkProtocolData();

  public OClientConnection(final int id, final ONetworkProtocol protocol) throws IOException {
    this.id = id;
    this.protocol = protocol;
    this.since = System.currentTimeMillis();
  }

  public void close() {
    if (database != null) {
      if (!database.isClosed()) {
        database.activateOnCurrentThread();
        database.close();
      }

      database = null;
    }
  }

  /**
   * Acquires the connection. This is fundamental to manage concurrent requests using the same session id.
   */
  public void acquire() {
    lock.lock();
  }

  /**
   * Releases an acquired connection.
   */
  public void release() {
    lock.unlock();
  }

  @Override
  public String toString() {
    return "OClientConnection [id=" + id + ", source=" + (protocol != null && protocol.getChannel() != null && protocol.getChannel().socket != null ? protocol.getChannel().socket.getRemoteSocketAddress() : "?") + ", since=" + since + "]";
  }

  /**
   * Returns the remote network address in the format <ip>:<port>.
   */
  public String getRemoteAddress() {
    final Socket socket = protocol.getChannel().socket;
    if (socket != null) {
      final InetSocketAddress remoteAddress = (InetSocketAddress) socket.getRemoteSocketAddress();
      return remoteAddress.getAddress().getHostAddress() + ":" + remoteAddress.getPort();
    }
    return null;
  }

  @Override
  public int hashCode() {
    return id;
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    final OClientConnection other = (OClientConnection) obj;
    if (id != other.id)
      return false;
    return true;
  }

  public OChannelBinary getChannel() {
    return (OChannelBinary) protocol.getChannel();
  }

  public ONetworkProtocol getProtocol() {
    return protocol;
  }
}
