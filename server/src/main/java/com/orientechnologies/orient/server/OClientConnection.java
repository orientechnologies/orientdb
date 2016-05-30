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

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.exception.OSystemException;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.security.OToken;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinary;
import com.orientechnologies.orient.enterprise.channel.binary.OTokenSecurityException;
import com.orientechnologies.orient.server.config.OServerUserConfiguration;
import com.orientechnologies.orient.server.network.protocol.ONetworkProtocol;
import com.orientechnologies.orient.server.network.protocol.ONetworkProtocolData;
import com.orientechnologies.orient.server.network.protocol.binary.ONetworkProtocolBinary;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class OClientConnection {
  private final int                            id;
  private final long                           since;
  private Set<ONetworkProtocol>                protocols = Collections.newSetFromMap(new WeakHashMap<ONetworkProtocol, Boolean>());
  private volatile ONetworkProtocol            protocol;
  private volatile ODatabaseDocumentInternal   database;
  private volatile OServerUserConfiguration    serverUser;
  private ONetworkProtocolData                 data      = new ONetworkProtocolData();
  private OClientConnectionStats               stats     = new OClientConnectionStats();
  private Lock                                 lock      = new ReentrantLock();
  private Boolean                              tokenBased;
  private byte[]                               tokenBytes;
  private OToken                               token;

  public OClientConnection(final int id, final ONetworkProtocol protocol) throws IOException {
    this.id = id;
    this.protocol = protocol;
    this.protocols.add(protocol);
    this.since = System.currentTimeMillis();
  }

  public void close() {
    if (getDatabase() != null) {
      if (!getDatabase().isClosed()) {
        getDatabase().activateOnCurrentThread();
        try {
          getDatabase().close();
        } catch (Exception e) {
          // IGNORE IT (ALREADY CLOSED?)
        }
      }

      setDatabase(null);
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
    return "OClientConnection [id=" + getId() + ", source="
        + (getProtocol() != null && getProtocol().getChannel() != null && getProtocol().getChannel().socket != null
            ? getProtocol().getChannel().socket.getRemoteSocketAddress() : "?")
        + ", since=" + getSince() + "]";
  }

  /**
   * Returns the remote network address in the format <ip>:<port>.
   */
  public String getRemoteAddress() {
    final Socket socket = getProtocol().getChannel().socket;
    if (socket != null) {
      final InetSocketAddress remoteAddress = (InetSocketAddress) socket.getRemoteSocketAddress();
      return remoteAddress.getAddress().getHostAddress() + ":" + remoteAddress.getPort();
    }
    return null;
  }

  @Override
  public int hashCode() {
    return getId();
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
    if (getId() != other.getId())
      return false;
    return true;
  }

  public OChannelBinary getChannel() {
    return (OChannelBinary) getProtocol().getChannel();
  }

  public ONetworkProtocol getProtocol() {
    return protocol;
  }

  public byte[] getTokenBytes() {
    return tokenBytes;
  }

  public void validateSession(byte[] tokenFromNetwork, OTokenHandler handler, ONetworkProtocolBinary protocol) {
    if (tokenFromNetwork == null || tokenFromNetwork.length == 0) {
      if (!protocols.contains(protocol))
        throw new OTokenSecurityException("No valid session found, provide a token");
    } else {
      // Active This Optimization but it need a periodic check on session
      /*
       * if(tokenBytes != null && tokenBytes.length > 0){ if(tokenBytes.equals(tokenFromNetwork)) //SAME SESSION AND TOKEN DO
       * NOTHING return; }
       */
      OToken token = null;
      try {
        if (tokenFromNetwork != null)
          token = handler.parseBinaryToken(tokenFromNetwork);
      } catch (Exception e) {
        throw OException.wrapException(new OSystemException("Error on token parse"), e);
      }
      if (token == null || !token.getIsVerified()) {
        cleanSession();
        throw new OTokenSecurityException("The token provided is not a valid token, signature does not match");
      }
      if (!handler.validateBinaryToken(token)) {
        cleanSession();
        throw new OTokenSecurityException("The token provided is expired");
      }
      if (tokenBased == null) {
        tokenBased = Boolean.TRUE;
      }
      if (!Arrays.equals(this.tokenBytes, tokenFromNetwork))
        cleanSession();
      this.tokenBytes = tokenFromNetwork;
      this.token = token;
      protocols.add(protocol);
    }
  }

  public void cleanSession() {
    if (database != null && !database.isClosed()) {
      database.activateOnCurrentThread();
      database.close();
    }
    database = null;
    protocols.clear();
  }

  public void endOperation() {
    if (database != null)
      if (!database.isClosed() && database.getLocalCache() != null)
        database.getLocalCache().clear();

    stats.lastCommandExecutionTime = System.currentTimeMillis() - stats.lastCommandReceived;
    stats.totalCommandExecutionTime += stats.lastCommandExecutionTime;

    stats.lastCommandInfo = data.commandInfo;
    stats.lastCommandDetail = data.commandDetail;

    data.commandDetail = "-";

    release();

  }

  public void init(OServer server) {
    if (database == null) {
      setData(server.getTokenHandler().getProtocolDataFromToken(this, token));

      if (data == null)
        throw new OTokenSecurityException("missing in token data");

      final String db = token.getDatabase();
      final String type = token.getDatabaseType();
      if (db != null && type != null) {
        if (data.serverUser) {
          setDatabase((ODatabaseDocumentTx) server.openDatabase(type + ":" + db, token.getUserName(), null, data, true));
        } else
          setDatabase((ODatabaseDocumentTx) server.openDatabase(type + ":" + db, token));
      }
    }
  }

  public Boolean getTokenBased() {
    return tokenBased;
  }

  public void setTokenBased(Boolean tokenBased) {
    this.tokenBased = tokenBased;
  }

  public void setTokenBytes(byte[] tokenBytes) {
    this.tokenBytes = tokenBytes;
  }

  public OToken getToken() {
    return token;
  }

  public void setToken(OToken token) {
    this.token = token;
  }

  public int getId() {
    return id;
  }

  public long getSince() {
    return since;
  }

  public void setProtocol(ONetworkProtocol protocol) {
    this.protocol = protocol;
  }

  public ODatabaseDocumentInternal getDatabase() {
    return database;
  }

  public void setDatabase(ODatabaseDocumentInternal database) {
    this.database = database;
  }

  public OServerUserConfiguration getServerUser() {
    return serverUser;
  }

  public void setServerUser(OServerUserConfiguration serverUser) {
    this.serverUser = serverUser;
  }

  public ONetworkProtocolData getData() {
    return data;
  }

  public void setData(ONetworkProtocolData data) {
    this.data = data;
  }

  public OClientConnectionStats getStats() {
    return stats;
  }

  public void statsUpdate() {

    if (database != null) {
      database.activateOnCurrentThread();
      stats.lastDatabase = database.getName();
      stats.lastUser = database.getUser() != null ? database.getUser().getName() : null;
    } else {
      stats.lastDatabase = null;
      stats.lastUser = null;
    }

    ++stats.totalRequests;
    data.commandInfo = "Listening";
    data.commandDetail = "-";
    stats.lastCommandReceived = System.currentTimeMillis();
  }
}
