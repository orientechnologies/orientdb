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
package com.orientechnologies.orient.client.remote;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.client.binary.OChannelBinaryAsynchClient;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Manages network connections against OrientDB servers. All the connection pools are managed in a Map<url,pool>, but in the future
 * we could have a unique pool per sever and manage database connections over the protocol.
 *
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 */
public class ORemoteConnectionManager {
  public static final String PARAM_MAX_POOL = "maxpool";

  protected final ConcurrentMap<String, ORemoteConnectionPool> connections;
  protected final long                                         timeout;

  public ORemoteConnectionManager(final long iTimeout) {
    connections = new ConcurrentHashMap<String, ORemoteConnectionPool>();
    timeout = iTimeout;
  }

  public void close() {
    for (Map.Entry<String, ORemoteConnectionPool> entry : connections.entrySet()) {
      closePool(entry.getValue());
    }

    connections.clear();
  }

  public OChannelBinaryAsynchClient acquire(String iServerURL, final OContextConfiguration clientConfiguration,
      final Map<String, Object> iConfiguration, final OStorageRemoteAsynchEventListener iListener) {
    if (iServerURL.startsWith(OEngineRemote.PREFIX))
      iServerURL = iServerURL.substring(OEngineRemote.PREFIX.length());

    if (iServerURL.endsWith("/"))
      iServerURL = iServerURL.substring(0, iServerURL.length() - 1);

    long localTimeout = timeout;

    ORemoteConnectionPool pool = connections.get(iServerURL);
    if (pool == null) {
      int maxPool = OGlobalConfiguration.CLIENT_CHANNEL_MAX_POOL.getValueAsInteger();

      if (iConfiguration != null && iConfiguration.size() > 0) {
        if (iConfiguration.containsKey(PARAM_MAX_POOL))
          maxPool = Integer.parseInt(iConfiguration.get(PARAM_MAX_POOL).toString());
        if (iConfiguration.containsKey(PARAM_MAX_POOL))
          maxPool = Integer.parseInt(iConfiguration.get(PARAM_MAX_POOL).toString());
      }

      if (clientConfiguration != null) {
        final Object max = clientConfiguration.getValue(OGlobalConfiguration.CLIENT_CHANNEL_MAX_POOL);
        if (max != null)
          maxPool = Integer.parseInt(max.toString());

        final Object netLockTimeout = clientConfiguration.getValue(OGlobalConfiguration.NETWORK_LOCK_TIMEOUT);
        if (netLockTimeout != null)
          localTimeout = Integer.parseInt(netLockTimeout.toString());
      }

      pool = new ORemoteConnectionPool(maxPool, iListener != null);
      final ORemoteConnectionPool prev = connections.putIfAbsent(iServerURL, pool);
      if (prev != null) {
        // ALREADY PRESENT, DESTROY IT AND GET THE ALREADY EXISTENT OBJ
        pool.getPool().close();
        pool = prev;
      }
    }

    try {
      // RETURN THE RESOURCE
      OChannelBinaryAsynchClient ret = pool.acquire(iServerURL, localTimeout, clientConfiguration, iConfiguration, iListener);
      return ret;

    } catch (RuntimeException e) {
      // ERROR ON RETRIEVING THE INSTANCE FROM THE POOL
      throw e;
    } catch (Exception e) {
      // ERROR ON RETRIEVING THE INSTANCE FROM THE POOL
      OLogManager.instance().debug(this, "Error on retrieving the connection from pool: " + iServerURL, e);
    }
    return null;
  }

  public void release(final OChannelBinaryAsynchClient conn) {
    if (conn == null)
      return;

    final ORemoteConnectionPool pool = connections.get(conn.getServerURL());
    if (pool != null) {
      if (!conn.isConnected()) {
        OLogManager.instance().debug(this, "Network connection pool is receiving a closed connection to reuse: discard it");
        remove(conn);
      } else {
        pool.getPool().returnResource(conn);
      }
    }
  }

  public void remove(final OChannelBinaryAsynchClient conn) {
    if (conn == null)
      return;

    final ORemoteConnectionPool pool = connections.get(conn.getServerURL());
    if (pool == null) {
      OLogManager.instance()
          .debug(this, "Connection '%s' cannot be released because the pool doesn't exist anymore", conn.getServerURL());
      return;
    }

    pool.getPool().remove(conn);

    try {
      conn.unlock();
    } catch (Exception e) {
      OLogManager.instance().debug(this, "Cannot unlock connection lock", e);
    }

    try {
      conn.close();
    } catch (Exception e) {
      OLogManager.instance().debug(this, "Cannot close connection", e);
    }

  }

  public Set<String> getURLs() {
    return connections.keySet();
  }

  public int getMaxResources(final String url) {
    final ORemoteConnectionPool pool = connections.get(url);
    if (pool == null)
      return 0;

    return pool.getPool().getMaxResources();
  }

  public int getAvailableConnections(final String url) {
    final ORemoteConnectionPool pool = connections.get(url);
    if (pool == null)
      return 0;

    return pool.getPool().getAvailableResources();
  }

  public int getReusableConnections(final String url) {
    final ORemoteConnectionPool pool = connections.get(url);
    if (pool == null)
      return 0;

    return pool.getPool().getInPoolResources();
  }

  public int getCreatedInstancesInPool(final String url) {
    final ORemoteConnectionPool pool = connections.get(url);
    if (pool == null)
      return 0;

    return pool.getPool().getCreatedInstances();
  }

  public void closePool(final String url) {
    final ORemoteConnectionPool pool = connections.remove(url);
    if (pool == null)
      return;

    closePool(pool);
  }

  protected void closePool(ORemoteConnectionPool pool) {
    final List<OChannelBinaryAsynchClient> conns = new ArrayList<OChannelBinaryAsynchClient>(pool.getPool().getAllResources());
    for (OChannelBinaryAsynchClient c : conns)
      try {
        // Unregister the listener that make the connection return to the closing pool.
        c.close();
      } catch (Exception e) {
        OLogManager.instance().debug(this, "Cannot close binary channel", e);
      }
    pool.getPool().close();
  }

  public ORemoteConnectionPool getPool(String url) {
    return connections.get(url);
  }

}
