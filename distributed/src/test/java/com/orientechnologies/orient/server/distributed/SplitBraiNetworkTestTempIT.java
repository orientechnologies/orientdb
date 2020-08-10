/*
 * Copyright 2010-2013 Luca Garulli (l.garulli--at--orientechnologies.com)
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
package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.ODatabaseLifecycleListenerAbstract;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.distributed.impl.proxy.OProxyServer;
import com.orientechnologies.orient.setup.ServerRun;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Distributed test with 3 servers running and after a while the server 2 is isolated from the
 * network (using a proxy) and then it re-merges the cluster again.
 */
public class SplitBraiNetworkTestTempIT extends AbstractHARemoveNode {
  static final int SERVERS = 3;

  private OProxyServer hzProxy;
  private OProxyServer server1And2Proxy;
  private OProxyServer server3Proxy;

  @Test
  @Ignore
  public void test() throws Exception {
    useTransactions = false;
    count = 10;
    startupNodesInSequence = true;
    init(SERVERS);
    prepare(false);
    startProxies();
    execute();
  }

  @Override
  protected void onAfterExecution() throws Exception {
    try {
      banner("SIMULATE ISOLATION OF SERVER " + (SERVERS - 1) + "...");

      Assert.assertNotNull(hzProxy);
      hzProxy.shutdown();
      Assert.assertNotNull(server3Proxy);
      server3Proxy.shutdown();

      banner(
          "SERVER "
              + (SERVERS - 1)
              + " HAS BEEN ISOLATED, WAITING FOR THE DATABASE ON SERVER 2 TO BE OFFLINE...");

      waitForDatabaseStatus(
          0,
          "europe-2",
          getDatabaseName(),
          ODistributedServerManager.DB_STATUS.NOT_AVAILABLE,
          90000);
      assertDatabaseStatusEquals(
          0, "europe-2", getDatabaseName(), ODistributedServerManager.DB_STATUS.NOT_AVAILABLE);

      banner("RUN TEST WITHOUT THE OFFLINE SERVER " + (SERVERS - 1) + "...");

      count = 10;

      executeMultipleTest();

      banner("TEST WITH THE ISOLATED NODE FINISHED, REJOIN THE SERVER " + (SERVERS - 1) + "...");

      server3Proxy.startup();
      hzProxy.startup();

      waitForDatabaseIsOnline(0, "europe-2", getDatabaseName(), 90000);
      assertDatabaseStatusEquals(
          0, "europe-2", getDatabaseName(), ODistributedServerManager.DB_STATUS.ONLINE);

      banner("NETWORK FOR THE ISOLATED NODE " + (SERVERS - 1) + " HAS BEEN RESTORED");

      banner("RESTARTING TESTS WITH SERVER " + (SERVERS - 1) + " CONNECTED...");

      count = 10;

      executeMultipleTest();

    } finally {
      if (server1And2Proxy != null) server1And2Proxy.shutdown();
      if (server3Proxy != null) server3Proxy.shutdown();
      if (hzProxy != null) hzProxy.shutdown();
    }
  }

  protected String getDatabaseURL(final ServerRun server) {
    return "plocal:" + server.getDatabasePath(getDatabaseName());
  }

  @Override
  protected String getDistributedServerConfiguration(final ServerRun server) {
    return "proxied-orientdb-dserver-config-" + server.getServerId() + ".xml";
  }

  @Override
  public String getDatabaseName() {
    return "distributed-split";
  }

  protected void startProxies() {
    hzProxy =
        new OProxyServer() {
          @Override
          protected void onMessage(
              final boolean request,
              final int fromPort,
              final int toPort,
              final byte[] buffer,
              final int size) {
            // PATCH HAZELCAST PROTOCOL BY REWRITING THE PORT TO FORCE USING THE PROXY
            // EXAMPLE ...,0,0,9,-126,4,0,0,0,9,49,50,55,46,48,46,48,46,49,...
            // PORT(INT), STATUS(1), IP_SIZE(INT), IP(BYTE[]=STRING)
            int state = 0;
            int start = -1;

            for (int i = 0; i < size; ++i) {
              final byte b = buffer[i];

              switch (state) {
                case 0:
                  if (i + 15 > size) return;

                  if (b == 0) {
                    start = i;
                    state++;
                  }
                  break;
                case 1:
                  if (b == 0) {
                    final byte[] portAsBytes = new byte[] {0, 0, buffer[++i], buffer[++i]};
                    final int port = OIntegerSerializer.INSTANCE.deserialize(portAsBytes, 0);
                    if (port >= 2434 && port < 2440) {
                      i++;
                      final byte[] ipSizeAsBytes =
                          new byte[] {buffer[++i], buffer[++i], buffer[++i], buffer[++i]};
                      final int ipSize = OIntegerSerializer.INSTANCE.deserialize(ipSizeAsBytes, 0);
                      if (ipSize > 0 && ipSize <= 15) {
                        // CHECK IT'S AN IP ADDRESS
                        final byte[] ipAsBytes = new byte[ipSize];
                        System.arraycopy(buffer, ++i, ipAsBytes, 0, ipSize);
                        final String ip = new String(ipAsBytes);
                        final String[] parts = ip.split("\\.");
                        if (parts.length == 4) {
                          boolean invalid = false;
                          for (String p : parts) {
                            int v = Integer.parseInt(p);
                            if (v < 0 || v > 255) {
                              invalid = true;
                              break;
                            }
                          }
                          if (!invalid) {
                            // OK
                            OIntegerSerializer.INSTANCE.serialize(port + 1000, buffer, start);
                            OLogManager.instance()
                                .info(
                                    this,
                                    "ProxyChannel: patching port %d to %d",
                                    port,
                                    port + 1000);
                          }
                        }
                      }
                    }
                    state = 0;
                  } else state = 0;
                  break;
              }
            }
          }
        };

    hzProxy.setPorts("3434->2434,3435->2435,3436->2436");
    hzProxy.setTracing("byte");
    hzProxy.setWaitUntilRemotePortsAreOpen(true);
    hzProxy.startup();

    // PROXIES FOR ORIENTDB BIN PROTOCOL FOR NODE 1 AND 2
    server1And2Proxy = new OProxyServer();
    server1And2Proxy.setPorts("3424->2424,3425->2425");
    // server1And2Proxy.setTracing("byte");
    server1And2Proxy.setWaitUntilRemotePortsAreOpen(true);
    server1And2Proxy.startup();

    // PROXY FOR NODE 3
    server3Proxy = new OProxyServer();
    server3Proxy.setPorts("3426->2426");
    server3Proxy.setWaitUntilRemotePortsAreOpen(true);
    server3Proxy.startup();

    // REWRITE THE ORIENTDB PORTS TO USE THE PROXIES
    Orient.instance()
        .addDbLifecycleListener(
            new ODatabaseLifecycleListenerAbstract() {
              @Override
              public void onLocalNodeConfigurationRequest(ODocument iConfiguration) {
                List<Map<String, Object>> listeners = iConfiguration.field("listeners");
                for (Map<String, Object> map : listeners) {
                  if (map.get("protocol").toString().equalsIgnoreCase("ONetworkProtocolBinary")) {
                    final String listen = map.get("listen").toString();
                    final String[] parts = listen.split(":");
                    final int port = Integer.parseInt(parts[1]);
                    if (port >= 2424 && port <= 2430)
                      // PATCH THE PORT
                      map.put("listen", parts[0] + ":" + (port + 1000));
                  }
                }
              }
            });
  }
}
