package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.agent.OEnterpriseAgent;
import com.orientechnologies.agent.OEnterpriseAgentMock;
import com.orientechnologies.agent.http.command.OServerCommandDistributedManager;
import com.orientechnologies.agent.profiler.OEnterpriseProfilerListener;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.network.OServerNetworkListener;
import com.orientechnologies.orient.server.network.protocol.http.ONetworkProtocolHttpAbstract;
import org.junit.Assert;
import org.junit.Test;

import java.util.Set;
import java.util.concurrent.CountDownLatch;

/**
 * Tests Enterprise Profiler Stats
 *
 * @author Enrico Risa
 */
public class EnterpriseProfilerTest extends AbstractServerClusterTest {
  private final static int SERVERS = 3;

  @Override
  public String getDatabaseName() {
    return "EnterpriseProfilerTest";
  }

  @Override
  protected void executeTest() throws Exception {

    // wait for the first stats push from each node
//    waitAllServersForPush();
    // check stats for each nodes
    for (ServerRun serverRun : serverInstance) {
      checkStatsOnServer(serverRun);
    }

    // kill first node
    ServerRun serverRun = serverInstance.get(0);

    String name = serverRun.getServerInstance().getDistributedManager().getLocalNodeName();

    serverRun.crashServer();

    serverRun.deleteNode();
    serverInstance.remove(serverRun);

    // wait for the first stats push from each node
//    waitAllServersForPush();

    waitForDatabaseIsOffline(name, getDatabaseName(), 2000);

    for (ServerRun sr : serverInstance) {

      // check again stats on single server. The removed server {{name}} should not be in the config

      checkStatsOnServer(sr, new String[] { name });
    }

  }

  private void waitAllServersForPush() throws InterruptedException {

    final CountDownLatch latch = new CountDownLatch(serverInstance.size());

    for (ServerRun serverRun : serverInstance) {

      final OEnterpriseAgent agent = serverRun.getServerInstance().getPluginByClass(OEnterpriseAgentMock.class);

      OEnterpriseProfilerListener listener = new OEnterpriseProfilerListener() {
        @Override
        public void onStatsPublished(String statsAsJson) {
          latch.countDown();
          agent.unregisterListener(this);
        }
      };
      agent.registerListener(listener);
    }
    latch.await();
  }

  private void checkStatsOnServer(ServerRun s) {
    checkStatsOnServer(s, new String[] {});
  }

  private void checkStatsOnServer(ServerRun s, String[] nodes) {

    OServer server = s.getServerInstance();
    ODistributedServerManager dm = server.getDistributedManager();

    Set<String> availableNodeNames = dm.getAvailableNodeNames(getDatabaseName());

    final OServerNetworkListener listener = server.getListenerByProtocol(ONetworkProtocolHttpAbstract.class);

    OServerCommandDistributedManager command = (OServerCommandDistributedManager) listener
        .getCommand(OServerCommandDistributedManager.class);

    ODocument clusterStats = command.getClusterConfig(dm);

    Assert.assertNotNull(clusterStats);

    for (String nodeName : availableNodeNames) {
      Assert.assertNotNull(String.format("Stats for server [%s] should't miss", nodeName),
          clusterStats.eval("clusterStats." + nodeName));
    }

    for (String node : nodes) {
      Assert
          .assertNull(String.format("Stats for server [%s] should't be in stats", node), clusterStats.eval("clusterStats." + node));
    }

  }

  @Test
  public void test() throws Exception {
    init(SERVERS);
    prepare(false);
    execute();
  }

  @Override
  protected String getDistributedServerConfiguration(ServerRun server) {
    return "dc-server-config-" + server.getServerId() + ".xml";
  }
}
