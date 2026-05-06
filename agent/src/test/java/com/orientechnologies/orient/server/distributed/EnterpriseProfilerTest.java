package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.agent.http.command.OServerCommandDistributedManager;
import com.orientechnologies.orient.distributed.db.OrientDBDistributed;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.config.OClusterConfiguration;
import com.orientechnologies.orient.server.network.OServerNetworkListener;
import com.orientechnologies.orient.server.network.protocol.http.ONetworkProtocolHttpAbstract;
import java.util.Set;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Tests Enterprise Profiler Stats
 *
 * @author Enrico Risa
 */
public class EnterpriseProfilerTest extends AbstractServerClusterTest {
  private static final int SERVERS = 3;

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

    String name = serverRun.getServerInstance().getNodeId().getNode();

    serverRun.crashServer();

    serverRun.deleteNode();
    serverInstance.remove(serverRun);

    // wait for the first stats push from each node
    //    waitAllServersForPush();

    waitForDatabaseIsOffline(name, getDatabaseName(), 2000);

    for (ServerRun sr : serverInstance) {

      // check again stats on single server. The removed server {{name}} should not be in the config

      checkStatsOnServer(sr, new String[] {name});
    }
  }

  private void checkStatsOnServer(ServerRun s) {
    checkStatsOnServer(s, new String[] {});
  }

  private void checkStatsOnServer(ServerRun s, String[] nodes) {

    OServer server = s.getServerInstance();
    OrientDBDistributed context = (OrientDBDistributed) server.getDatabases();
    ODistributedServerManager dm = server.getDistributedManager();

    Set<String> availableNodeNames = context.getAvailableNodeNames(getDatabaseName());

    final OServerNetworkListener listener =
        server.getListenerByProtocol(ONetworkProtocolHttpAbstract.class);

    OServerCommandDistributedManager command =
        (OServerCommandDistributedManager)
            listener.getCommand(OServerCommandDistributedManager.class);

    OClusterConfiguration clusterStats = command.getClusterConfig(dm);

    Assert.assertNotNull(clusterStats);

    for (String nodeName : availableNodeNames) {
      Assert.assertNotNull(
          String.format("Stats for server [%s] should't miss", nodeName),
          clusterStats.getDocument().eval("clusterStats." + nodeName));
    }

    for (String node : nodes) {
      Assert.assertNull(
          String.format("Stats for server [%s] should't be in stats", node),
          clusterStats.getDocument().eval("clusterStats." + node));
    }
  }

  @Test
  @Ignore
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
