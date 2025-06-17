package com.orientechnologies.orient.server.distributed.http;

import com.orientechnologies.agent.http.command.OServerCommandDistributedManager;
import com.orientechnologies.orient.server.distributed.AbstractServerClusterTest;
import com.orientechnologies.orient.server.distributed.ODistributedConfiguration;
import com.orientechnologies.orient.server.distributed.ServerRun;
import com.orientechnologies.orient.server.distributed.config.OClusterConfiguration;
import com.orientechnologies.orient.server.network.OServerNetworkListener;
import com.orientechnologies.orient.server.network.protocol.http.ONetworkProtocolHttpAbstract;
import org.junit.Assert;
import org.junit.Test;

/** Created by Enrico Risa on 04/08/16. */
public class HttpEEDistributedIT extends AbstractServerClusterTest {

  @Override
  protected String getDatabaseName() {
    return "httpTest";
  }

  @Test
  public void testChangeConfig() throws Exception {

    init(2);
    prepare(true);
    execute();
  }

  @Override
  protected void executeTest() throws Exception {
    ServerRun s = serverInstance.iterator().next();

    OServerNetworkListener listener =
        s.getServerInstance().getListenerByProtocol(ONetworkProtocolHttpAbstract.class);

    OServerCommandDistributedManager distributedManager =
        (OServerCommandDistributedManager)
            listener.getCommand(OServerCommandDistributedManager.class);

    OServerCommandDistributedManager first = distributedManager;

    ODistributedConfiguration dbConfig =
        distributedManager.doGetDatabaseInfo(s.getServerInstance(), getDatabaseName());

    dbConfig.getDocument().field("writeQuorum", "all");

    distributedManager.changeConfig(
        s.getServerInstance(), getDatabaseName(), dbConfig.getDocument().toJSON());

    for (ServerRun serverRun : serverInstance) {

      listener =
          serverRun.getServerInstance().getListenerByProtocol(ONetworkProtocolHttpAbstract.class);

      distributedManager =
          (OServerCommandDistributedManager)
              listener.getCommand(OServerCommandDistributedManager.class);

      dbConfig = distributedManager.doGetDatabaseInfo(s.getServerInstance(), getDatabaseName());
      Assert.assertEquals("all", dbConfig.getDocument().field("writeQuorum"));
    }

    first.configure(s.getServerInstance());

    OClusterConfiguration config =
        first.doGetNodeConfig(s.getServerInstance().getDistributedManager());

    Assert.assertNotNull(config);

    config = first.doGetNodeConfig(null);

    Assert.assertNotNull(config);
  }

  @Override
  protected String getDistributedServerConfiguration(ServerRun server) {
    return "dc-server-config-" + server.getServerId() + ".xml";
  }
}
