package com.orientechnologies.agent.cloud;

import com.orientechnologies.agent.OEnterpriseAgent;
import com.orientechnologies.orient.server.distributed.AbstractEnterpriseServerClusterTest;
import com.orientechnologies.orient.server.distributed.ServerRun;
import com.orientechnologies.orientdb.cloud.protocol.Command;
import com.orientechnologies.orientdb.cloud.protocol.CommandResponse;
import com.orientechnologies.orientdb.cloud.protocol.CommandType;
import org.junit.Assert;
import org.junit.Test;

import java.util.UUID;

/**
 * Created by Enrico Risa on 23/01/2018.
 */
public class CloudEndpointDistributedTest extends AbstractEnterpriseServerClusterTest {
  @Override
  protected String getDatabaseName() {
    return "CloudEndpointTest";
  }

  @Test
  public void testStats() throws Exception {

    execute(2, () -> {

      ServerRun firstServer = this.serverInstance.get(0);
      ServerRun secondServer = this.serverInstance.get(1);

      OEnterpriseAgent firstAgent = getAgent(firstServer.getNodeName());
      OEnterpriseAgent secondAgent = getAgent(secondServer.getNodeName());

      secondAgent.getCloudManager().getCommandFactory().removeCommand(CommandType.LIST_SERVERS.command);

      Command command = new Command();
      command.setId(UUID.randomUUID().toString());
      command.setPayload("");
      command.setResponseChannel("fake");
      command.setCmd(CommandType.LIST_SERVERS.command);

      CommandResponse commandResponse = firstAgent.getCloudManager().getCloudEndpoint().processRequest(command);

      Assert.assertTrue(commandResponse.isSuccess());

      commandResponse = secondAgent.getCloudManager().getCloudEndpoint().processRequest(command);

      Assert.assertFalse(commandResponse.isSuccess());
      return null;
    });
  }

  @Override
  protected String getDistributedServerConfiguration(ServerRun server) {
    return "orientdb-distributed-server-config-" + server.getServerId() + ".xml";
  }
}
