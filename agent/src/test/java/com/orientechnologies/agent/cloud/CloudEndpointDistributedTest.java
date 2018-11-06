package com.orientechnologies.agent.cloud;

import com.orientechnologies.agent.OEnterpriseAgent;
import com.orientechnologies.orient.server.distributed.AbstractEnterpriseServerClusterTest;
import com.orientechnologies.orient.server.distributed.ServerRun;
import com.orientechnologies.orientdb.cloud.protocol.*;
import org.junit.Assert;
import org.junit.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

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

  @Test
  public void testServerConnections() throws Exception {

    execute(2, () -> {

      ServerRun firstServer = this.serverInstance.get(0);
      ServerRun secondServer = this.serverInstance.get(1);

      OEnterpriseAgent firstAgent = getAgent(firstServer.getNodeName());
      OEnterpriseAgent secondAgent = getAgent(secondServer.getNodeName());

      secondAgent.getCloudManager().getCommandFactory().removeCommand(CommandType.SERVER_CONNECTIONS.command);

      ServerInfo info = new ServerInfo();

      info.setName(firstServer.getNodeName());

      Command command = new Command();
      command.setId(UUID.randomUUID().toString());
      command.setPayload(info);
      command.setResponseChannel("fake");
      command.setCmd(CommandType.SERVER_CONNECTIONS.command);

      CommandResponse commandResponse = firstAgent.getCloudManager().getCloudEndpoint().processRequest(command);

      Assert.assertTrue(commandResponse.isSuccess());

      assertThat(commandResponse.getPayload()).isInstanceOf(ServerConnections.class);

      ServerConnections connections = (ServerConnections) commandResponse.getPayload();

      assertThat(connections.getConnections()).isNotEmpty();

      connections.getConnections().forEach((c) -> {
        assertThat(c.getConnectionId()).isNotNull();
        assertThat(c.getSince()).isNotNull();
//        assertThat(c.getCommandDetail()).isNotNull();
        assertThat(c.getCommandInfo()).isNotNull();
        assertThat(c.getProtocolType()).isNotNull();
        assertThat(c.getProtocolVersion()).isNotNull();
//        assertThat(c.getLastCommandDetail()).isNotNull();
        assertThat(c.getLastCommandInfo()).isNotNull();
        assertThat(c.getLastCommandOn()).isNotNull();
        assertThat(c.getRemoteAddress()).isNotNull();
        assertThat(c.getSessionId()).isNotNull();

      });

      commandResponse = secondAgent.getCloudManager().getCloudEndpoint().processRequest(command);

      Assert.assertFalse(commandResponse.isSuccess());
      return null;
    });
  }

  @Test
  public void testServerThreadDump() throws Exception {

    execute(2, () -> {

      ServerRun firstServer = this.serverInstance.get(0);
      ServerRun secondServer = this.serverInstance.get(1);

      OEnterpriseAgent firstAgent = getAgent(firstServer.getNodeName());
      OEnterpriseAgent secondAgent = getAgent(secondServer.getNodeName());

      secondAgent.getCloudManager().getCommandFactory().removeCommand(CommandType.SERVER_THREAD_DUMP.command);

      ServerInfo info = new ServerInfo();

      info.setName(firstServer.getNodeName());

      Command command = new Command();
      command.setId(UUID.randomUUID().toString());
      command.setPayload(info);
      command.setResponseChannel("fake");
      command.setCmd(CommandType.SERVER_THREAD_DUMP.command);

      CommandResponse commandResponse = firstAgent.getCloudManager().getCloudEndpoint().processRequest(command);

      Assert.assertTrue(commandResponse.isSuccess());

      assertThat(commandResponse.getPayload()).isInstanceOf(ServerThreadDump.class);

      ServerThreadDump connections = (ServerThreadDump) commandResponse.getPayload();

      assertThat(connections.getThreadDump()).isNotEmpty();

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
