package com.orientechnologies.agent.cloud;

import com.orientechnologies.agent.OEnterpriseAgent;
import com.orientechnologies.agent.cloud.processor.backup.ListConnectionsCommandProcessor;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.distributed.AbstractEnterpriseServerClusterTest;
import com.orientechnologies.orient.server.distributed.ServerRun;
import com.orientechnologies.orientdb.cloud.protocol.Command;
import com.orientechnologies.orientdb.cloud.protocol.CommandResponse;
import com.orientechnologies.orientdb.cloud.protocol.ServerInfo;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by Enrico Risa on 19/12/2017.
 */
public class ServerCommandProcessorDistributedTest extends AbstractEnterpriseServerClusterTest {

  private final String DB_NAME     = "backupDB";
  private final String BACKUP_PATH =
      System.getProperty("buildDirectory", "target") + File.separator + "databases" + File.separator + DB_NAME;

  private void deleteBackupConfig(OEnterpriseAgent agent) {
    ODocument configuration = agent.getBackupManager().getConfiguration();

    configuration.<List<ODocument>>field("backups").stream().map(cfg -> cfg.<String>field("uuid")).collect(Collectors.toList())
        .forEach((b) -> agent.getBackupManager().removeAndStopBackup(b));
  }

  @Test
  public void testListConnectionsCommand() throws Exception {

    execute(2, () -> {

      ServerRun firstServer = this.serverInstance.get(0);
      ServerRun secondServer = this.serverInstance.get(1);

      Command command = new Command();
      command.setId("test");
      command.setPayload(new ServerInfo());
      command.setResponseChannel("channelTest");

      ListConnectionsCommandProcessor remove = new ListConnectionsCommandProcessor();

      CommandResponse execute = remove.execute(command, getAgent(secondServer.getNodeName()));

      String result = (String) execute.getPayload();

      Assert.assertNotNull(result);
      return null;
    });

  }

  private OEnterpriseAgent getAgent(String server) {

    return this.serverInstance.stream().filter(serverRun -> serverRun.getNodeName().equals(server)).findFirst()
        .orElseThrow(() -> new IllegalArgumentException(String.format("Cannot find server with name %s", server)))
        .getServerInstance().getPluginByClass(OEnterpriseAgent.class);

  }

  @Override
  protected String getDatabaseName() {
    return DB_NAME;
  }

  @Override
  protected String getDistributedServerConfiguration(ServerRun server) {
    return "orientdb-distributed-server-config-" + server.getServerId() + ".xml";
  }

}
