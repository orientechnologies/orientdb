package com.orientechnologies.agent.cloud;

import com.orientechnologies.agent.OEnterpriseAgent;
import com.orientechnologies.agent.cloud.processor.ListServersCommandProcessor;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.AbstractServerClusterTest;
import com.orientechnologies.orient.server.distributed.ServerRun;
import com.orientechnologies.orientdb.cloud.protocol.*;
import org.junit.Assert;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by Enrico Risa on 19/12/2017.
 */
public class ListDistributedServerCommandProcessorTest extends AbstractServerClusterTest {

  private final String DB_NAME = "backupDB";

  private OEnterpriseAgent agent;

  @Test
  public void testListServer() throws Exception {
    init(1);
    prepare(false);
    execute();
  }

  @Override
  protected String getDatabaseName() {
    return DB_NAME;
  }

  @Override
  protected void executeTest() throws Exception {

    OServer server = this.serverInstance.get(0).getServerInstance();
    agent = server.getPluginByClass(OEnterpriseAgent.class);

    ListServersCommandProcessor backupCommandProcessor = new ListServersCommandProcessor();

    Command command = new Command();
    command.setId("test");
    command.setPayload("");
    command.setResponseChannel("channelTest");

    CommandResponse execute = backupCommandProcessor.execute(command, agent);

    Assert.assertTrue(execute.getPayload() instanceof ServerList);

    ServerList serverList = (ServerList) execute.getPayload();

    assertThat(serverList.getInfo().size()).isEqualTo(1);

    ServerBasicInfo info = serverList.getInfo().get(0);

    assertThat(info.getName()).isEqualTo("europe-0");
    assertThat(info.getId()).isEqualTo("europe-0");
    assertThat(info.getDistributed()).isEqualTo(true);
    assertThat(info.getJavaVendor()).isNotNull();
    assertThat(info.getJavaVersion()).isNotNull();
    assertThat(info.getOsArch()).isNotNull();
    assertThat(info.getOsName()).isNotNull();
    assertThat(info.getOsVersion()).isNotNull();
    assertThat(info.getStartedOn()).isNotNull();
    assertThat(info.getStatus()).isEqualTo("ONLINE");
    assertThat(info.getStats()).isNotNull();

    ServerStats stats = info.getStats();

    assertThat(stats.getActiveConnections()).isNotNull();
    assertThat(stats.getCpuUsage()).isNotNull();
    assertThat(stats.getScanOps()).isNotNull();
    assertThat(stats.getCreateOps()).isNotNull();
    assertThat(stats.getDeleteOps()).isNotNull();
    assertThat(stats.getReadOps()).isNotNull();
    assertThat(stats.getUpdateOps()).isNotNull();
    assertThat(stats.getTxCommitOps()).isNotNull();
    assertThat(stats.getTxRollbackOps()).isNotNull();
    assertThat(stats.getConflictOps()).isNotNull();
    assertThat(stats.getDistributedTxRetriesOps()).isNotNull();
    assertThat(stats.getDiskSize()).isNotNull();
    assertThat(stats.getDiskUsed()).isNotNull();
    assertThat(stats.getTotalDiskCache()).isNotNull();
    assertThat(stats.getUsedDiskCache()).isNotNull();
    assertThat(stats.getTotalHeapMemory()).isNotNull();
    assertThat(stats.getTotalHeapMemory()).isNotNull();
    assertThat(stats.getNumberOfCPUs()).isNotNull();
    assertThat(stats.getNetworkRequests()).isNotNull();

    assertThat(stats.getMessages()).isNotNull();
  }

  @Override
  protected String getDistributedServerConfiguration(ServerRun server) {
    return "orientdb-distributed-server-config-" + server.getServerId() + ".xml";
  }
}
