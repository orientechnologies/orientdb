package com.orientechnologies.agent.cloud;

import com.orientechnologies.agent.OEnterpriseAgent;
import com.orientechnologies.agent.cloud.processor.ListServersCommandProcessor;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.OServerMain;
import com.orientechnologies.orientdb.cloud.protocol.*;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.InputStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by Enrico Risa on 19/12/2017.
 */
public class ListServerCommandProcessorTest {

  private OServer server;

  private final String DB_NAME = "backupDB";

  private OEnterpriseAgent agent;

  @Before
  public void bootOrientDB() throws Exception {

    InputStream stream = ClassLoader.getSystemResourceAsStream("orientdb-server-config.xml");
    server = OServerMain.create(false);
    server.startup(stream);

    OrientDB orientDB = server.getContext();

    if (orientDB.exists(DB_NAME))
      orientDB.drop(DB_NAME);

    orientDB.create(DB_NAME, ODatabaseType.PLOCAL);

    server.activate();

    agent = server.getPluginByClass(OEnterpriseAgent.class);

  }

  @After
  public void tearDownOrientDB() {

    OrientDB orientDB = server.getContext();
    if (orientDB.exists(DB_NAME))
      orientDB.drop(DB_NAME);

    if (server != null)
      server.shutdown();

    Orient.instance().shutdown();
    Orient.instance().startup();

  }

  @Test
  public void testListServer() {

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

    assertThat(info.getName()).isEqualTo("orientdb");
    assertThat(info.getId()).isEqualTo("orientdb");
    assertThat(info.getDistributed()).isEqualTo(false);
    assertThat(info.getJavaVendor()).isNotNull();
    assertThat(info.getJavaVersion()).isNotNull();
    assertThat(info.getOsArch()).isNotNull();
    assertThat(info.getOsName()).isNotNull();
    assertThat(info.getOsVersion()).isNotNull();
    assertThat(info.getStartedOn()).isNotNull();
    assertThat(info.getStatus()).isEqualTo("ONLINE");
    assertThat(info.getAddresses()).isNotEmpty();
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

  }

}
