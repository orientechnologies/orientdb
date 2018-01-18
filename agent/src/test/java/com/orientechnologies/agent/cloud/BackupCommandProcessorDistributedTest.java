package com.orientechnologies.agent.cloud;

import com.orientechnologies.agent.OEnterpriseAgent;
import com.orientechnologies.agent.backup.OBackupTask;
import com.orientechnologies.agent.backup.log.OBackupLog;
import com.orientechnologies.agent.backup.log.OBackupLogType;
import com.orientechnologies.agent.cloud.processor.backup.AddBackupCommandProcessor;
import com.orientechnologies.agent.cloud.processor.backup.ListBackupCommandProcessor;
import com.orientechnologies.agent.cloud.processor.backup.ListBackupLogsCommandProcessor;
import com.orientechnologies.agent.cloud.processor.backup.RemoveBackupCommandProcessor;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.distributed.AbstractEnterpriseServerClusterTest;
import com.orientechnologies.orient.server.distributed.ServerRun;
import com.orientechnologies.orientdb.cloud.protocol.Command;
import com.orientechnologies.orientdb.cloud.protocol.CommandResponse;
import com.orientechnologies.orientdb.cloud.protocol.backup.*;
import com.orientechnologies.orientdb.cloud.protocol.backup.log.BackupLogsList;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by Enrico Risa on 19/12/2017.
 */
public class BackupCommandProcessorDistributedTest extends AbstractEnterpriseServerClusterTest {

  private final String DB_NAME     = "backupDB";
  private final String BACKUP_PATH =
      System.getProperty("buildDirectory", "target") + File.separator + "databases" + File.separator + DB_NAME;

  private void deleteBackupConfig(OEnterpriseAgent agent) {
    ODocument configuration = agent.getBackupManager().getConfiguration();

    configuration.<List<ODocument>>field("backups").stream().map(cfg -> cfg.<String>field("uuid")).collect(Collectors.toList())
        .forEach((b) -> agent.getBackupManager().removeAndStopBackup(b));
  }

  @Test
  public void testBackupCommandProcessorEmptyBackups() throws Exception {

    execute(2, () -> {

      ServerRun serverRun = this.serverInstance.get(0);

      BackupList payload = getBackupList(serverRun.getNodeName());

      assertThat(payload.getBackups()).hasSize(0);

      return null;
    });

  }

  @Test
  public void testListBackupCommandProcessor() throws Exception {

    execute(2, () -> {

      ServerRun firstServer = this.serverInstance.get(0);
      ServerRun secondServer = this.serverInstance.get(1);

      deleteBackupConfig(getAgent(firstServer.getNodeName()));
      deleteBackupConfig(getAgent(secondServer.getNodeName()));

      ODocument cfg = createBackupConfig(getAgent(firstServer.getNodeName()));

      String uuid = cfg.field("uuid");

      BackupList payload = getBackupList(secondServer.getNodeName());

      assertThat(payload.getBackups()).hasSize(1);

      BackupInfo backupInfo = payload.getBackups().get(0);

      assertThat(backupInfo.getUuid()).isEqualTo(uuid);

      assertThat(backupInfo.getDbName()).isEqualTo(DB_NAME);

      assertThat(backupInfo.getDirectory()).isEqualTo(BACKUP_PATH);

      assertThat(backupInfo.getEnabled()).isEqualTo(true);

      assertThat(backupInfo.getRetentionDays()).isEqualTo(30);

      assertThat(backupInfo.getModes()).containsKeys(BackupMode.FULL_BACKUP);
      assertThat(backupInfo.getModes()).containsKeys(BackupMode.INCREMENTAL_BACKUP);

      BackupModeConfig incremental = backupInfo.getModes().get(BackupMode.INCREMENTAL_BACKUP);

      BackupModeConfig full = backupInfo.getModes().get(BackupMode.FULL_BACKUP);

      assertThat(full.getWhen()).isEqualTo("0/5 * * * * ?");
      assertThat(incremental.getWhen()).isEqualTo("0/2 * * * * ?");

      // Add second config the the second node
      createBackupConfig(getAgent(secondServer.getNodeName()));

      payload = getBackupList(firstServer.getNodeName());

      assertThat(payload.getBackups()).hasSize(2);

      return null;
    });

  }

  private ODocument createBackupConfig(OEnterpriseAgent agent) {
    ODocument modes = new ODocument();

    ODocument mode = new ODocument();
    modes.field("FULL_BACKUP", mode);
    mode.field("when", "0/5 * * * * ?");

    ODocument incrementalMode = new ODocument();
    modes.field("INCREMENTAL_BACKUP", incrementalMode);
    incrementalMode.field("when", "0/2 * * * * ?");

    ODocument backup = new ODocument();
    backup.field("dbName", DB_NAME);
    backup.field("directory", BACKUP_PATH);
    backup.field("modes", modes);
    backup.field("enabled", true);
    backup.field("retentionDays", 30);

    return agent.getBackupManager().addBackup(backup);
  }

  private BackupList getBackupList(String nodeName) {

    OEnterpriseAgent agent = getAgent(nodeName);
    ListBackupCommandProcessor backupCommandProcessor = new ListBackupCommandProcessor();

    Command command = new Command();
    command.setId("test");
    command.setPayload("");
    command.setResponseChannel("channelTest");

    CommandResponse execute = backupCommandProcessor.execute(command, agent);

    Assert.assertTrue(execute.getPayload() instanceof BackupList);

    return (BackupList) execute.getPayload();
  }

  private OEnterpriseAgent getAgent(String server) {

    return this.serverInstance.stream().filter(serverRun -> serverRun.getNodeName().equals(server)).findFirst()
        .orElseThrow(() -> new IllegalArgumentException(String.format("Cannot find server with name %s", server)))
        .getServerInstance().getPluginByClass(OEnterpriseAgent.class);

  }

  @Test
  public void testAddBackupCommandProcessor() throws Exception {

    execute(2, () -> {

      ServerRun firstServer = this.serverInstance.get(0);
      ServerRun secondServer = this.serverInstance.get(1);

      deleteBackupConfig(getAgent(firstServer.getNodeName()));
      deleteBackupConfig(getAgent(secondServer.getNodeName()));

      BackupInfo info = new BackupInfo();
      info.setDbName(DB_NAME);
      info.setDirectory(BACKUP_PATH);
      info.setServer(firstServer.getNodeName());
      info.setEnabled(true);
      info.setRetentionDays(30);

      info.setModes(new HashMap<BackupMode, BackupModeConfig>() {
        {
          put(BackupMode.FULL_BACKUP, new BackupModeConfig("0/5 * * * * ?"));
          put(BackupMode.INCREMENTAL_BACKUP, new BackupModeConfig("0/2 * * * * ?"));
        }
      });

      Command command = new Command();
      command.setId("test");
      command.setPayload(info);
      command.setResponseChannel("channelTest");

      AddBackupCommandProcessor backupCommandProcessor = new AddBackupCommandProcessor();

      CommandResponse execute = backupCommandProcessor.execute(command, getAgent(secondServer.getNodeName()));

      Assert.assertTrue(execute.getPayload() instanceof BackupInfo);

      BackupInfo backupInfo = (BackupInfo) execute.getPayload();

      assertThat(backupInfo.getUuid()).isNotNull();

      assertThat(backupInfo.getDbName()).isEqualTo(DB_NAME);

      assertThat(backupInfo.getDirectory()).isEqualTo(BACKUP_PATH);

      assertThat(backupInfo.getEnabled()).isEqualTo(true);

      assertThat(backupInfo.getServer()).isEqualTo(firstServer.getNodeName());

      assertThat(backupInfo.getRetentionDays()).isEqualTo(30);

      assertThat(backupInfo.getModes()).containsKeys(BackupMode.FULL_BACKUP);
      assertThat(backupInfo.getModes()).containsKeys(BackupMode.INCREMENTAL_BACKUP);

      BackupModeConfig incremental = backupInfo.getModes().get(BackupMode.INCREMENTAL_BACKUP);

      BackupModeConfig full = backupInfo.getModes().get(BackupMode.FULL_BACKUP);

      assertThat(full.getWhen()).isEqualTo("0/5 * * * * ?");
      assertThat(incremental.getWhen()).isEqualTo("0/2 * * * * ?");

      BackupList payload = getBackupList(firstServer.getNodeName());

      assertThat(payload.getBackups()).hasSize(1);

      return null;
    });

  }

  @Test
  public void testRemoveBackupCommandProcessor() throws Exception {

    execute(2, () -> {

      ServerRun firstServer = this.serverInstance.get(0);
      ServerRun secondServer = this.serverInstance.get(1);

      deleteBackupConfig(getAgent(firstServer.getNodeName()));
      deleteBackupConfig(getAgent(secondServer.getNodeName()));

      ODocument cfg = createBackupConfig(getAgent(firstServer.getNodeName()));

      BackupList backupList = getBackupList(secondServer.getNodeName());

      assertThat(backupList.getBackups()).hasSize(1);

      String uuid = cfg.field("uuid");

      Command command = new Command();
      command.setId("test");
      command.setPayload(new BackupLogRequest(uuid).setServer(firstServer.getNodeName()));
      command.setResponseChannel("channelTest");

      RemoveBackupCommandProcessor backupCommandProcessor = new RemoveBackupCommandProcessor();

      backupCommandProcessor.execute(command, getAgent(secondServer.getNodeName()));

      backupList = getBackupList(secondServer.getNodeName());

      assertThat(backupList.getBackups()).hasSize(0);

      return null;
    });

  }

  @Test
  public void testListLogsCommandProcessor() throws Exception {

    execute(2, () -> {
      ServerRun firstServer = this.serverInstance.get(0);
      ServerRun secondServer = this.serverInstance.get(1);

      deleteBackupConfig(getAgent(firstServer.getNodeName()));
      deleteBackupConfig(getAgent(secondServer.getNodeName()));

      ODocument cfg = createBackupConfig(getAgent(firstServer.getNodeName()));

      BackupList backupList = getBackupList(secondServer.getNodeName());

      assertThat(backupList.getBackups()).hasSize(1);

      String uuid = cfg.field("uuid");

      final OBackupTask task = getAgent(firstServer.getNodeName()).getBackupManager().getTask(uuid);

      final OBackupTask task1 = getAgent(secondServer.getNodeName()).getBackupManager().getTask(uuid);

      assertThat(task1).isNull();

      final CountDownLatch latch = new CountDownLatch(3);
      task.registerListener((cfg1, log) -> {
        latch.countDown();
        return latch.getCount() > 0;

      });
      latch.await();

      BackupLogsList backupLogsList = getBackupLogList(getAgent(secondServer.getNodeName()), uuid, firstServer.getNodeName());

      assertThat(backupLogsList.getLogs()).hasSize(4);
      return null;
    });

  }

  private BackupLogsList getBackupLogList(OEnterpriseAgent agent, String uuid, String server) {
    Command command = new Command();
    command.setId("test");
    command.setPayload(new BackupLogRequest(uuid, null, null, new HashMap<>()).setServer(server));
    command.setResponseChannel("channelTest");

    ListBackupLogsCommandProcessor backupCommandProcessor = new ListBackupLogsCommandProcessor();

    CommandResponse execute = backupCommandProcessor.execute(command, agent);

    return (BackupLogsList) execute.getPayload();
  }

  @Test
  public void testListLogsWithUnitIdCommandProcessor() throws Exception {

    execute(2, () -> {

      ServerRun firstServer = this.serverInstance.get(0);
      ServerRun secondServer = this.serverInstance.get(1);

      deleteBackupConfig(getAgent(firstServer.getNodeName()));
      deleteBackupConfig(getAgent(secondServer.getNodeName()));

      ODocument cfg = createBackupConfig(getAgent(firstServer.getNodeName()));

      BackupList backupList = getBackupList(secondServer.getNodeName());

      assertThat(backupList.getBackups()).hasSize(1);

      String uuid = cfg.field("uuid");

      final OBackupTask task = getAgent(firstServer.getNodeName()).getBackupManager().getTask(uuid);

      final OBackupTask task1 = getAgent(secondServer.getNodeName()).getBackupManager().getTask(uuid);

      assertThat(task1).isNull();

      AtomicReference<OBackupLog> lastLog = new AtomicReference<>();
      final CountDownLatch latch = new CountDownLatch(1);
      task.registerListener((cfg1, log) -> {

        if (OBackupLogType.BACKUP_FINISHED.equals(log.getType())) {
          latch.countDown();
          lastLog.set(log);
        }
        return latch.getCount() > 0;

      });
      latch.await();

      Command command = new Command();
      command.setId("test");
      command.setPayload(new BackupLogRequest(uuid, lastLog.get().getUnitId(), null, null, new HashMap<String, String>() {{
        put("op", "BACKUP_FINISHED");
      }}).setServer(firstServer.getNodeName()));
      command.setResponseChannel("channelTest");

      ListBackupLogsCommandProcessor backupCommandProcessor = new ListBackupLogsCommandProcessor();

      CommandResponse execute = backupCommandProcessor.execute(command, getAgent(secondServer.getNodeName()));

      BackupLogsList backupLogsList = (BackupLogsList) execute.getPayload();

      assertThat(backupLogsList.getLogs()).hasSize(1);

      return null;
    });

  }

  @Test
  public void testRemoveBackupLogsCommandProcessor() throws Exception {

    execute(2, () -> {

      ServerRun firstServer = this.serverInstance.get(0);
      ServerRun secondServer = this.serverInstance.get(1);

      deleteBackupConfig(getAgent(firstServer.getNodeName()));
      deleteBackupConfig(getAgent(secondServer.getNodeName()));

      ODocument cfg = createBackupConfig(getAgent(firstServer.getNodeName()));

      BackupList backupList = getBackupList(secondServer.getNodeName());

      assertThat(backupList.getBackups()).hasSize(1);

      String uuid = cfg.field("uuid");

      final OBackupTask task = getAgent(firstServer.getNodeName()).getBackupManager().getTask(uuid);

      final OBackupTask task1 = getAgent(secondServer.getNodeName()).getBackupManager().getTask(uuid);

      assertThat(task1).isNull();

      AtomicReference<OBackupLog> lastLog = new AtomicReference<>();
      final CountDownLatch latch = new CountDownLatch(1);
      task.registerListener((cfg1, log) -> {
        if (OBackupLogType.BACKUP_FINISHED.equals(log.getType())) {
          latch.countDown();
          lastLog.set(log);
        }
        return latch.getCount() > 0;

      });
      latch.await();

      Command command = new Command();
      command.setId("test");
      command.setPayload(
          new BackupLogRequest(uuid, lastLog.get().getUnitId(), lastLog.get().getTxId()).setServer(firstServer.getNodeName()));
      command.setResponseChannel("channelTest");

      RemoveBackupCommandProcessor remove = new RemoveBackupCommandProcessor();

      CommandResponse execute = remove.execute(command, getAgent(secondServer.getNodeName()));

      String result = (String) execute.getPayload();

      BackupLogsList backupLogsList = getBackupLogList(getAgent(secondServer.getNodeName()), uuid, firstServer.getNodeName());

      assertThat(backupLogsList.getLogs()).hasSize(0);
      return null;
    });

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
