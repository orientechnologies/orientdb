package com.orientechnologies.agent.cloud;

import com.orientechnologies.agent.OEnterpriseAgent;
import com.orientechnologies.agent.backup.OBackupTask;
import com.orientechnologies.agent.backup.log.OBackupLog;
import com.orientechnologies.agent.backup.log.OBackupLogType;
import com.orientechnologies.agent.cloud.processor.backup.*;
import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.OServerMain;
import com.orientechnologies.orientdb.cloud.protocol.Command;
import com.orientechnologies.orientdb.cloud.protocol.CommandResponse;
import com.orientechnologies.orientdb.cloud.protocol.backup.*;
import com.orientechnologies.orientdb.cloud.protocol.backup.log.BackupLogsList;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by Enrico Risa on 19/12/2017.
 */
public class BackupCommandProcessorTest {

  private OServer server;

  private final String DB_NAME     = "backupDB";
  private final String NEW_DB_NAME = "newDB";
  private final String BACKUP_PATH =
      System.getProperty("buildDirectory", "target") + File.separator + "databases" + File.separator + DB_NAME;
  private OEnterpriseAgent agent;

  @Before
  public void bootOrientDB() throws Exception {
    OFileUtils.deleteRecursively(new File(BACKUP_PATH));

    InputStream stream = ClassLoader.getSystemResourceAsStream("orientdb-server-config.xml");
    server = OServerMain.create(false);
    server.startup(stream);

    OrientDB orientDB = server.getContext();

    if (orientDB.exists(DB_NAME))
      orientDB.drop(DB_NAME);

    if (orientDB.exists(NEW_DB_NAME))
      orientDB.drop(NEW_DB_NAME);
    
    orientDB.create(DB_NAME, ODatabaseType.PLOCAL);

    server.activate();

    server.getSystemDatabase().executeInDBScope(iArgument -> {

      iArgument.command("delete from OBackupLog");
      return null;
    });

    agent = server.getPluginByClass(OEnterpriseAgent.class);

    deleteBackupConfig();

  }

  @After
  public void tearDownOrientDB() {

    deleteBackupConfig();

    OrientDB orientDB = server.getContext();
    if (orientDB.exists(DB_NAME))
      orientDB.drop(DB_NAME);

    if (orientDB.exists(NEW_DB_NAME))
      orientDB.drop(NEW_DB_NAME);

    if (server != null)
      server.shutdown();

    Orient.instance().shutdown();
    Orient.instance().startup();

    OFileUtils.deleteRecursively(new File(BACKUP_PATH));

  }

  private void deleteBackupConfig() {
    ODocument configuration = agent.getBackupManager().getConfiguration();

    configuration.<List<ODocument>>field("backups").stream().map(cfg -> cfg.<String>field("uuid")).collect(Collectors.toList())
        .forEach((b) -> agent.getBackupManager().removeAndStopBackup(b));
  }

  @Test
  public void testBackupCommandProcessorEmptyBackups() {

    BackupList payload = getBackupList();

    assertThat(payload.getBackups()).hasSize(0);

  }

  private BackupList getBackupList() {
    ListBackupCommandProcessor backupCommandProcessor = new ListBackupCommandProcessor();

    Command command = new Command();
    command.setId("test");
    command.setPayload("");
    command.setResponseChannel("channelTest");

    CommandResponse execute = backupCommandProcessor.execute(command, agent);

    Assert.assertTrue(execute.getPayload() instanceof BackupList);

    return (BackupList) execute.getPayload();
  }

  @Test
  public void testBackupCommandProcessor() {

    ODocument cfg = createBackupConfig();

    String uuid = cfg.field("uuid");

    BackupList payload = getBackupList();

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

  }

  private ODocument createBackupConfig() {
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

  @Test
  public void testAddBackupCommandProcessor() {

    BackupInfo info = new BackupInfo();
    info.setDbName(DB_NAME);
    info.setDirectory(BACKUP_PATH);
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

    CommandResponse execute = backupCommandProcessor.execute(command, agent);

    Assert.assertTrue(execute.getPayload() instanceof BackupInfo);

    BackupInfo backupInfo = (BackupInfo) execute.getPayload();

    assertThat(backupInfo.getUuid()).isNotNull();

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

  }

  @Test
  public void testRemoveBackupCommandProcessor() {

    ODocument cfg = createBackupConfig();

    BackupList backupList = getBackupList();

    assertThat(backupList.getBackups()).hasSize(1);

    String uuid = cfg.field("uuid");

    Command command = new Command();
    command.setId("test");
    command.setPayload(new BackupLogRequest(uuid));
    command.setResponseChannel("channelTest");

    RemoveBackupCommandProcessor backupCommandProcessor = new RemoveBackupCommandProcessor();

    CommandResponse execute = backupCommandProcessor.execute(command, agent);

    backupList = getBackupList();

    assertThat(backupList.getBackups()).hasSize(0);

  }

  @Test
  public void testListLogsCommandProcessor() throws InterruptedException {

    ODocument cfg = createBackupConfig();

    BackupList backupList = getBackupList();

    assertThat(backupList.getBackups()).hasSize(1);

    String uuid = cfg.field("uuid");

    final OBackupTask task = agent.getBackupManager().getTask(uuid);

    final CountDownLatch latch = new CountDownLatch(3);
    task.registerListener((cfg1, log) -> {
      latch.countDown();
      return latch.getCount() > 0;

    });
    latch.await();

    BackupLogsList backupLogsList = getBackupLogList(uuid);

    assertThat(backupLogsList.getLogs()).hasSize(4);

  }

  private BackupLogsList getBackupLogList(String uuid) {
    Command command = new Command();
    command.setId("test");
    command.setPayload(new BackupLogRequest(uuid, null, null, new HashMap<>()));
    command.setResponseChannel("channelTest");

    ListBackupLogsCommandProcessor backupCommandProcessor = new ListBackupLogsCommandProcessor();

    CommandResponse execute = backupCommandProcessor.execute(command, agent);

    return (BackupLogsList) execute.getPayload();
  }

  @Test
  public void testListLogsWithUnitIdCommandProcessor() throws InterruptedException {

    ODocument cfg = createBackupConfig();

    BackupList backupList = getBackupList();

    assertThat(backupList.getBackups()).hasSize(1);

    String uuid = cfg.field("uuid");

    final OBackupTask task = agent.getBackupManager().getTask(uuid);

    AtomicReference<OBackupLog> lastLog = new AtomicReference<>();
    final CountDownLatch latch = new CountDownLatch(3);
    task.registerListener((cfg1, log) -> {
      latch.countDown();
      lastLog.set(log);
      return latch.getCount() > 0;

    });
    latch.await();

    Command command = new Command();
    command.setId("test");
    command.setPayload(new BackupLogRequest(uuid, lastLog.get().getUnitId(), null, null, new HashMap<String, String>() {{
      put("op", "BACKUP_FINISHED");
    }}));
    command.setResponseChannel("channelTest");

    ListBackupLogsCommandProcessor backupCommandProcessor = new ListBackupLogsCommandProcessor();

    CommandResponse execute = backupCommandProcessor.execute(command, agent);

    BackupLogsList backupLogsList = (BackupLogsList) execute.getPayload();

    assertThat(backupLogsList.getLogs()).hasSize(1);

  }

  @Test
  public void testRemoveBackupLogsCommandProcessor() throws InterruptedException {

    ODocument cfg = createBackupConfig();

    BackupList backupList = getBackupList();

    assertThat(backupList.getBackups()).hasSize(1);

    String uuid = cfg.field("uuid");

    final OBackupTask task = agent.getBackupManager().getTask(uuid);

    AtomicReference<OBackupLog> lastLog = new AtomicReference<>();
    final CountDownLatch latch = new CountDownLatch(3);
    task.registerListener((cfg1, log) -> {
      latch.countDown();

      if (OBackupLogType.BACKUP_FINISHED.equals(log.getType())) {
        lastLog.set(log);
      }
      return latch.getCount() > 0;

    });
    latch.await();

    Command command = new Command();
    command.setId("test");
    command.setPayload(new BackupLogRequest(uuid, lastLog.get().getUnitId(), lastLog.get().getTxId()));
    command.setResponseChannel("channelTest");

    RemoveBackupCommandProcessor remove = new RemoveBackupCommandProcessor();

    CommandResponse execute = remove.execute(command, agent);

    String result = (String) execute.getPayload();

    BackupLogsList backupLogsList = getBackupLogList(uuid);

    assertThat(backupLogsList.getLogs()).hasSize(0);

  }

  @Test
  public void testRestoreDatabaseCommandProcessor() throws InterruptedException {

    ODocument cfg = createBackupConfig();

    BackupList backupList = getBackupList();

    assertThat(backupList.getBackups()).hasSize(1);

    String uuid = cfg.field("uuid");

    final OBackupTask task = agent.getBackupManager().getTask(uuid);

    AtomicReference<OBackupLog> lastLog = new AtomicReference<>();
    final CountDownLatch latch = new CountDownLatch(3);
    task.registerListener((cfg1, log) -> {
      latch.countDown();
      if (OBackupLogType.BACKUP_FINISHED.equals(log.getType())) {
        lastLog.set(log);
      }
      return latch.getCount() > 0;

    });
    latch.await();

    CountDownLatch latch1 = new CountDownLatch(1);
    task.registerListener((cfg1, log) -> {
      if (OBackupLogType.RESTORE_FINISHED.equals(log.getType()) || OBackupLogType.RESTORE_ERROR.equals(log.getType())) {
        latch1.countDown();
      }
      return latch1.getCount() > 0;

    });

    Command command = new Command();
    command.setId("test");
    command.setPayload(new BackupLogRequest(uuid, lastLog.get().getUnitId(), new HashMap<String, String>() {{
      put("target", NEW_DB_NAME);
    }}));
    command.setResponseChannel("channelTest");

    RestoreBackupCommandProcessor remove = new RestoreBackupCommandProcessor();

    CommandResponse execute = remove.execute(command, agent);

    String result = (String) execute.getPayload();

    latch1.await();

    assertThat(server.existsDatabase(NEW_DB_NAME)).isTrue();

  }

}
