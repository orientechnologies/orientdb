package com.orientechnologies.agent.cloud.processor.backup;

import com.orientechnologies.agent.OEnterpriseAgent;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orientdb.cloud.protocol.Command;
import com.orientechnologies.orientdb.cloud.protocol.CommandResponse;
import com.orientechnologies.orientdb.cloud.protocol.backup.BackupInfo;
import com.orientechnologies.orientdb.cloud.protocol.backup.BackupList;

import java.util.List;
import java.util.stream.Collectors;

public class ListBackupCommandProcessor extends AbstractBackupCommandProcessor {
  @Override
  public CommandResponse execute(Command command, OEnterpriseAgent agent) {

    CommandResponse response = fromRequest(command);

    BackupList backupList = getBackupConfig(agent);
    response.setPayload(backupList);

    return response;
  }

  // TODO Call ALL agents

  private BackupList getBackupConfig(OEnterpriseAgent agent) {

    ODistributedServerManager manager = agent.server.getDistributedManager();
    String server = manager != null ? manager.getLocalNodeName() : "orientdb";
    ODocument config = agent.getBackupManager().getConfiguration();

    List<BackupInfo> backupInfos = config.<List<ODocument>>field("backups").stream().map(c -> {
      BackupInfo backupInfo = fromODocument(c);
      backupInfo.setServer(server);
      return backupInfo;
    }).collect(Collectors.toList());
    BackupList backupList = new BackupList();

    backupList.setBackups(backupInfos);
    return backupList;
  }

}
