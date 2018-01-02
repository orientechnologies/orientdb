package com.orientechnologies.agent.cloud.processor.backup;

import com.orientechnologies.agent.OEnterpriseAgent;
import com.orientechnologies.orientdb.cloud.protocol.Command;
import com.orientechnologies.orientdb.cloud.protocol.CommandResponse;
import com.orientechnologies.orientdb.cloud.protocol.backup.BackupInfo;

public class ChangeBackupCommandProcessor extends AbstractBackupCommandProcessor {
  @Override
  public CommandResponse execute(Command command, OEnterpriseAgent agent) {

    CommandResponse response = fromRequest(command);

    BackupInfo info = (BackupInfo) command.getPayload();

    BackupInfo newBackup = changeBackupInfo(agent, info.getUuid(), info);
    response.setPayload(newBackup);

    return response;
  }

  private BackupInfo changeBackupInfo(OEnterpriseAgent agent, String uuid, BackupInfo info) {

    agent.getBackupManager().changeBackup(uuid, toODocument(info));

    return info;
  }

}
