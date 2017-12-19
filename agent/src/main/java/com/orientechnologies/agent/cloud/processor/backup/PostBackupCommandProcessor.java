package com.orientechnologies.agent.cloud.processor.backup;

import com.orientechnologies.agent.OEnterpriseAgent;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orientdb.cloud.protocol.Command;
import com.orientechnologies.orientdb.cloud.protocol.CommandResponse;
import com.orientechnologies.orientdb.cloud.protocol.backup.BackupInfo;

public class PostBackupCommandProcessor extends AbstractBackupCommandProcessor {
  @Override
  public CommandResponse execute(Command command, OEnterpriseAgent agent) {

    CommandResponse response = fromRequest(command);

    BackupInfo info = (BackupInfo) command.getPayload();

    BackupInfo newBackup = postBackupConfig(agent, info);
    response.setPayload(newBackup);

    return response;
  }

  private BackupInfo postBackupConfig(OEnterpriseAgent agent, BackupInfo info) {

    ODocument addedBackup = agent.getBackupManager().addBackup(toODocument(info));

    return fromODocument(addedBackup);
  }

}
