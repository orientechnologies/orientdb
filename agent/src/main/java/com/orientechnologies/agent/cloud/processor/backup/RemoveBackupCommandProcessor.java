package com.orientechnologies.agent.cloud.processor.backup;

import com.orientechnologies.agent.OEnterpriseAgent;
import com.orientechnologies.orientdb.cloud.protocol.Command;
import com.orientechnologies.orientdb.cloud.protocol.CommandResponse;
import com.orientechnologies.orientdb.cloud.protocol.backup.BackupLogRequest;

public class RemoveBackupCommandProcessor extends AbstractBackupCommandProcessor {
  @Override
  public CommandResponse execute(Command command, OEnterpriseAgent agent) {

    CommandResponse response = fromRequest(command);

    BackupLogRequest request = (BackupLogRequest) command.getPayload();

    if (request.getUnitId() != null && request.getTxId() != null) {
      agent.getBackupManager().deleteBackup(request.getBackupId(), request.getUnitId(), request.getTxId());
    } else {
      agent.getBackupManager().removeAndStopBackup(request.getBackupId());
    }

    response.setPayload("");
    return response;
  }

}
