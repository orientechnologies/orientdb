package com.orientechnologies.agent.cloud.processor.backup;

import com.orientechnologies.agent.OEnterpriseAgent;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orientdb.cloud.protocol.Command;
import com.orientechnologies.orientdb.cloud.protocol.CommandResponse;
import com.orientechnologies.orientdb.cloud.protocol.backup.BackupLogRequest;

public class RestoreBackupCommandProcessor extends AbstractBackupCommandProcessor {
  @Override
  public CommandResponse execute(Command command, OEnterpriseAgent agent) {

    CommandResponse response = fromRequest(command);

    BackupLogRequest request = (BackupLogRequest) command.getPayload();

    if (request.getUnitId() != null && request.getParams() != null) {

      ODocument body = new ODocument().fromMap(request.getParams()).field("unitId", request.getUnitId());

      agent.getBackupManager().restoreBackup(request.getBackupId(), body);
    }
    response.setPayload("");
    return response;
  }

}
