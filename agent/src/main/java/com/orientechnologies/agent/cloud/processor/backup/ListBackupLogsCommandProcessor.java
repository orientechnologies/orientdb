package com.orientechnologies.agent.cloud.processor.backup;

import com.orientechnologies.agent.OEnterpriseAgent;
import com.orientechnologies.orientdb.cloud.protocol.Command;
import com.orientechnologies.orientdb.cloud.protocol.CommandResponse;
import com.orientechnologies.orientdb.cloud.protocol.backup.BackupLogRequest;
import com.orientechnologies.orientdb.cloud.protocol.backup.log.BackupLogsList;

public class ListBackupLogsCommandProcessor extends AbstractBackupCommandProcessor {
  @Override
  public CommandResponse execute(Command command, OEnterpriseAgent agent) {

    CommandResponse response = fromRequest(command);

    BackupLogRequest request = (BackupLogRequest) command.getPayload();

    BackupLogsList newBackup = fetchBackupLogs(agent, request);
    response.setPayload(newBackup);

    return response;
  }

  private BackupLogsList fetchBackupLogs(OEnterpriseAgent agent, BackupLogRequest request) {

    BackupLogsList backupLogsList = new BackupLogsList();

    int page = request.getPage() != null ? request.getPage().intValue() : 1;
    int pageSide = request.getPageSize() != null ? request.getPageSize().intValue() : -1;

    if (request.getUnitId() == null) {
      agent.getBackupManager().findLogs(request.getBackupId(), page, pageSide, request.getParams()).stream()
          .map(BackupLogConverter::convert).forEach(l -> backupLogsList.addLog(l));
    } else {
      agent.getBackupManager().findLogs(request.getBackupId(),request.getUnitId(), page, pageSide, request.getParams()).stream()
          .map(BackupLogConverter::convert).forEach(l -> backupLogsList.addLog(l));
    }

    return backupLogsList;
  }

}
