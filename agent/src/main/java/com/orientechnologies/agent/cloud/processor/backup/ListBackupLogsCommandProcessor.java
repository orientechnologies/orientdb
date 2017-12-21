package com.orientechnologies.agent.cloud.processor.backup;

import com.orientechnologies.agent.OEnterpriseAgent;
import com.orientechnologies.orientdb.cloud.protocol.Command;
import com.orientechnologies.orientdb.cloud.protocol.CommandResponse;
import com.orientechnologies.orientdb.cloud.protocol.backup.BackupLogRequest;
import com.orientechnologies.orientdb.cloud.protocol.backup.log.BackupLogsList;

import java.util.HashMap;

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
    agent.getBackupManager().findLogs(request.getBackupId(), page, pageSide, new HashMap<>()).stream()
        .map(BackupLogConverter::convert).forEach(l -> backupLogsList.addLog(l));

    return backupLogsList;
  }

}
