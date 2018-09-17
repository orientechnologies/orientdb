package com.orientechnologies.agent.cloud.processor.backup;

import com.orientechnologies.agent.OEnterpriseAgent;
import com.orientechnologies.agent.cloud.processor.tasks.backup.ListBackupLogsResponse;
import com.orientechnologies.agent.cloud.processor.tasks.backup.ListBackupLogsTask;
import com.orientechnologies.agent.operation.NodeResponse;
import com.orientechnologies.agent.operation.OperationResponseFromNode;
import com.orientechnologies.agent.operation.ResponseOk;
import com.orientechnologies.agent.services.backup.OBackupService;
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

    if (!agent.isDistributed()) {
      return getBackupLogsList(agent, request);
    } else {

      OperationResponseFromNode response = agent.getNodesManager().send(request.getServer(), new ListBackupLogsTask(request));

      NodeResponse nodeResponse = response.getNodeResponse();

      if (nodeResponse.getResponseType() == 1) {
        ResponseOk ok = (ResponseOk) nodeResponse;

        ListBackupLogsResponse res = (ListBackupLogsResponse) ok.getPayload();

        return res.getPayload();

      }
      return null;
    }
  }

  public static BackupLogsList getBackupLogsList(OEnterpriseAgent agent, BackupLogRequest request) {
    BackupLogsList backupLogsList = new BackupLogsList();

    OBackupService backupService = agent.getServiceByClass(OBackupService.class).get();
    int page = request.getPage() != null ? request.getPage().intValue() : 1;
    int pageSide = request.getPageSize() != null ? request.getPageSize().intValue() : -1;

    if (request.getUnitId() == null) {
      backupService.findLogs(request.getBackupId(), page, pageSide, request.getParams()).stream().map(BackupLogConverter::convert)
          .forEach(l -> backupLogsList.addLog(l));
    } else {
      backupService.findLogs(request.getBackupId(), request.getUnitId(), page, pageSide, request.getParams()).stream()
          .map(BackupLogConverter::convert).forEach(l -> backupLogsList.addLog(l));
    }

    return backupLogsList;
  }

}
