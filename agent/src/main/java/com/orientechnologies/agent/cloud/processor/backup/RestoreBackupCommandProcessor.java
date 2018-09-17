package com.orientechnologies.agent.cloud.processor.backup;

import com.orientechnologies.agent.OEnterpriseAgent;
import com.orientechnologies.agent.cloud.CloudException;
import com.orientechnologies.agent.cloud.processor.tasks.backup.RestoreBackupTask;
import com.orientechnologies.agent.operation.NodeResponse;
import com.orientechnologies.agent.operation.OperationResponseFromNode;
import com.orientechnologies.agent.operation.ResponseOk;
import com.orientechnologies.agent.services.backup.OBackupService;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orientdb.cloud.protocol.Command;
import com.orientechnologies.orientdb.cloud.protocol.CommandResponse;
import com.orientechnologies.orientdb.cloud.protocol.backup.BackupLogRequest;

public class RestoreBackupCommandProcessor extends AbstractBackupCommandProcessor {
  @Override
  public CommandResponse execute(Command command, OEnterpriseAgent agent) {

    CommandResponse response = fromRequest(command);

    BackupLogRequest request = (BackupLogRequest) command.getPayload();

    if (!agent.isDistributed()) {
      if (request.getUnitId() != null && request.getParams() != null) {

        restoreBackup(agent, request);
      }
    } else {
      OperationResponseFromNode res = agent.getNodesManager().send(request.getServer(), new RestoreBackupTask(request));
      NodeResponse nodeResponse = res.getNodeResponse();

      if (nodeResponse.getResponseType() == 1) {
        ResponseOk ok = (ResponseOk) nodeResponse;
      } else {
        throw new CloudException("", 500, String.format("Cannot execute request on node %d", request.getServer()), "");
      }
    }
    response.setPayload("");
    return response;
  }

  public static void restoreBackup(OEnterpriseAgent agent, BackupLogRequest request) {

    OBackupService backupService = agent.getServiceByClass(OBackupService.class).get();;
    ODocument body = new ODocument().fromMap(request.getParams()).field("unitId", request.getUnitId())
        .field("backupId", request.getBackupId());

    backupService.restoreBackup(request.getBackupId(), body);
  }

}
