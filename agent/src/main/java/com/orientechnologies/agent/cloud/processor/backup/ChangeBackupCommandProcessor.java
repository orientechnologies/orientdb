package com.orientechnologies.agent.cloud.processor.backup;

import com.orientechnologies.agent.OEnterpriseAgent;
import com.orientechnologies.agent.cloud.CloudException;
import com.orientechnologies.agent.cloud.processor.tasks.backup.ChangeBackupTask;
import com.orientechnologies.agent.cloud.processor.tasks.backup.ChangeBackupTaskResponse;
import com.orientechnologies.agent.operation.NodeResponse;
import com.orientechnologies.agent.operation.OperationResponseFromNode;
import com.orientechnologies.agent.operation.ResponseOk;
import com.orientechnologies.agent.services.backup.OBackupService;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orientdb.cloud.protocol.Command;
import com.orientechnologies.orientdb.cloud.protocol.CommandResponse;
import com.orientechnologies.orientdb.cloud.protocol.backup.BackupInfo;

public class ChangeBackupCommandProcessor extends AbstractBackupCommandProcessor {
  @Override
  public CommandResponse execute(Command command, OEnterpriseAgent agent) {

    CommandResponse response = fromRequest(command);

    BackupInfo info = (BackupInfo) command.getPayload();

    if (!agent.isDistributed()) {
      BackupInfo newBackup = changeBackupInfo(agent, info.getUuid(), info);
      response.setPayload(newBackup);
    } else {

      OperationResponseFromNode res = agent.getNodesManager().send(info.getServer(), new ChangeBackupTask(toODocument(info)));

      NodeResponse nodeResponse = res.getNodeResponse();
      // OK
      if (nodeResponse.getResponseType() == 1) {
        ResponseOk ok = (ResponseOk) nodeResponse;
        ChangeBackupTaskResponse payload = (ChangeBackupTaskResponse) ok.getPayload();
        ODocument changed = payload.getConfig();
        response.setPayload(fromODocument(changed));

      } else {
        throw new CloudException("", 500, String.format("Cannot execute request on node %d", info.getServer()), "");
      }
    }
    return response;
  }

  public static BackupInfo changeBackupInfo(OEnterpriseAgent agent, String uuid, BackupInfo info) {

    ODocument config = toODocument(info);
    OBackupService backupService = agent.getServiceByClass(OBackupService.class).get();
    backupService.changeBackup(uuid, config);
    return fromODocument(config);
  }

}
