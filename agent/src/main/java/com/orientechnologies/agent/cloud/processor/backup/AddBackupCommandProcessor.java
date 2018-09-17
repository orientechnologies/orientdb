package com.orientechnologies.agent.cloud.processor.backup;

import com.orientechnologies.agent.OEnterpriseAgent;
import com.orientechnologies.agent.cloud.CloudException;
import com.orientechnologies.agent.cloud.processor.tasks.backup.AddBackupTask;
import com.orientechnologies.agent.cloud.processor.tasks.backup.AddBackupTaskResponse;
import com.orientechnologies.agent.operation.NodeResponse;
import com.orientechnologies.agent.operation.OperationResponseFromNode;
import com.orientechnologies.agent.operation.ResponseOk;
import com.orientechnologies.agent.services.backup.OBackupService;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orientdb.cloud.protocol.Command;
import com.orientechnologies.orientdb.cloud.protocol.CommandResponse;
import com.orientechnologies.orientdb.cloud.protocol.backup.BackupInfo;

public class AddBackupCommandProcessor extends AbstractBackupCommandProcessor {
  @Override
  public CommandResponse execute(Command command, OEnterpriseAgent agent) {

    CommandResponse response = fromRequest(command);

    BackupInfo info = (BackupInfo) command.getPayload();

    BackupInfo newBackup = postBackupConfig(agent, info);
    response.setPayload(newBackup);

    return response;
  }

  private BackupInfo postBackupConfig(OEnterpriseAgent agent, BackupInfo info) {

    ODocument addedBackup;

    if (!agent.isDistributed()) {
      OBackupService backupService = agent.getServiceByClass(OBackupService.class).get();
      addedBackup = backupService.addBackup(toODocument(info));
    } else {

      OperationResponseFromNode response = agent.getNodesManager().send(info.getServer(), new AddBackupTask(toODocument(info)));

      NodeResponse nodeResponse = response.getNodeResponse();

      if (nodeResponse.getResponseType() == 1) {
        ResponseOk ok = (ResponseOk) nodeResponse;
        AddBackupTaskResponse payload = (AddBackupTaskResponse) ok.getPayload();
        addedBackup = payload.getConfig();
      } else {
        throw new CloudException("", 500, String.format("Cannot execute request on node %d", info.getServer()), "");
      }
    }

    return fromODocument(addedBackup);
  }

}
