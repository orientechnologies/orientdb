package com.orientechnologies.agent.cloud.processor.backup;

import com.orientechnologies.agent.OEnterpriseAgent;
import com.orientechnologies.agent.cloud.processor.tasks.backup.ListBackupTask;
import com.orientechnologies.agent.cloud.processor.tasks.backup.ListBackupTaskResponse;
import com.orientechnologies.agent.operation.OperationResponseFromNode;
import com.orientechnologies.agent.operation.ResponseOk;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orientdb.cloud.protocol.Command;
import com.orientechnologies.orientdb.cloud.protocol.CommandResponse;
import com.orientechnologies.orientdb.cloud.protocol.backup.BackupInfo;
import com.orientechnologies.orientdb.cloud.protocol.backup.BackupList;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class ListBackupCommandProcessor extends AbstractBackupCommandProcessor {
  @Override
  public CommandResponse execute(Command command, OEnterpriseAgent agent) {

    CommandResponse response = fromRequest(command);

    BackupList backupList = getBackupConfig(agent);
    response.setPayload(backupList);

    return response;
  }

  private BackupList getBackupConfig(OEnterpriseAgent agent) {

    BackupList backupList = new BackupList();

    if (agent.isDistributed()) {

      List<BackupInfo> backupInfos = new ArrayList<>();
      List<OperationResponseFromNode> response = agent.getNodesManager().sendAll(new ListBackupTask());

      for (OperationResponseFromNode r : response) {
        if (r.getNodeResponse().getResponseType() == 1) {
          ResponseOk ok = (ResponseOk) r.getNodeResponse();
          ListBackupTaskResponse backup = (ListBackupTaskResponse) ok.getPayload();
          String server = r.getSenderNodeName();
          ODocument config = backup.getConfig();
          List<BackupInfo> infos = config.<List<ODocument>>field("backups").stream().map(c -> {
            BackupInfo backupInfo = fromODocument(c);
            backupInfo.setServer(server);
            return backupInfo;
          }).collect(Collectors.toList());

          backupInfos.addAll(infos);
        }

      }
      backupList.setBackups(backupInfos);

    } else {
      String server = "orientdb";
      ODocument config = agent.getBackupManager().getConfiguration();

      List<BackupInfo> backupInfos = config.<List<ODocument>>field("backups").stream().map(c -> {
        BackupInfo backupInfo = fromODocument(c);
        backupInfo.setServer(server);
        return backupInfo;
      }).collect(Collectors.toList());

      backupList.setBackups(backupInfos);

    }
    return backupList;
  }

}
