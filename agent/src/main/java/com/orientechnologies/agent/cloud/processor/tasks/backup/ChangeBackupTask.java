package com.orientechnologies.agent.cloud.processor.tasks.backup;

import com.orientechnologies.agent.OEnterpriseAgent;
import com.orientechnologies.agent.cloud.processor.tasks.AbstractDocumentTask;
import com.orientechnologies.agent.services.backup.OBackupService;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.distributed.operation.NodeOperationResponse;

/**
 * Created by Enrico Risa on 17/01/2018.
 */
public class ChangeBackupTask extends AbstractDocumentTask {

  public ChangeBackupTask() {
  }

  public ChangeBackupTask(ODocument document) {
    super(document);
  }

  @Override
  public NodeOperationResponse execute(OServer iServer, ODistributedServerManager iManager) {

    OEnterpriseAgent agent = iServer.getPluginByClass(OEnterpriseAgent.class);
    OBackupService backupService = agent.getServiceByClass(OBackupService.class).get();
    backupService.changeBackup(payload.field("uuid"), payload);
    payload.field("server", iManager.getLocalNodeName());
    return new ChangeBackupTaskResponse(payload);
  }

  @Override
  public int getMessageId() {
    return 14;
  }
}
