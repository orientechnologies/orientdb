package com.orientechnologies.agent.cloud.processor.tasks.backup;

import com.orientechnologies.agent.OEnterpriseAgent;
import com.orientechnologies.agent.cloud.processor.backup.ListBackupLogsCommandProcessor;
import com.orientechnologies.agent.cloud.processor.tasks.AbstractRPCTask;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.distributed.operation.NodeOperationResponse;
import com.orientechnologies.orientdb.cloud.protocol.backup.BackupLogRequest;
import com.orientechnologies.orientdb.cloud.protocol.backup.log.BackupLogsList;

/**
 * Created by Enrico Risa on 17/01/2018.
 */
public class ListBackupLogsTask extends AbstractRPCTask<BackupLogRequest> {

  public ListBackupLogsTask() {
  }

  public ListBackupLogsTask(BackupLogRequest request) {
    super(request);
  }

  @Override
  protected Class<BackupLogRequest> getPayloadType() {
    return BackupLogRequest.class;
  }

  @Override
  public NodeOperationResponse execute(OServer iServer, ODistributedServerManager iManager) {

    OEnterpriseAgent agent = iServer.getPluginByClass(OEnterpriseAgent.class);
    BackupLogsList logsList = ListBackupLogsCommandProcessor.getBackupLogsList(agent, getPayload());
    return new ListBackupLogsResponse(logsList);
  }

  @Override
  public int getMessageId() {
    return 13;
  }
}
