package com.orientechnologies.agent.cloud.processor.tasks.backup;

import com.orientechnologies.agent.OEnterpriseAgent;
import com.orientechnologies.agent.services.backup.OBackupService;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.distributed.operation.NodeOperation;
import com.orientechnologies.orient.server.distributed.operation.NodeOperationResponse;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Enrico Risa on 17/01/2018.
 */
public class ListBackupTask implements NodeOperation {

  @Override
  public NodeOperationResponse execute(OServer iServer, ODistributedServerManager iManager) {
    OEnterpriseAgent agent = iServer.getPluginByClass(OEnterpriseAgent.class);
    OBackupService backupService = agent.getServiceByClass(OBackupService.class).get();
    ODocument config = backupService.getConfiguration();
    return new ListBackupTaskResponse(config);
  }

  @Override
  public void write(DataOutput out) throws IOException {

  }

  @Override
  public void read(DataInput in) throws IOException {

  }

  @Override
  public int getMessageId() {
    return 11;
  }
}
