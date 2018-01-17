package com.orientechnologies.agent.cloud.processor.tasks.backup;

import com.orientechnologies.agent.cloud.processor.tasks.AbstractRPCTask;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.distributed.operation.NodeOperationResponse;
import com.orientechnologies.orientdb.cloud.protocol.backup.BackupInfo;

/**
 * Created by Enrico Risa on 17/01/2018.
 */
public class AddBackupTask extends AbstractRPCTask<BackupInfo> {

  private BackupInfo info;

  public AddBackupTask() {
  }

  public AddBackupTask(BackupInfo info) {
    this.info = info;
  }

  @Override
  public NodeOperationResponse execute(OServer iServer, ODistributedServerManager iManager) {
    return null;
  }

  @Override
  protected BackupInfo getPayload() {
    return info;
  }

  @Override
  protected void setPayload(BackupInfo payload) {
    info = payload;
  }

  @Override
  protected Class<BackupInfo> getPayloadType() {
    return BackupInfo.class;
  }

  @Override
  public int getMessageId() {
    return 10;
  }
}
