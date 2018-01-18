package com.orientechnologies.agent.cloud.processor.tasks.backup;

import com.orientechnologies.agent.cloud.processor.tasks.AbstractRPCTaskResponse;
import com.orientechnologies.orientdb.cloud.protocol.backup.log.BackupLogsList;

/**
 * Created by Enrico Risa on 18/01/2018.
 */
public class ListBackupLogsResponse extends AbstractRPCTaskResponse<BackupLogsList> {

  public ListBackupLogsResponse() {
  }

  public ListBackupLogsResponse(BackupLogsList payload) {
    super(payload);
  }

  @Override
  protected Class<BackupLogsList> getPayloadType() {
    return BackupLogsList.class;
  }
}
