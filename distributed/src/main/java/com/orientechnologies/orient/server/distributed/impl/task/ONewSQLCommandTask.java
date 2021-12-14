package com.orientechnologies.orient.server.distributed.impl.task;

import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.OScenarioThreadLocal;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.ODistributedRequestId;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.distributed.ORemoteTaskFactory;
import com.orientechnologies.orient.server.distributed.impl.ODatabaseDocumentDistributed;
import com.orientechnologies.orient.server.distributed.impl.ODistributedDatabaseImpl;
import com.orientechnologies.orient.server.distributed.task.OAbstractRemoteTask;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ONewSQLCommandTask extends OAbstractRemoteTask {

  public static final int FACTORYID = 56;

  private String query;

  public ONewSQLCommandTask() {}

  public ONewSQLCommandTask(String query) {
    this.query = query;
  }

  @Override
  public String getName() {
    return "sql_command_ddl";
  }

  @Override
  public OCommandDistributedReplicateRequest.QUORUM_TYPE getQuorumType() {
    return OCommandDistributedReplicateRequest.QUORUM_TYPE.ALL;
  }

  @Override
  public Object execute(
      ODistributedRequestId requestId,
      OServer iServer,
      ODistributedServerManager iManager,
      ODatabaseDocumentInternal database)
      throws Exception {
    OScenarioThreadLocal.executeAsDistributed(
        () -> {
          ((ODistributedDatabaseImpl)
                  ((ODatabaseDocumentDistributed) database).getDistributedShared())
              .resetLastValidBackup();
          database.command(query);
          return null;
        });
    return null;
  }

  @Override
  public void toStream(DataOutput out) throws IOException {
    super.toStream(out);
    out.writeUTF(query);
  }

  @Override
  public void fromStream(DataInput in, ORemoteTaskFactory factory) throws IOException {
    super.fromStream(in, factory);
    query = in.readUTF();
  }

  @Override
  public int getFactoryId() {
    return FACTORYID;
  }
}
