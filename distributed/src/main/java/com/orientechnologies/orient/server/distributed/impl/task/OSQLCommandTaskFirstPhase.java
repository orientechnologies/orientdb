package com.orientechnologies.orient.server.distributed.impl.task;

import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.tx.OTransactionId;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.ODistributedRequestId;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.distributed.ORemoteTaskFactory;
import com.orientechnologies.orient.server.distributed.impl.ODatabaseDocumentDistributed;
import com.orientechnologies.orient.server.distributed.impl.task.transaction.OTransactionResultPayload;
import com.orientechnologies.orient.server.distributed.task.OAbstractRemoteTask;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OSQLCommandTaskFirstPhase extends OAbstractRemoteTask {

  public static final int FACTORYID = 59;

  private String query;
  private OTransactionId preChangeId;
  private OTransactionId afterChangeId;

  public OSQLCommandTaskFirstPhase() {}

  public OSQLCommandTaskFirstPhase(
      String query, OTransactionId preChangeId, OTransactionId afterChangeId) {
    this.query = query;
    this.preChangeId = preChangeId;
    this.afterChangeId = afterChangeId;
  }

  @Override
  public String getName() {
    return "sql_command_ddl_first_phase";
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
    OTransactionResultPayload res =
        ((ODatabaseDocumentDistributed) database)
            .firstPhaseDDL(query, preChangeId, afterChangeId, requestId);
    return new OTransactionPhase1TaskResult(res);
  }

  @Override
  public void toStream(DataOutput out) throws IOException {
    super.toStream(out);
    out.writeUTF(query);
    preChangeId.write(out);
    afterChangeId.write(out);
  }

  @Override
  public void fromStream(DataInput in, ORemoteTaskFactory factory) throws IOException {
    super.fromStream(in, factory);
    query = in.readUTF();
    preChangeId = OTransactionId.read(in);
    afterChangeId = OTransactionId.read(in);
  }

  @Override
  public int getFactoryId() {
    return FACTORYID;
  }

  public String getQuery() {
    return query;
  }

  public OTransactionId getPreChangeId() {
    return preChangeId;
  }

  public OTransactionId getAfterChangeId() {
    return afterChangeId;
  }
}
