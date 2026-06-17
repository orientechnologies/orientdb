package com.orientechnologies.orient.server.distributed.impl.task;

import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.ODistributedRequestId;
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
  private OTransactionIdPromise preChangeId;
  private OTransactionIdPromise afterChangeId;

  public OSQLCommandTaskFirstPhase() {}

  public OSQLCommandTaskFirstPhase(
      String query, OTransactionIdPromise preChangeId, OTransactionIdPromise afterChangeId) {
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
    return OCommandDistributedReplicateRequest.QUORUM_TYPE.WRITE;
  }

  @Override
  public Object execute(
      ODistributedRequestId requestId, OServer iServer, ODatabaseDocumentInternal database)
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
    preChangeId.writeNetwork(out);
    afterChangeId.writeNetwork(out);
  }

  @Override
  public void fromStream(DataInput in, ORemoteTaskFactory factory) throws IOException {
    super.fromStream(in, factory);
    query = in.readUTF();
    preChangeId = OTransactionIdPromise.readNetwork(in);
    afterChangeId = OTransactionIdPromise.readNetwork(in);
  }

  @Override
  public int getFactoryId() {
    return FACTORYID;
  }

  public String getQuery() {
    return query;
  }

  public OTransactionIdPromise getPreChangeId() {
    return preChangeId;
  }

  public OTransactionIdPromise getAfterChangeId() {
    return afterChangeId;
  }
}
