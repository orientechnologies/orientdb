package com.orientechnologies.orient.server.distributed.impl.task;

import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.ODistributedRequestId;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.distributed.ORemoteTaskFactory;
import com.orientechnologies.orient.server.distributed.impl.ODatabaseDocumentDistributed;
import com.orientechnologies.orient.server.distributed.task.OAbstractRemoteTask;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OSQLCommandTaskSecondPhase extends OAbstractRemoteTask {

  public static final int FACTORYID = 60;

  private ODistributedRequestId confirmSentRequest;
  private boolean apply;

  public OSQLCommandTaskSecondPhase() {}

  public OSQLCommandTaskSecondPhase(ODistributedRequestId confirmSentRequest, boolean apply) {
    this.confirmSentRequest = confirmSentRequest;
    this.apply = apply;
  }

  @Override
  public String getName() {
    return "sql_command_ddl_second_phase";
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
    ((ODatabaseDocumentDistributed) database).secondPhaseDDL(this.confirmSentRequest, this.apply);
    return null;
  }

  @Override
  public void toStream(DataOutput out) throws IOException {
    super.toStream(out);
    this.confirmSentRequest.toStream(out);
    out.writeBoolean(apply);
  }

  @Override
  public void fromStream(DataInput in, ORemoteTaskFactory factory) throws IOException {
    super.fromStream(in, factory);
    this.confirmSentRequest = new ODistributedRequestId();
    this.confirmSentRequest.fromStream(in);
    this.apply = in.readBoolean();
  }

  @Override
  public int getFactoryId() {
    return FACTORYID;
  }

  public ODistributedRequestId getConfirmSentRequest() {
    return confirmSentRequest;
  }

  public boolean isApply() {
    return apply;
  }
}
