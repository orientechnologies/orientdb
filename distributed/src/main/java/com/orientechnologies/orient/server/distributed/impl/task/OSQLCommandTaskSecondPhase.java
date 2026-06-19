package com.orientechnologies.orient.server.distributed.impl.task;

import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.transaction.OTransactionId;
import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.ODistributedRequestId;
import com.orientechnologies.orient.server.distributed.ORemoteTaskFactory;
import com.orientechnologies.orient.server.distributed.impl.ODatabaseDocumentDistributed;
import com.orientechnologies.orient.server.distributed.impl.task.transaction.OTransactionUniqueKey;
import com.orientechnologies.orient.server.distributed.task.OAbstractRemoteTask;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.SortedSet;
import java.util.TreeSet;

public class OSQLCommandTaskSecondPhase extends OAbstractRemoteTask implements OLockKeySource {

  public static final int FACTORYID = 60;

  private ODistributedRequestId confirmSentRequest;
  private OTransactionIdPromise preChange;
  private OTransactionIdPromise afterChange;
  private boolean apply;

  public OSQLCommandTaskSecondPhase() {}

  public OSQLCommandTaskSecondPhase(
      ODistributedRequestId confirmSentRequest,
      OTransactionIdPromise preChange,
      OTransactionIdPromise afterChange,
      boolean apply) {
    this.confirmSentRequest = confirmSentRequest;
    this.preChange = preChange;
    this.afterChange = afterChange;
    this.apply = apply;
  }

  @Override
  public String getName() {
    return "sql_command_ddl_second_phase";
  }

  @Override
  public OCommandDistributedReplicateRequest.QUORUM_TYPE getQuorumType() {
    return OCommandDistributedReplicateRequest.QUORUM_TYPE.WRITE;
  }

  @Override
  public Object execute(
      ODistributedRequestId requestId, OServer iServer, ODatabaseDocumentInternal database)
      throws Exception {
    ((ODatabaseDocumentDistributed) database).secondPhaseDDL(this.confirmSentRequest, this.apply);
    return null;
  }

  @Override
  public void toStream(DataOutput out) throws IOException {
    super.toStream(out);
    this.confirmSentRequest.toStream(out);
    this.preChange.writeNetwork(out);
    ;
    this.afterChange.writeNetwork(out);
    out.writeBoolean(apply);
  }

  @Override
  public void fromStream(DataInput in, ORemoteTaskFactory factory) throws IOException {
    super.fromStream(in, factory);
    this.confirmSentRequest = new ODistributedRequestId();
    this.confirmSentRequest.fromStream(in);
    this.preChange = OTransactionIdPromise.readNetwork(in);
    this.afterChange = OTransactionIdPromise.readNetwork(in);
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

  @Override
  public SortedSet<ORID> getRids() {
    var rid = new TreeSet<ORID>();
    // Manually set the record id to 0:1 because that's the schema id, and all DDL
    // have to be executed sequentially
    rid.add(new ORecordId(0, 1));
    return rid;
  }

  @Override
  public OTransactionId getTransactionId() {
    return preChange.getId();
  }

  @Override
  public SortedSet<OTransactionUniqueKey> getUniqueKeys() {
    return Collections.emptySortedSet();
  }

  public OTransactionIdPromise getPreChange() {
    return preChange;
  }

  public OTransactionIdPromise getAfterChange() {
    return afterChange;
  }
}
