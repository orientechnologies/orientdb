package com.orientechnologies.orient.distributed.context.coordination.message;

import com.orientechnologies.orient.core.transaction.OGroupId;
import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.distributed.context.coordination.message.state.ONodeStateNetwork;
import com.orientechnologies.orient.distributed.db.OrientDBDistributed;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OMergeRequest implements OStructuralMessage {

  private OTransactionIdPromise promise;
  private OGroupId group;
  private ONodeStateNetwork state;
  private ONodeStateNetwork original;

  public OMergeRequest(
      OTransactionIdPromise promise,
      OGroupId groupId,
      ONodeStateNetwork state,
      ONodeStateNetwork original) {
    this.promise = promise;
    this.group = groupId;
    this.state = state;
    this.original = original;
  }

  @Override
  public void execute(OrientDBDistributed ctx) {
    ctx.validateMergeToNetwork(group, state, original, promise);
  }

  @Override
  public void serialize(DataOutput out) throws IOException {
    promise.writeNetwork(out);
    group.writeNetwork(out);
    this.state.writeNetwork(out);
    this.original.writeNetwork(out);
  }

  @Override
  public short getType() {
    return 12;
  }

  public static OMergeRequest fromNetwork(DataInput input) throws IOException {
    OTransactionIdPromise promise = OTransactionIdPromise.readNetwork(input);
    OGroupId group = OGroupId.readNetwork(input);
    var state = ONodeStateNetwork.fromNetwork(input);
    var original = ONodeStateNetwork.fromNetwork(input);
    return new OMergeRequest(promise, group, state, original);
  }

  public OGroupId getGroup() {
    return group;
  }

  public OTransactionIdPromise getPromise() {
    return promise;
  }

  @Override
  public String toString() {
    return "OMergeRequest [promise=" + promise + ", group=" + group + "]";
  }
}
