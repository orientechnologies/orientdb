package com.orientechnologies.orient.distributed.context.coordination.message;

import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.distributed.context.coordination.result.OAcceptResult;
import com.orientechnologies.orient.distributed.db.OrientDBDistributed;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Optional;

public class OMergeResult implements OStructuralMessage {

  private ONodeId node;
  private OTransactionIdPromise promise;
  private Optional<OAcceptResult> accepted;

  public OMergeResult(
      ONodeId node, OTransactionIdPromise promise, Optional<OAcceptResult> accepted) {
    this.node = node;
    this.promise = promise;
    this.accepted = accepted;
  }

  @Override
  public void execute(OrientDBDistributed ctx) {
    ctx.nodeMergeResult(this.node, this.promise, this.accepted);
  }

  @Override
  public void serialize(DataOutput out) throws IOException {
    this.node.writeNetwork(out);
    this.promise.writeNetwork(out);
    if (this.accepted.isPresent()) {
      out.writeBoolean(true);
      this.accepted.get().writeNetwork(out);
    } else {
      out.writeBoolean(false);
    }
  }

  @Override
  public short getType() {
    return 13;
  }

  public static OMergeResult fromNetwork(DataInput input) throws IOException {
    ONodeId nodeId = ONodeId.readNetwork(input);
    OTransactionIdPromise promise = OTransactionIdPromise.readNetwork(input);
    boolean present = input.readBoolean();
    Optional<OAcceptResult> result;
    if (present) {
      result = Optional.of(OAcceptResult.readNetwork(input));
    } else {
      result = Optional.empty();
    }
    return new OMergeResult(nodeId, promise, result);
  }

  public ONodeId getNode() {
    return node;
  }

  public OTransactionIdPromise getPromise() {
    return promise;
  }

  public Optional<OAcceptResult> getAccepted() {
    return accepted;
  }

  @Override
  public String toString() {
    return "OMergeResult [node=" + node + ", promise=" + promise + ", accepted=" + accepted + "]";
  }
}
