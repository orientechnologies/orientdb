package com.orientechnologies.orient.distributed.context.coordination.message;

import com.orientechnologies.orient.core.id.ONodeId;
import com.orientechnologies.orient.core.tx.OTransactionSequenceStatus;
import com.orientechnologies.orient.distributed.db.OrientDBDistributed;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OSendTransactions implements OStructuralMessage {

  private final ONodeId receiver;
  private final OTransactionSequenceStatus state;

  public OSendTransactions(ONodeId nodeId, OTransactionSequenceStatus state) {
    this.receiver = nodeId;
    this.state = state;
  }

  @Override
  public void execute(OrientDBDistributed ctx) {
    ctx.sendTopologyTransactions(this.receiver, this.state);
  }

  @Override
  public void serialize(DataOutput out) throws IOException {
    this.receiver.writeNetwork(out);
    state.writeNetwork(out);
  }

  @Override
  public short getType() {
    return 18;
  }

  public static OSendTransactions fromNetwork(DataInput input) throws IOException {
    var nodeId = ONodeId.readNetwork(input);
    var state = OTransactionSequenceStatus.readNetwork(input);
    return new OSendTransactions(nodeId, state);
  }

  public ONodeId getReceiver() {
    return receiver;
  }

  public OTransactionSequenceStatus getState() {
    return state;
  }

  @Override
  public String toString() {
    return " SendTransactions [receiver=" + receiver + ", transactions=" + state + "]";
  }
}
