package com.orientechnologies.orient.distributed.context.coordination.message;

import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.distributed.context.coordination.message.operation.OOperationMessage;
import com.orientechnologies.orient.distributed.db.OrientDBDistributed;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class OConfirmedOps implements OStructuralMessage {

  private final List<OConfirmedRetryOp> ops;

  public OConfirmedOps(List<OConfirmedRetryOp> ops) {
    this.ops = ops;
  }

  @Override
  public void execute(OrientDBDistributed ctx) {
    ctx.receiveRecovery(ops);
  }

  @Override
  public void serialize(DataOutput out) throws IOException {
    out.writeInt(ops.size());
    for (var op : ops) {
      op.promiseId().writeNetwork(out);
      op.op().writeNetwork(out);
    }
  }

  @Override
  public short getType() {
    return 19;
  }

  public static OConfirmedOps fromNetwork(DataInput input) throws IOException {
    int size = input.readInt();
    var ops = new ArrayList<OConfirmedRetryOp>(size);
    while (size-- > 0) {
      var promise = OTransactionIdPromise.readNetwork(input);
      var op = OOperationMessage.readNetwork(input);
      ops.add(new OConfirmedRetryOp(promise, op));
    }
    return new OConfirmedOps(ops);
  }
}
