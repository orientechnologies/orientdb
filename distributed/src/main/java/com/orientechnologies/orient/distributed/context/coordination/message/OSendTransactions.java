package com.orientechnologies.orient.distributed.context.coordination.message;

import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.core.transaction.OTransactionId;
import com.orientechnologies.orient.distributed.db.OrientDBDistributed;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class OSendTransactions implements OStructuralMessage {

  private final ONodeId nodeId;
  private final List<OTransactionId> transactions;

  public OSendTransactions(ONodeId nodeId, List<OTransactionId> transactions) {
    this.nodeId = nodeId;
    this.transactions = transactions;
  }

  @Override
  public void execute(OrientDBDistributed ctx) {
    ctx.sendTopologyTransactions(this.nodeId, this.transactions);
  }

  @Override
  public void serialize(DataOutput out) throws IOException {
    this.nodeId.writeNetwork(out);
    out.writeInt(transactions.size());
    for (var tx : transactions) {
      tx.writeNetwork(out);
    }
  }

  @Override
  public short getType() {
    return 18;
  }

  public static OSendTransactions fromNetwork(DataInput input) throws IOException {
    var nodeId = ONodeId.readNetwork(input);
    int size = input.readInt();
    var transactions = new ArrayList<OTransactionId>(size);
    while (size-- > 0) {
      transactions.add(OTransactionId.readNetwork(input));
    }
    return new OSendTransactions(nodeId, transactions);
  }

  public ONodeId getNodeId() {
    return nodeId;
  }

  public List<OTransactionId> getTransactions() {
    return transactions;
  }
}
