package com.orientechnologies.orient.distributed.context.coordination.message.operation;

import com.orientechnologies.orient.core.transaction.ODatabaseId;
import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.distributed.context.coordination.result.OAcceptResult;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

public class ODeclareDbMessage implements OOperationMessage {

  private String name;
  private ODatabaseId id;
  private Set<OAddNodeInfo> partecipants;
  private int minimumQuorum;

  public ODeclareDbMessage(
      String name, ODatabaseId id, Set<OAddNodeInfo> partecipants, int minimumQuorum) {
    this.name = name;
    this.id = id;
    this.partecipants = partecipants;
    this.minimumQuorum = minimumQuorum;
  }

  @Override
  public Optional<OAcceptResult> validate(OOperationContext ctx, OTransactionIdPromise promise) {
    return ctx.getOps().validateDeclareDatabase(promise, id, name, partecipants, minimumQuorum);
  }

  @Override
  public void apply(OOperationContext ctx, OTransactionIdPromise promise) {
    ctx.declareDatabase(promise, id, name, partecipants, minimumQuorum);
  }

  @Override
  public void cancel(OOperationContext ctx, OTransactionIdPromise promise) {
    ctx.getOps().cancelDeclareDatabase(promise, id, name);
  }

  @Override
  public void serialize(DataOutput out) throws IOException {
    out.writeUTF(name);
    id.writeNetwork(out);
    out.writeInt(minimumQuorum);
    out.writeInt(partecipants.size());
    for (OAddNodeInfo node : partecipants) {
      node.writeNetwork(out);
    }
  }

  @Override
  public short getType() {
    return 4;
  }

  public static ODeclareDbMessage readNetwork(DataInput input) throws IOException {
    String database = input.readUTF();
    ODatabaseId id = ODatabaseId.readNetwork(input);
    int minimumQuorum = input.readInt();
    int size = input.readInt();
    Set<OAddNodeInfo> part = new HashSet<>(size);
    while (size-- > 0) {
      part.add(OAddNodeInfo.readNetwork(input));
    }
    return new ODeclareDbMessage(database, id, part, minimumQuorum);
  }

  public ODatabaseId getId() {
    return id;
  }

  public String getName() {
    return name;
  }

  public Set<OAddNodeInfo> getPartecipants() {
    return partecipants;
  }

  public int getMinimumQuorum() {
    return minimumQuorum;
  }

  @Override
  public String toString() {
    return "Declaring database with name="
        + name
        + ", id="
        + id
        + " nodes="
        + partecipants
        + ", minimumQuorum="
        + minimumQuorum;
  }
}
