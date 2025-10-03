package com.orientechnologies.orient.distributed.db;

import com.orientechnologies.orient.core.transaction.ODatabaseId;
import com.orientechnologies.orient.core.transaction.ONodeId;
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
  private Set<ONodeId> partecipants;

  public ODeclareDbMessage(String name, ODatabaseId id, Set<ONodeId> partecipants) {
    this.name = name;
    this.id = id;
    this.partecipants = partecipants;
  }

  @Override
  public Optional<OAcceptResult> validate(OrientDBDistributed ctx, OTransactionIdPromise promise) {
    return ctx.promiseDeclare(promise, id, name);
  }

  @Override
  public void apply(OrientDBDistributed ctx, OTransactionIdPromise promise) {
    ctx.declareDatabase(promise, id, name);
  }

  @Override
  public void cancel(OrientDBDistributed ctx, OTransactionIdPromise promise) {
    ctx.cancelDeclare(promise, id, name);
  }

  @Override
  public void serialize(DataOutput out) throws IOException {
    out.writeUTF(name);
    id.writeNetwork(out);
    out.writeInt(partecipants.size());
    for (ONodeId node : partecipants) {
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
    int size = input.readInt();
    Set<ONodeId> part = new HashSet<>(size);
    while (size-- > 0) {
      part.add(ONodeId.readNetwork(input));
    }
    return new ODeclareDbMessage(database, id, part);
  }

  public ODatabaseId getId() {
    return id;
  }

  public String getName() {
    return name;
  }

  public Set<ONodeId> getPartecipants() {
    return partecipants;
  }
}
