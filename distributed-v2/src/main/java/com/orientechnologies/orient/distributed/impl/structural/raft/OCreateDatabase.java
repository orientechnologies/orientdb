package com.orientechnologies.orient.distributed.impl.structural.raft;

import static com.orientechnologies.orient.distributed.impl.coordinator.OCoordinateMessagesFactory.CREATE_DATABASE_REQUEST;

import com.orientechnologies.orient.distributed.OrientDBDistributed;
import com.orientechnologies.orient.distributed.impl.coordinator.transaction.OSessionOperationId;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class OCreateDatabase implements ORaftOperation {

  private OSessionOperationId operationId;
  private String database;
  private String type;
  private Map<String, String> configurations;

  public OCreateDatabase(
      OSessionOperationId operationId,
      String database,
      String type,
      Map<String, String> configurations) {
    this.operationId = operationId;
    this.database = database;
    this.type = type;
    this.configurations = configurations;
  }

  public OCreateDatabase() {}

  @Override
  public void apply(OrientDBDistributed context) {
    context.internalCreateDatabase(operationId, database, type, configurations);
  }

  @Override
  public void serialize(DataOutput output) throws IOException {
    operationId.serialize(output);
    output.writeUTF(database);
    output.writeUTF(type);
    output.writeInt(configurations.size());
    for (Map.Entry<String, String> configuration : configurations.entrySet()) {
      output.writeUTF(configuration.getKey());
      output.writeUTF(configuration.getValue());
    }
  }

  @Override
  public int getRequestType() {
    return CREATE_DATABASE_REQUEST;
  }

  @Override
  public void deserialize(DataInput input) throws IOException {
    this.operationId = new OSessionOperationId();
    this.operationId.deserialize(input);
    this.database = input.readUTF();
    this.type = input.readUTF();
    int size = input.readInt();
    this.configurations = new HashMap<>(size);
    while (size-- > 0) {
      String key = input.readUTF();
      String value = input.readUTF();
      configurations.put(key, value);
    }
  }

  @Override
  public Optional<OSessionOperationId> getRequesterSequential() {
    return Optional.of(operationId);
  }
}
