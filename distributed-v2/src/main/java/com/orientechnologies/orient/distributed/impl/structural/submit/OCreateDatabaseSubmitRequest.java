package com.orientechnologies.orient.distributed.impl.structural.submit;

import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.distributed.impl.coordinator.OCoordinateMessagesFactory;
import com.orientechnologies.orient.distributed.impl.coordinator.transaction.OSessionOperationId;
import com.orientechnologies.orient.distributed.impl.structural.raft.OLeaderContext;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class OCreateDatabaseSubmitRequest implements OStructuralSubmitRequest {

  private String database;
  private String type;
  private Map<String, String> configurations;

  public OCreateDatabaseSubmitRequest(
      String database, String type, Map<String, String> configurations) {
    this.database = database;
    this.type = type;
    this.configurations = configurations;
  }

  public OCreateDatabaseSubmitRequest() {}

  @Override
  public void begin(
      Optional<ONodeIdentity> requester, OSessionOperationId id, OLeaderContext context) {
    context.createDatabase(requester, id, database, type, configurations);
  }

  @Override
  public void serialize(DataOutput output) throws IOException {
    output.writeUTF(database);
    output.writeUTF(type);
    output.writeInt(configurations.size());
    for (Map.Entry<String, String> configuration : configurations.entrySet()) {
      output.writeUTF(configuration.getKey());
      output.writeUTF(configuration.getValue());
    }
  }

  @Override
  public void deserialize(DataInput input) throws IOException {
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
  public int getRequestType() {
    return OCoordinateMessagesFactory.CREATE_DATABASE_SUBMIT_REQUEST;
  }
}
