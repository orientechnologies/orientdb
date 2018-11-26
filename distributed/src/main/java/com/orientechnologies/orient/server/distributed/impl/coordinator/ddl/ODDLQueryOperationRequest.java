package com.orientechnologies.orient.server.distributed.impl.coordinator.ddl;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.OScenarioThreadLocal;
import com.orientechnologies.orient.server.distributed.impl.coordinator.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import static com.orientechnologies.orient.server.distributed.impl.coordinator.OCoordinateMessagesFactory.DDL_QUERY_NODE_REQUEST;

public class ODDLQueryOperationRequest implements ONodeRequest {
  private String query;

  public ODDLQueryOperationRequest(String query) {
    this.query = query;
  }

  public ODDLQueryOperationRequest() {

  }

  @Override
  public ONodeResponse execute(ODistributedMember nodeFrom, OLogId opId, ODistributedExecutor executor,
      ODatabaseDocumentInternal session) {
    OScenarioThreadLocal.executeAsDistributed(() -> {
      return session.command(query);
    });

    return new ODDLQueryOperationResponse();
  }

  @Override
  public void serialize(DataOutput output) throws IOException {
    output.writeUTF(query);
  }

  @Override
  public void deserialize(DataInput input) throws IOException {
    query = input.readUTF();
  }

  @Override
  public int getRequestType() {
    return DDL_QUERY_NODE_REQUEST;
  }
}
