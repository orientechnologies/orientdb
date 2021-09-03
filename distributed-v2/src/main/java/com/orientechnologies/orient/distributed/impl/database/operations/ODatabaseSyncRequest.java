package com.orientechnologies.orient.distributed.impl.database.operations;

import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.distributed.OrientDBDistributed;
import com.orientechnologies.orient.distributed.impl.database.sync.ODatabaseFullSyncSender;
import com.orientechnologies.orient.distributed.impl.log.OLogId;
import com.orientechnologies.orient.distributed.impl.structural.operations.OOperation;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Optional;

public class ODatabaseSyncRequest implements OOperation {

  private String database;
  private Optional<OLogId> opId;

  public ODatabaseSyncRequest(String database, Optional<OLogId> opId) {
    this.database = database;
    this.opId = opId;
  }

  @Override
  public void apply(ONodeIdentity sender, OrientDBDistributed context) {
    boolean delta =
        context.getDistributedContext(this.database).getCoordinator().requestSync(sender, opId);
    if (!delta) {
      new ODatabaseFullSyncSender(context, sender, database).run();
    }
  }

  @Override
  public void deserialize(DataInput input) throws IOException {
    database = input.readUTF();
    opId = OLogId.deserializeOptional(input);
  }

  @Override
  public void serialize(DataOutput output) throws IOException {
    output.writeUTF(database);
    OLogId.serializeOptional(opId, output);
  }

  @Override
  public int getOperationId() {
    return 0;
  }
}
