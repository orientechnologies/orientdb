package com.orientechnologies.orient.distributed.impl.structural;

import com.orientechnologies.orient.distributed.OrientDBDistributed;
import com.orientechnologies.orient.distributed.impl.coordinator.OLogId;
import com.orientechnologies.orient.distributed.impl.coordinator.transaction.OSessionOperationId;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.orientechnologies.orient.distributed.impl.coordinator.OCoordinateMessagesFactory.CONFIGURATION_FETCH_SUBMIT_REQUEST;

public class OConfigurationFetchRequest implements OStructuralSubmitRequest {

  private OLogId                        lastLogId;
  private List<OStructuralNodeDatabase> databases;

  public OConfigurationFetchRequest(OLogId lastLogId, List<OStructuralNodeDatabase> databases) {
    this.lastLogId = lastLogId;
    this.databases = databases;
  }

  @Override
  public void begin(OStructuralDistributedMember sender, OSessionOperationId operationId, OStructuralCoordinator coordinator,
      OrientDBDistributed context) {
    coordinator
        .reply(sender, operationId, new OConfigurationFetchResponse(context.getStructuralConfiguration().getSharedConfiguration()));

  }

  @Override
  public void serialize(DataOutput output) throws IOException {
    if (lastLogId == null) {
      output.writeBoolean(false);
    } else {
      output.writeBoolean(true);
      OLogId.serialize(lastLogId, output);
    }
    output.write(databases.size());
    for (OStructuralNodeDatabase database : databases) {
      database.distributedSerialize(output);
    }
  }

  @Override
  public void deserialize(DataInput input) throws IOException {
    if (input.readBoolean()) {
      lastLogId = OLogId.deserialize(input);
    } else {
      lastLogId = null;
    }
    int size = input.readInt();
    databases = new ArrayList<>(size);
    while (size-- > 0) {
      OStructuralNodeDatabase database = new OStructuralNodeDatabase();
      database.deserialize(input);
      databases.add(database);
    }
  }

  @Override
  public int getRequestType() {
    return CONFIGURATION_FETCH_SUBMIT_REQUEST;
  }
}
