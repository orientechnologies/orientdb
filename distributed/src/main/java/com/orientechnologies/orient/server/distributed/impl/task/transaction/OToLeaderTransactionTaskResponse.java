package com.orientechnologies.orient.server.distributed.impl.task.transaction;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.serialization.OStreamable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OToLeaderTransactionTaskResponse implements OStreamable {

  private Map<ORID, ORID> newIds;

  public OToLeaderTransactionTaskResponse(Map<ORID, ORID> newIds) {
    this.newIds = newIds;
  }

  public OToLeaderTransactionTaskResponse() {
  }

  @Override
  public void toStream(DataOutput out) throws IOException {
    out.writeInt(newIds.size());
    for (Map.Entry<ORID, ORID> entry : newIds.entrySet()) {
      out.writeShort(entry.getKey().getClusterId());
      out.writeLong(entry.getKey().getClusterPosition());
      out.writeShort(entry.getValue().getClusterId());
      out.writeLong(entry.getValue().getClusterPosition());
    }
  }

  @Override
  public void fromStream(DataInput in) throws IOException {
    int size = in.readInt();
    newIds = new HashMap<>(size);
    while (size-- > 0) {
      short cluster = in.readShort();
      long position = in.readLong();
      short cluster1 = in.readShort();
      long position1 = in.readLong();
      newIds.put(new ORecordId(cluster, position), new ORecordId(cluster1, position1));
    }
  }

  public Map<ORID, ORID> getNewIds() {
    return newIds;
  }
}
