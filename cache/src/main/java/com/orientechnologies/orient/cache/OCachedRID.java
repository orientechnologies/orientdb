package com.orientechnologies.orient.cache;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Represents a minimal serializable ORID.
 * 
 * @author Luca Garulli (l.garulli-at-orientdb.com)
 */
public class OCachedRID implements Externalizable {
  private short clusterId;
  private long  clusterPosition;

  public OCachedRID(final ORID iRid) {
    clusterId = (short) iRid.getClusterId();
    clusterPosition = iRid.getClusterPosition();
  }

  public ORID toRID() {
    return new ORecordId(clusterId, clusterPosition);
  }

  @Override
  public void writeExternal(final ObjectOutput out) throws IOException {
    out.writeShort(clusterId);
    out.writeLong(clusterPosition);
  }

  @Override
  public void readExternal(final ObjectInput in) throws IOException, ClassNotFoundException {
    clusterId = in.readShort();
    clusterPosition = in.readLong();
  }
}
