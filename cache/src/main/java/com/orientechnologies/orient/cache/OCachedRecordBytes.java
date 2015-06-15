package com.orientechnologies.orient.cache;

import com.orientechnologies.orient.core.compression.impl.OSnappyCompression;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ORecordBytes;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Represents a minimal ORecordBytes to save previous RAM when it's stored in cache.
 * 
 * @author Luca Garulli (l.garulli-at-orientdb.com)
 */
public class OCachedRecordBytes extends OCachedRecord<ORecordBytes> {
  private byte[] data;

  public OCachedRecordBytes() {
  }

  public OCachedRecordBytes(final ORecordBytes iRB) {
    rid = iRB.getIdentity();
    data = OSnappyCompression.INSTANCE.compress(iRB.toStream());
  }

  public ORecordBytes toRecord() {
    final ORecordBytes record = new ORecordBytes(rid).fromStream(OSnappyCompression.INSTANCE.uncompress(data));
    ORecordInternal.unsetDirty(record);
    return record;
  }

  @Override
  public void writeExternal(final ObjectOutput out) throws IOException {
    out.writeShort(rid.getClusterId());
    out.writeLong(rid.getClusterPosition());
    out.writeInt(data.length);
    out.write(data);
  }

  @Override
  public void readExternal(final ObjectInput in) throws IOException, ClassNotFoundException {
    rid = new ORecordId(in.readShort(), in.readLong());
    final int length = in.readInt();
    data = new byte[length];
    in.readFully(data);
  }
}
