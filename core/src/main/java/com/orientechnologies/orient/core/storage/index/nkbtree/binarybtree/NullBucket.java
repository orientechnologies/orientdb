package com.orientechnologies.orient.core.storage.index.nkbtree.binarybtree;

import com.orientechnologies.common.serialization.types.OShortSerializer;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;

public class NullBucket extends ODurablePage {
  public NullBucket(OCacheEntry cacheEntry) {
    super(cacheEntry);
  }

  public void init() {
    setByteValue(NEXT_FREE_POSITION, (byte) 0);
  }

  public void setValue(final ORID value) {
    setByteValue(NEXT_FREE_POSITION, (byte) 1);

    setShortValue(NEXT_FREE_POSITION + 1, (short) value.getClusterId());
    setLongValue(NEXT_FREE_POSITION + 1 + OShortSerializer.SHORT_SIZE, value.getClusterPosition());
  }

  public ORID getValue() {
    if (getByteValue(NEXT_FREE_POSITION) == 0) {
      return null;
    }

    final int clusterId = getShortValue(NEXT_FREE_POSITION + 1);
    final long clusterPosition = getLongValue(NEXT_FREE_POSITION + 1 + OShortSerializer.SHORT_SIZE);
    return new ORecordId(clusterId, clusterPosition);
  }

  public void removeValue() {
    setByteValue(NEXT_FREE_POSITION, (byte) 0);
  }
}
