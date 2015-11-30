package com.orientechnologies.orient.core.storage.impl.local.paginated;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperationMetadata;

import java.util.HashSet;
import java.util.Set;

public class ORecordOperationMetadata implements OAtomicOperationMetadata<Set<ORID>> {
  public static final String RID_METADATA_KEY = "cluster.record.rid";

  private final Set<ORID> rids = new HashSet<ORID>();

  public void addRid(ORID rid) {
    rids.add(rid);
  }

  @Override
  public String getKey() {
    return RID_METADATA_KEY;
  }

  @Override
  public Set<ORID> getValue() {
    return rids;
  }
}
