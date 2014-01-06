package com.orientechnologies.orient.core.record;

import com.orientechnologies.orient.core.id.ORID;

public interface OIdentityChangeListener {
  public void onIdentityChanged(ORID prevRid, ORecord<?> record);
}
