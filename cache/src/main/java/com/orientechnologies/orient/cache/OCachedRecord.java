package com.orientechnologies.orient.cache;

import java.io.Externalizable;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.ORecord;

/**
 * Created by luca on 13/06/15.
 */
public abstract class OCachedRecord<T extends ORecord> implements Externalizable {
  protected ORID rid;

  public abstract T toRecord();

  public ORID getIdentity() {
    return rid;
  }
}
