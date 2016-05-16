package com.orientechnologies.orient.core.index;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.hook.ORecordHook;
import com.orientechnologies.orient.core.record.ORecord;

/**
 * Created by tglman on 29/04/16.
 */
public class ClassIndexManagerRemote extends OClassIndexManager {

  public ClassIndexManagerRemote(ODatabaseDocument database) {
    super(database);
  }

  @Override
  public RESULT onTrigger(TYPE iType, ORecord iRecord) {
    if (database.getTransaction().isActive())
      return super.onTrigger(iType, iRecord);
    else
      return RESULT.RECORD_NOT_CHANGED;
  }

  @Override
  protected void putInIndex(OIndex<?> index, Object key, OIdentifiable value) {
    assert index instanceof OIndexTxAware;
    ((OIndexTxAware) index).putOnlyClientTrack(key, value);
  }

  @Override
  protected void removeFromIndex(OIndex<?> index, Object key, OIdentifiable value) {
    assert index instanceof OIndexTxAware;
    ((OIndexTxAware) index).removeOnlyClientTrack(key, value);
  }
}
