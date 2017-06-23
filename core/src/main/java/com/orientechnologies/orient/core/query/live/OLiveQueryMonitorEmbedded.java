package com.orientechnologies.orient.core.query.live;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.OLiveQueryMonitor;

/**
 * Created by luigidellaquila on 15/06/17.
 */
public class OLiveQueryMonitorEmbedded implements OLiveQueryMonitor {

  private final int                       token;
  private final ODatabaseDocumentInternal db;

  public OLiveQueryMonitorEmbedded(int token, ODatabaseDocumentInternal dbCopy) {
    this.token = token;
    this.db = dbCopy;
  }

  @Override
  public void unSubscribe() {
    ODatabaseDocumentInternal prev = ODatabaseRecordThreadLocal.INSTANCE.getIfDefined();
    db.activateOnCurrentThread();
    OLiveQueryHookV2.unsubscribe(token, db);
    if (prev != null) {
      ODatabaseRecordThreadLocal.INSTANCE.set(prev);
    } else {
      ODatabaseRecordThreadLocal.INSTANCE.remove();
    }
  }

  @Override
  public int getMonitorId() {
    return token;
  }
}
