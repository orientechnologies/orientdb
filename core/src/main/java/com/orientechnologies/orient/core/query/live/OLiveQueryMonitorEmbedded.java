package com.orientechnologies.orient.core.query.live;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.OLiveQueryMonitor;

/** Created by luigidellaquila on 15/06/17. */
public class OLiveQueryMonitorEmbedded implements OLiveQueryMonitor {

  private final int token;
  private final ODatabaseDocumentInternal db;

  public OLiveQueryMonitorEmbedded(int token, ODatabaseDocumentInternal dbCopy) {
    this.token = token;
    this.db = dbCopy;
  }

  @Override
  public void unSubscribe() {
    ODatabaseDocumentInternal prev = ODatabaseRecordThreadLocal.instance().getIfDefined();
    db.activateOnCurrentThread();
    OLiveQueryHookV2.unsubscribe(token, db);
    if (prev != null) {
      ODatabaseRecordThreadLocal.instance().set(prev);
    } else {
      ODatabaseRecordThreadLocal.instance().remove();
    }
  }

  @Override
  public int getMonitorId() {
    return token;
  }
}
