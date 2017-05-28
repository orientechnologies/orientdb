package com.orientechnologies.orient.client.remote;

import com.orientechnologies.orient.client.remote.message.OLiveQueryPushRequest;
import com.orientechnologies.orient.client.remote.message.live.OLiveQueryResult;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.OLiveQueryResultListener;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.query.live.OLiveQueryListener;

/**
 * Created by tglman on 28/05/17.
 */
public class OLiveQueryClientListener {

  private ODatabaseDocument        database;
  private OLiveQueryResultListener listener;

  public OLiveQueryClientListener(ODatabaseDocument database, OLiveQueryResultListener listener) {
    this.database = database;
    this.listener = listener;
  }

  /**
   * Return true if the push request require an unregister
   *
   * @param pushRequest
   *
   * @return
   */
  public boolean onEvent(OLiveQueryPushRequest pushRequest) {
    ODatabaseDocumentInternal old = ODatabaseRecordThreadLocal.INSTANCE.getIfDefined();
    try {
      database.activateOnCurrentThread();
      for (OLiveQueryResult result : pushRequest.getEvents()) {
        switch (result.getEventType()) {
        case OLiveQueryResult.CREATE_EVENT:
          listener.onCreate(database, result.getCurrentValue());
          break;
        case OLiveQueryResult.UPDATE_EVENT:
          listener.onUpdate(database, result.getOldValue(), result.getCurrentValue());
          break;
        case OLiveQueryResult.DELETE_EVENT:
          listener.onDelete(database, result.getCurrentValue());
          break;
        }
      }
      if (pushRequest.getStatus() == OLiveQueryPushRequest.END) {
        onEnd();
        return true;
      }
      return false;
    } finally {
      ODatabaseRecordThreadLocal.INSTANCE.set(old);
    }
  }

  public void onError() {
    ODatabaseDocumentInternal old = ODatabaseRecordThreadLocal.INSTANCE.getIfDefined();
    try {
      database.activateOnCurrentThread();
      listener.onError(database);
      database.close();
    } finally {
      ODatabaseRecordThreadLocal.INSTANCE.set(old);
    }
  }

  public void onEnd() {
    ODatabaseDocumentInternal old = ODatabaseRecordThreadLocal.INSTANCE.getIfDefined();
    try {
      database.activateOnCurrentThread();
      listener.onEnd(database);
      database.close();
    } finally {
      ODatabaseRecordThreadLocal.INSTANCE.set(old);
    }
  }
}
