package com.orientechnologies.orient.server;

import com.orientechnologies.common.exception.OErrorCode;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.client.remote.message.OLiveQueryPushRequest;
import com.orientechnologies.orient.client.remote.message.live.OLiveQueryResult;
import com.orientechnologies.orient.core.db.OLiveQueryBatchResultListener;
import com.orientechnologies.orient.core.db.OSharedContext;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.exception.OCoreException;
import com.orientechnologies.orient.core.exception.OLiveQueryInterruptedException;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.server.network.protocol.binary.ONetworkProtocolBinary;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Created by tglman on 19/06/17. */
class OServerLiveQueryResultListener implements OLiveQueryBatchResultListener {
  private final ONetworkProtocolBinary protocol;
  private final OSharedContext sharedContext;
  private int monitorId;

  List<OLiveQueryResult> toSend = new ArrayList<>();

  public OServerLiveQueryResultListener(
      ONetworkProtocolBinary protocol, OSharedContext sharedContext) {
    this.protocol = protocol;
    this.sharedContext = sharedContext;
  }

  public void setMonitorId(int monitorId) {
    this.monitorId = monitorId;
  }

  private synchronized void addEvent(OLiveQueryResult event) {
    toSend.add(event);
  }

  @Override
  public void onCreate(ODatabaseDocument database, OResult data) {
    addEvent(new OLiveQueryResult(OLiveQueryResult.CREATE_EVENT, data, null));
  }

  @Override
  public void onUpdate(ODatabaseDocument database, OResult before, OResult after) {
    addEvent(new OLiveQueryResult(OLiveQueryResult.UPDATE_EVENT, after, before));
  }

  @Override
  public void onDelete(ODatabaseDocument database, OResult data) {
    addEvent(new OLiveQueryResult(OLiveQueryResult.DELETE_EVENT, data, null));
  }

  @Override
  public void onError(ODatabaseDocument database, OException exception) {
    try {
      // TODO: resolve error identifier
      int errorIdentifier = 0;
      OErrorCode code = OErrorCode.GENERIC_ERROR;
      if (exception instanceof OCoreException) {
        code = ((OCoreException) exception).getErrorCode();
      }
      protocol.push(
          new OLiveQueryPushRequest(monitorId, errorIdentifier, code, exception.getMessage()));
    } catch (IOException e) {
      throw OException.wrapException(
          new OLiveQueryInterruptedException("Live query interrupted by socket close"), e);
    }
  }

  @Override
  public void onEnd(ODatabaseDocument database) {
    try {
      protocol.push(
          new OLiveQueryPushRequest(monitorId, OLiveQueryPushRequest.END, Collections.emptyList()));
    } catch (IOException e) {
      throw OException.wrapException(
          new OLiveQueryInterruptedException("Live query interrupted by socket close"), e);
    }
  }

  @Override
  public void onBatchEnd(ODatabaseDocument database) {
    sendEvents(database);
  }

  private synchronized void sendEvents(ODatabaseDocument database) {
    if (toSend.isEmpty()) {
      return;
    }
    List<OLiveQueryResult> events = toSend;
    toSend = new ArrayList<>();

    try {
      protocol.push(new OLiveQueryPushRequest(monitorId, OLiveQueryPushRequest.HAS_MORE, events));
    } catch (IOException e) {
      sharedContext.getLiveQueryOpsV2().getSubscribers().remove(monitorId);
      throw OException.wrapException(
          new OLiveQueryInterruptedException("Live query interrupted by socket close"), e);
    }
  }
}
